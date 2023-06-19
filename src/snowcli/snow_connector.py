from __future__ import annotations

import os
from io import StringIO
from pathlib import Path
from typing import Optional

import click
import snowflake.connector
from jinja2 import Environment, FileSystemLoader
from snowflake.connector.cursor import SnowflakeCursor

TEMPLATES_PATH = Path(__file__).parent / "sql"


def get_standard_stage_name(name: str) -> str:
    # Handle embedded stages
    if name.startswith("snow://"):
        return name

    return f"@{name}"


class SnowflakeConnector:
    """Initialize a connection from a snowsql-formatted config"""

    def __init__(
        self,
        connection_config: dict,
        overrides: Optional[dict] = None,
    ):
        self.connection_config = connection_config
        if overrides:
            for config, value in ((k, v) for k, v in overrides.items() if v):
                self.connection_config[config] = value
        self.connection_config["application"] = self._find_command_path()
        self.ctx = snowflake.connector.connect(**self.connection_config)
        self.cs = self.ctx.cursor()

    @staticmethod
    def _find_command_path():
        ctx = click.get_current_context(silent=True)
        if ctx:
            # Example: SNOWCLI.WAREHOUSE.STATUS
            return ".".join(["SNOWCLI", *ctx.command_path.split(" ")[1:]]).upper()
        return "SNOWCLI"

    def __del__(self):
        try:
            self.cs.close()
            self.ctx.close()
        except (TypeError, AttributeError):
            pass

    def get_version(self):
        self.cs.execute("SELECT current_version()")
        return self.cs.fetchone()[0]

    def create_function(
        self,
        name: str,
        input_parameters: str,
        return_type: str,
        handler: str,
        imports: str,
        database: str,
        schema: str,
        role: str,
        warehouse: str,
        overwrite: bool,
        packages: list[str],
    ) -> SnowflakeCursor:
        return self.run_sql(
            "create_function",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "overwrite": overwrite,
                "input_parameters": input_parameters,
                "return_type": return_type,
                "handler": handler,
                "imports": imports,
                "packages": packages,
                "signature": self.generate_signature_from_params(
                    input_parameters,
                ),
            },
        )

    def create_procedure(
        self,
        name: str,
        input_parameters: str,
        return_type: str,
        handler: str,
        imports: str,
        database: str,
        schema: str,
        role: str,
        warehouse: str,
        overwrite: bool,
        packages: list[str],
        execute_as_caller: bool,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "create_procedure",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "overwrite": overwrite,
                "input_parameters": input_parameters,
                "return_type": return_type,
                "handler": handler,
                "imports": imports,
                "packages": packages,
                "signature": self.generate_signature_from_params(
                    input_parameters,
                ),
                "execute_as_caller": execute_as_caller,
            },
        )

    def upload_file_to_stage(
        self,
        file_path,
        destination_stage,
        path,
        role,
        database,
        warehouse,
        schema,
        overwrite,
        parallel: int = 4,
        create_stage: bool = True,
    ):
        create_stage_command = ""
        if create_stage:
            create_stage_command = (
                f"create stage if not exists {destination_stage} "
                "comment='deployments managed by snowcli'"
            )

        full_stage_name = (
            f"@{destination_stage}"
            if not destination_stage.startswith("snow://")
            else destination_stage
        )
        return self.run_sql(
            "put_stage",
            {
                "role": role,
                "database": database,
                "schema": schema,
                "warehouse": warehouse,
                "path": file_path,
                "destination_path": path,
                "name": full_stage_name,
                "create_stage_command": create_stage_command,
                "parallel": parallel,
                "overwrite": overwrite,
            },
        )

    def execute_function(
        self,
        function,
        database,
        schema,
        role,
        warehouse,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "execute_function",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "function": function,
            },
        )

    def execute_procedure(
        self,
        procedure,
        database,
        schema,
        role,
        warehouse,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "call_procedure",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "procedure": procedure,
            },
        )

    def describe_function(
        self,
        database,
        schema,
        role,
        warehouse,
        signature=None,
        name=None,
        input_parameters=None,
        show_exceptions=True,
    ) -> SnowflakeCursor:
        if signature is None and name and input_parameters:
            signature = name + self.generate_signature_from_params(input_parameters)
        return self.run_sql(
            "describe_function",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "signature": signature,
            },
            show_exceptions,
        )

    def describe_procedure(
        self,
        database,
        schema,
        role,
        warehouse,
        signature=None,
        name=None,
        input_parameters=None,
        show_exceptions=True,
    ) -> SnowflakeCursor:
        if signature is None and name and input_parameters:
            signature = name + self.generate_signature_from_params(input_parameters)
        return self.run_sql(
            "describe_procedure",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "signature": signature,
            },
            show_exceptions,
        )

    def list_functions(
        self,
        database,
        schema,
        role,
        warehouse,
        like="%%",
    ) -> SnowflakeCursor:
        return self.run_sql(
            "list_functions",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "like": like,
            },
        )

    def list_stages(
        self,
        database,
        schema,
        role,
        warehouse,
        like="%%",
    ) -> SnowflakeCursor:
        return self.run_sql(
            "list_stages",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "like": like,
            },
        )

    def list_stage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
        like="%%",
    ) -> SnowflakeCursor:
        name = get_standard_stage_name(name)

        return self.run_sql(
            "list_stage",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "like": like,
            },
        )

    def get_stage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
        path,
    ) -> SnowflakeCursor:
        name = get_standard_stage_name(name)

        return self.run_sql(
            "get_stage",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "path": path,
            },
        )

    def set_procedure_comment(
        self,
        database,
        schema,
        role,
        warehouse,
        signature=None,
        name=None,
        input_parameters=None,
        show_exceptions=True,
        comment="",
    ) -> SnowflakeCursor:
        if signature is None and name and input_parameters:
            signature = name + self.generate_signature_from_params(input_parameters)
        return self.run_sql(
            "set_procedure_comment",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "signature": signature,
                "comment": comment,
            },
            show_exceptions,
        )

    def put_stage(
        self,
        database,
        schema,
        warehouse,
        role,
        name,
        path,
        overwrite: bool = False,
        parallel: int = 4,
    ) -> SnowflakeCursor:
        name = get_standard_stage_name(name)

        return self.run_sql(
            "put_stage",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "path": path,
                "overwrite": overwrite,
                "parallel": parallel,
            },
        )

    def remove_from_stage(
        self, database, schema, role, warehouse, name, path
    ) -> SnowflakeCursor:
        name = get_standard_stage_name(name)

        return self.run_sql(
            "remove_from_stage",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "path": path,
            },
        )

    def create_stage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "create_stage",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def drop_stage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "drop_stage",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def list_procedures(
        self,
        database,
        schema,
        role,
        warehouse,
        like="%%",
    ) -> SnowflakeCursor:
        return self.run_sql(
            "list_procedures",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "like": like,
            },
        )

    def drop_function(
        self,
        database,
        schema,
        role,
        warehouse,
        signature,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "drop_function",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "signature": signature,
            },
        )

    def drop_procedure(
        self,
        database,
        schema,
        role,
        warehouse,
        signature,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "drop_procedure",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "signature": signature,
            },
        )

    def list_streamlits(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
    ) -> SnowflakeCursor:
        return self.run_sql(
            "list_streamlits",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
            },
        )

    def show_warehouses(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
        like="%%",
    ) -> SnowflakeCursor:
        return self.run_sql(
            "show_warehouses",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
            },
        )

    def create_streamlit(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
        name="",
        file="",
        from_stage_command="",
    ) -> SnowflakeCursor:
        return self.run_sql(
            "create_streamlit",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "file_name": file,
                "from_stage_command": from_stage_command,
            },
        )

    def share_streamlit(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
        name="",
        to_role="",
    ) -> SnowflakeCursor:
        return self.run_sql(
            "share_streamlit",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "to_role": to_role,
            },
        )

    def drop_streamlit(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
        name="",
    ) -> SnowflakeCursor:
        return self.run_sql(
            "drop_streamlit",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def deploy_streamlit(
        self,
        name,
        file_path,
        stage_path,
        role,
        database,
        schema,
        warehouse,
        overwrite,
    ):
        stage_name = f"snow://streamlit/{database}.{schema}.{name}/default_checkout"

        """Upload main python file to stage and return url of streamlit"""
        self.upload_file_to_stage(
            file_path=file_path,
            destination_stage=stage_name,
            path=stage_path,
            role=role,
            database=database,
            schema=schema,
            warehouse=warehouse,
            overwrite=overwrite,
            create_stage=False,
        )

        result = self.run_sql(
            "get_streamlit_url",
            {
                "name": name,
                "role": role,
                "database": database,
                "schema": schema,
            },
        )

        return result.fetchone()[0]

    def describe_streamlit(self, name, database, schema, role, warehouse):
        description = self.run_sql(
            "describe_streamlit",
            {
                "name": name,
                "role": role,
                "database": database,
                "schema": schema,
                "warehouse": warehouse,
            },
        )
        url = self.run_sql(
            "get_streamlit_url",
            {
                "name": name,
                "role": role,
                "database": database,
                "schema": schema,
            },
        )
        return (description, url)

    def run_sql(
        self,
        command,
        context,
        show_exceptions=True,
    ) -> SnowflakeCursor:
        env = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
        template = env.get_template(f"{command}.sql")
        sql = template.render(**context)
        try:
            if os.getenv("DEBUG"):
                print(f"Executing sql:\n{sql}")
            results = self.ctx.execute_stream(StringIO(sql))

            # Return result from last cursor
            *_, last_result = results
            return last_result
        except snowflake.connector.errors.ProgrammingError as e:
            if show_exceptions:
                print(f"Error executing sql:\n{sql}")
                print(e)
            raise (e)

    @staticmethod
    def generate_signature_from_params(params: str) -> str:
        if params == "()":
            return "()"
        return "(" + " ".join(params.split()[1::2])
