from __future__ import annotations

import os

import click
import logging
import hashlib
from io import StringIO
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Optional

import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)
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
        connection_parameters: dict,
        overrides: Optional[dict] = None,
    ):
        if overrides:
            connection_parameters.update(
                {k: v for k, v in overrides.items() if v is not None}
            )
        self.ctx = snowflake.connector.connect(
            application=self._find_command_path(),
            **connection_parameters,
        )
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

    def create_service(
        self,
        name: str,
        compute_pool: str,
        spec_path: str,
        role: str,
        warehouse: str,
        database: str,
        num_instances: int,
        schema: str,
        stage: str,
    ) -> SnowflakeCursor:
        spec_filename = os.path.basename(spec_path)
        file_hash = hashlib.md5(open(spec_path, "rb").read()).hexdigest()
        stage_dir = os.path.join("services", file_hash)
        return self.run_sql(
            "snowservices/services/create_service",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "num_instances": num_instances,
                "compute_pool": compute_pool,
                "spec_path": spec_path,
                "stage_dir": stage_dir,
                "stage_filename": spec_filename,
                "stage": stage,
            },
        )

    def desc_service(
        self, name: str, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/services/desc_service",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def status_service(
        self, name: str, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/services/status_service",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def list_service(
        self, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/services/list_service",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
            },
        )

    def drop_service(
        self, name: str, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/services/drop_service",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def logs_service(
        self,
        name: str,
        instance_id: str,
        container_name: str,
        role: str,
        warehouse: str,
        database: str,
        schema: str,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/services/logs_service",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "instance_id": instance_id,
                "container_name": container_name,
            },
        )

    def create_job(
        self,
        compute_pool: str,
        spec_path: str,
        role: str,
        warehouse: str,
        database: str,
        schema: str,
        stage: str,
    ) -> SnowflakeCursor:
        spec_filename = os.path.basename(spec_path)
        file_hash = hashlib.md5(open(spec_path, "rb").read()).hexdigest()
        stage_dir = os.path.join("jobs", file_hash)
        return self.run_sql(
            "snowservices/jobs/create_job",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "compute_pool": compute_pool,
                "spec_path": spec_path,
                "stage_dir": stage_dir,
                "stage_filename": spec_filename,
                "stage": stage,
            },
        )

    def desc_job(
        self, id: str, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/jobs/desc_job",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "id": id,
            },
        )

    def status_job(
        self, id: str, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/jobs/status_job",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "id": id,
            },
        )

    def drop_job(
        self, id: str, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/jobs/drop_job",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "id": id,
            },
        )

    def logs_job(
        self,
        id: str,
        container_name: str,
        role: str,
        warehouse: str,
        database: str,
        schema: str,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/jobs/logs_job",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "id": id,
                "container_name": container_name,
            },
        )

    def create_compute_pool(
        self,
        name: str,
        num_instances: int,
        instance_family: str,
        role: str,
        warehouse: str,
        database: str,
        schema: str,
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/compute_pool/create_compute_pool",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "min_node": num_instances,
                "max_node": num_instances,
                "instance_family": instance_family,
            },
        )

    def stop_compute_pool(
        self, role: str, warehouse: str, database: str, schema: str, name: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/compute_pool/stop_compute_pools",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def drop_compute_pool(
        self, name: str, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/compute_pool/drop_compute_pool",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def list_compute_pools(
        self, role: str, warehouse: str, database: str, schema: str
    ) -> SnowflakeCursor:
        return self.run_sql(
            "snowservices/compute_pool/list_compute_pools",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
            },
        )

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
            log.debug(f"Executing sql:\n{sql}")
            results = self.ctx.execute_stream(StringIO(sql))

            # Return result from last cursor
            *_, last_result = results
            return last_result
        except snowflake.connector.errors.ProgrammingError as e:
            if show_exceptions:
                log.error(f"Error executing sql:\n{sql}")
            raise e

    @staticmethod
    def generate_signature_from_params(params: str) -> str:
        if params == "()":
            return "()"
        return "(" + " ".join(params.split()[1::2])
