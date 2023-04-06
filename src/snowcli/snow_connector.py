from __future__ import annotations

import os
import pkgutil
from io import StringIO
from pathlib import Path
from typing import Optional

import snowflake.connector
from jinja2 import Environment, FileSystemLoader
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.snowsql_config import SnowsqlConfig

TEMPLATES_PATH = Path(__file__).parent / "sql"


class SnowflakeConnector:
    """Initialize a connection from a snowsql-formatted config"""

    def __init__(
        self,
        snowsql_config: SnowsqlConfig,
        connection_name: str,
        overrides: Optional[dict] = None,
    ):
        self.snowsql_config = snowsql_config
        self.connection_name = connection_name
        self.connection_config: dict = snowsql_config.get_connection(
            connection_name,
        )
        if overrides:
            for config, value in ((k, v) for k, v in overrides.items() if v):
                self.connection_config[config] = value
        self.connection_config["application"] = "SNOWCLI"
        self.ctx = snowflake.connector.connect(**self.connection_config)
        self.cs = self.ctx.cursor()

    def __del__(self):
        try:
            self.cs.close()
            self.ctx.close()
        except (TypeError, AttributeError):
            pass

    def getVersion(self):
        self.cs.execute("SELECT current_version()")
        return self.cs.fetchone()[0]

    def createFunction(
        self,
        name: str,
        inputParameters: str,
        returnType: str,
        handler: str,
        imports: str,
        database: str,
        schema: str,
        role: str,
        warehouse: str,
        overwrite: bool,
        packages: list[str],
    ) -> SnowflakeCursor:
        return self.runSql(
            "create_function",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "overwrite": overwrite,
                "input_parameters": inputParameters,
                "return_type": returnType,
                "handler": handler,
                "imports": imports,
                "packages": packages,
                "signature": self.generate_signature_from_params(
                    inputParameters,
                ),
            },
        )

    def createProcedure(
        self,
        name: str,
        inputParameters: str,
        returnType: str,
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
        return self.runSql(
            "create_procedure",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "overwrite": overwrite,
                "input_parameters": inputParameters,
                "return_type": returnType,
                "handler": handler,
                "imports": imports,
                "packages": packages,
                "signature": self.generate_signature_from_params(
                    inputParameters,
                ),
                "execute_as_caller": execute_as_caller,
            },
        )

    def uploadFileToStage(
        self,
        file_path,
        destination_stage,
        path,
        role,
        database,
        schema,
        overwrite,
    ):
        self.cs.execute(f"use role {role}")
        self.cs.execute(f"use database {database}")
        self.cs.execute(f"use schema {schema}")
        self.cs.execute(
            f"create stage if not exists {destination_stage} "
            'comment="deployments managed by snowcli"',
        )
        self.cs.execute(
            f"PUT file://{file_path} @{destination_stage}{path} "
            "auto_compress=false overwrite="
            f'{"true" if overwrite else "false"}',
        )
        return self.cs.fetchone()

    def executeFunction(
        self,
        function,
        database,
        schema,
        role,
        warehouse,
    ) -> SnowflakeCursor:
        return self.runSql(
            "execute_function",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "function": function,
            },
        )

    def executeProcedure(
        self,
        procedure,
        database,
        schema,
        role,
        warehouse,
    ) -> SnowflakeCursor:
        return self.runSql(
            "call_procedure",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "procedure": procedure,
            },
        )

    def describeFunction(
        self,
        database,
        schema,
        role,
        warehouse,
        signature=None,
        name=None,
        inputParameters=None,
        show_exceptions=True,
    ) -> SnowflakeCursor:
        if signature is None and name and inputParameters:
            signature = name + self.generate_signature_from_params(inputParameters)
        return self.runSql(
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

    def describeProcedure(
        self,
        database,
        schema,
        role,
        warehouse,
        signature=None,
        name=None,
        inputParameters=None,
        show_exceptions=True,
    ) -> SnowflakeCursor:
        if signature is None and name and inputParameters:
            signature = name + self.generate_signature_from_params(inputParameters)
        return self.runSql(
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

    def listFunctions(
        self,
        database,
        schema,
        role,
        warehouse,
        like="%%",
    ) -> SnowflakeCursor:
        return self.runSql(
            "list_functions",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "like": like,
            },
        )

    def listStages(
        self,
        database,
        schema,
        role,
        warehouse,
        like="%%",
    ) -> SnowflakeCursor:
        return self.runSql(
            "list_stages",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "like": like,
            },
        )

    def listStage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
        like="%%",
    ) -> SnowflakeCursor:
        return self.runSql(
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

    def getStage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
        path,
    ) -> SnowflakeCursor:
        return self.runSql(
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

    def setProcedureComment(
        self,
        database,
        schema,
        role,
        warehouse,
        signature=None,
        name=None,
        inputParameters=None,
        show_exceptions=True,
        comment="",
    ) -> SnowflakeCursor:
        if signature is None and name and inputParameters:
            signature = name + self.generate_signature_from_params(inputParameters)
        return self.runSql(
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

    def putStage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
        path,
        overwrite: bool = False,
        parallel: int = 4,
    ) -> SnowflakeCursor:
        return self.runSql(
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

    def removeFromStage(
        self, database, schema, role, warehouse, name, path
    ) -> SnowflakeCursor:
        return self.runSql(
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

    def createStage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
    ) -> SnowflakeCursor:
        return self.runSql(
            "create_stage",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def dropStage(
        self,
        database,
        schema,
        role,
        warehouse,
        name,
    ) -> SnowflakeCursor:
        return self.runSql(
            "drop_stage",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
            },
        )

    def listProcedures(
        self,
        database,
        schema,
        role,
        warehouse,
        like="%%",
    ) -> SnowflakeCursor:
        return self.runSql(
            "list_procedures",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "like": like,
            },
        )

    def dropFunction(
        self,
        database,
        schema,
        role,
        warehouse,
        signature,
    ) -> SnowflakeCursor:
        return self.runSql(
            "drop_function",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "signature": signature,
            },
        )

    def dropProcedure(
        self,
        database,
        schema,
        role,
        warehouse,
        signature,
    ) -> SnowflakeCursor:
        return self.runSql(
            "drop_procedure",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "signature": signature,
            },
        )

    def listStreamlits(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
    ) -> SnowflakeCursor:
        return self.runSql(
            "list_streamlits",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
            },
        )

    def showWarehouses(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
        like="%%",
    ) -> SnowflakeCursor:
        return self.runSql(
            "show_warehouses",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
            },
        )

    def createStreamlit(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
        name="",
        file="",
    ) -> SnowflakeCursor:
        return self.runSql(
            "create_streamlit",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "file_name": file,
            },
        )

    def shareStreamlit(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
        name="",
        to_role="",
    ) -> SnowflakeCursor:
        return self.runSql(
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

    def dropStreamlit(
        self,
        database="",
        schema="",
        role="",
        warehouse="",
        name="",
        drop_stage: bool = True,
    ) -> SnowflakeCursor:

        drop_command = f'drop stage "{name}_STAGE";' if drop_stage else ""

        return self.runSql(
            "drop_streamlit",
            {
                "database": database,
                "schema": schema,
                "role": role,
                "warehouse": warehouse,
                "name": name,
                "drop_command": drop_command,
            },
        )

    def deployStreamlit(
        self,
        name,
        file_path,
        stage_path,
        role,
        database,
        schema,
        overwrite,
    ):
        """Upload main python file to stage and return url of streamlit"""
        self.uploadFileToStage(
            file_path,
            f"{name}_stage",
            stage_path,
            role,
            database,
            schema,
            overwrite,
        )

        result = self.runSql(
            "get_streamlit_url",
            {
                "name": name,
                "role": role,
                "database": database,
                "schema": schema,
            },
        )

        return result.fetchone()[0]

    def describeStreamlit(self, name, database, schema, role, warehouse):
        description = self.runSql(
            "describe_streamlit",
            {
                "name": name,
                "role": role,
                "database": database,
                "schema": schema,
                "warehouse": warehouse,
            },
        )
        url = self.runSql(
            "get_streamlit_url",
            {
                "name": name,
                "role": role,
                "database": database,
                "schema": schema,
            },
        )
        return (description, url)

    def runSql(
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

    def generate_signature_from_params(self, params: str) -> str:
        if params == "()":
            return "()"
        return "(" + " ".join(params.split()[1::2])
