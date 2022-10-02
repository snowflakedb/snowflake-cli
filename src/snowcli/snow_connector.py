import os
import pkgutil
from io import StringIO

import snowflake.connector

from snowcli.snowsql_config import SnowsqlConfig

class SnowflakeConnector():
    def __init__(self, *args):
        if len(args) == 3:
            self.ctx = snowflake.connector.connect(
                    user=args[1],
                    password=args[2],
                    account=args[0]
            )
        elif len(args) == 1:
            config = args[0]
            self.ctx = snowflake.connector.connect(**config)
        self.cs = self.ctx.cursor()

    """Initialize a connection from a snowsql-formatted config"""
    @classmethod
    def fromConfig(cls, path, connection_name):
        config = SnowsqlConfig(path)
        return cls(config.get_connection(connection_name))

    def getVersion(self):
        self.cs.execute('SELECT current_version()')
        return self.cs.fetchone()[0]

    def createFunction(self, name: str, inputParameters: str, returnType: str, handler: str, imports: str, database: str, schema: str, role: str, warehouse: str, overwrite: bool, packages: list[str]):
        return self.runSql('create_function', {
            'database': database,
            'schema': schema,
            'role': role,
            'warehouse': warehouse,
            'name': name,
            'overwrite': overwrite,
            'input_parameters': inputParameters,
            'return_type': returnType,
            'handler': handler,
            'imports': imports,
            'packages': packages,
            'signature': self.generate_signature_from_params(inputParameters)
        })

    def uploadFileToStage(self, file_path, destination_stage, path, role, overwrite):
        self.cs.execute(f'use role {role}')
        self.cs.execute(
            f'create stage if not exists {destination_stage} comment="deployments managed by snowcli"')
        self.cs.execute(
            f'PUT file://{file_path} @{destination_stage}{path} auto_compress=false overwrite={"true" if overwrite else "false"}')
        return self.cs.fetchone()[0]

    def executeFunction(self, function, database, schema, role, warehouse):
        self.cs.execute(f'use role {role}')
        self.cs.execute(f'use warehouse {warehouse}')
        self.cs.execute(f'use database {database}')
        self.cs.execute(f'use schema {schema}')
        self.cs.execute(f'select {function}')
        return self.cs.fetchall()

    def describeFunction(self, database, schema, role, warehouse, signature = None, name = None, inputParameters = None) -> list[tuple]:
        self.cs.execute(f'use role {role}')
        self.cs.execute(f'use warehouse {warehouse}')
        self.cs.execute(f'use database {database}')
        self.cs.execute(f'use schema {schema}')
        if signature:
            self.cs.execute(f'desc FUNCTION {signature}')
        elif name and inputParameters:
            self.cs.execute(f'desc FUNCTION {name}{self.generate_signature_from_params(inputParameters)}')
        return self.cs.fetchall()

    def listFunctions(self, database, schema, role, warehouse, like) -> list[tuple]:
        self.cs.execute(f'use role {role}')
        self.cs.execute(f'use warehouse {warehouse}')
        self.cs.execute(f'use database {database}')
        self.cs.execute(f'use schema {schema}')
        self.cs.execute(f'show USER FUNCTIONS LIKE \'{like}\'')
        return self.cs.fetchall()

    def listStreamlits(self, database="", schema="", role="", warehouse="", like='%%') -> list[tuple]:
        return self.runSql('list_streamlits', {
            'database': database,
            'schema': schema,
            'role': role,
            'warehouse': warehouse
        })

    def showWarehouses(self, database="", schema="", role="", warehouse="", like='%%') -> list[tuple]:
        return self.runSql('show_warehouses', {
            'database': database,
            'schema': schema,
            'role': role,
            'warehouse': warehouse
        })

    def createStreamlit(self, database="", schema="", role="", warehouse="", name="", file="") -> list[tuple]:
        return self.runSql('create_streamlit', {
            'database': database,
            'schema': schema,
            'role': role,
            'warehouse': warehouse,
            'name': name,
            'file_name': file
        })

    def deployStreamlit(self, name, file_path, stage_path, role, overwrite):
        self.uploadFileToStage(file_path, f"{name}_stage", stage_path, role, overwrite)
        return self.runSql("get_streamlit_url", { "name": name })

    def runSql(self, command, context):
        sql = pkgutil.get_data(__name__, f"sql/{command}.sql").decode()
        try:
            # if sql starts with f###
            if sql.startswith('f"""'):
                sql = eval(sql, context)
            else:
                sql.format(**context)

            if os.getenv('DEBUG'): print(f"Executing sql:\n{sql}")
            results = self.ctx.execute_stream(StringIO(sql))

            # Return result from last cursor
            *_, last_result = results
            return last_result
        except snowflake.connector.errors.ProgrammingError as e:
            print(f"Error executing sql:\n{sql}")
            print(e)
            raise(e)


    def generate_signature_from_params(self, params: str) -> str:
       return  '(' + ' '.join(params.strip('()').split()[1::2]) + ')'