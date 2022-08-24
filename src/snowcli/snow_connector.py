from lib2to3.pgen2.pgen import DFAState
import os
import snowflake.connector
import pkgutil
from snowcli.snowsql_config import SnowsqlConfig
from io import StringIO

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
        return cls(config.getConnection(connection_name))

    def getVersion(self):
        self.cs.execute('SELECT current_version()')
        return self.cs.fetchone()[0]

    def createFunction(self, name: str, inputParameters: str, returnType: str, handler: str, imports: str, database: str, schema: str, role: str, warehouse: str, overwrite: bool, packages: list[str]):
        self.cs.execute(f'use role {role}')
        self.cs.execute(f'use warehouse {warehouse}')
        self.cs.execute(f'use database {database}')
        self.cs.execute(f'''
        CREATE {"OR REPLACE " if overwrite else ""} FUNCTION {schema}.{name}({inputParameters})
         RETURNS {returnType}
         LANGUAGE PYTHON
         RUNTIME_VERSION=3.8
         IMPORTS=('{imports}')
         HANDLER='{handler}'
         PACKAGES=({','.join(["'{}'".format(package)
                   for package in packages]) if packages else ""})
        ''')

        return self.cs.fetchone()[0]

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

    def describeFunction(self, function, database, schema, role, warehouse) -> list[tuple]:
        self.cs.execute(f'use role {role}')
        self.cs.execute(f'use warehouse {warehouse}')
        self.cs.execute(f'use database {database}')
        self.cs.execute(f'use schema {schema}')
        self.cs.execute(f'desc FUNCTION {function}')
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
        try:
            sql = pkgutil.get_data(__name__, f"sql/{command}.sql").decode()
            for (k, v) in context.items():
                sql = sql.replace("{" + k + "}", v)

            if os.getenv('DEBUG'): print(f"Executing sql:\n{sql}")
            results = self.ctx.execute_stream(StringIO(sql))

            # Return result from last cursor
            *_, last_result = results
            return last_result
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)

