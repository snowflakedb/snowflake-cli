from lib2to3.pgen2.pgen import DFAState
import snowflake.connector

class SnowflakeConnector():
    def __init__(self, account, username, password):
        self.ctx = snowflake.connector.connect(
            user=username,
            password=password,
            account=account
        )
        self.cs = self.ctx.cursor()

    def getVersion(self):
        self.cs.execute('SELECT current_version()')
        return self.cs.fetchone()[0] 

    def createFunction(self, name, inputParameters, returnType, handler, imports, database, schema, role, warehouse, overwrite):
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
        ''')
        return self.cs.fetchone()[0]