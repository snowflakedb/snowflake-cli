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