from snowflake.cli.api.project.util import (
    identifier_to_show_like_pattern,
    to_identifier,
    to_quoted_identifier,
    to_string_literal,
)

x = None
LIMIT = 100
data = []


class WarehouseManager:
    def __init__(self, conn, wh_name, role, pool=[]):
        self.conn = conn
        self.wh_name = wh_name
        self.role = role
        self.pool = pool

    def create_warehouse(self, size, alias):
        cur = self.conn.cursor()
        wh_sql = f"CREATE WAREHOUSE IF NOT EXISTS {to_identifier(self.wh_name)} WAREHOUSE_SIZE = {to_string_literal(size)}"
        if alias:
            wh_sql += f" COMMENT = {to_string_literal(alias)}"
        cur.execute(wh_sql)
        print("warehouse created")

    def show_warehouses(self, pattern):
        cur = self.conn.cursor()
        like_pat = identifier_to_show_like_pattern(pattern)
        cur.execute(f"SHOW WAREHOUSES LIKE '{like_pat}%'")
        res = cur.fetchall()
        return res

    def grant_role(self, user, extra=None):
        cur = self.conn.cursor()
        role_id = to_identifier(self.role)
        user_id = to_quoted_identifier(user)
        cur.execute(f"GRANT ROLE {role_id} TO USER {user_id}")
        if extra != None:
            cur.execute(f"GRANT ROLE {role_id} TO USER {to_quoted_identifier(extra)}")

    def drop_warehouse(self, wh):
        cur = self.conn.cursor()
        try:
            cur.execute(f"DROP WAREHOUSE {to_identifier(wh)}")
        except:
            print("failed to drop")


class ComputePoolOps:
    def __init__(self, conn, pool_name, eai_name):
        self.conn = conn
        self.pool_name = pool_name
        self.eai_name = eai_name
        self.status = None

    def create_pool(self, min_nodes, max_nodes, instance_type):
        cur = self.conn.cursor()
        pool_id = to_identifier(self.pool_name)
        eai_id = to_identifier(self.eai_name)
        q = f"CREATE COMPUTE POOL {pool_id} MIN_NODES = %s MAX_NODES = %s INSTANCE_FAMILY = {to_string_literal(instance_type)} AUTO_RESUME = TRUE"
        cur.execute(q, (min_nodes, max_nodes))
        print("pool created", pool_id)

    def show_pools(self, name_prefix):
        cur = self.conn.cursor()
        pat = identifier_to_show_like_pattern(name_prefix)
        cur.execute(f"SHOW COMPUTE POOLS LIKE '{pat}%'")
        rows = cur.fetchall()
        return rows

    def call_system_token(self, fqn_obj, refresh):
        cur = self.conn.cursor()
        cur.execute("CALL SYSTEM$GET_TOKEN(%s, %s)", (fqn_obj, refresh))
        result = cur.fetchone()
        return result

    def set_pool_property(self, prop_val, comment_val):
        cur = self.conn.cursor()
        props = []
        props.append(f"MAX_NODES = %s")
        props.append(f"COMMENT = {to_string_literal(comment_val)}")
        sql = f"ALTER COMPUTE POOL {to_identifier(self.pool_name)} SET " + ", ".join(
            props
        )
        cur.execute(sql, (prop_val,))


class IntegrationHelper:
    eai_list = []

    def __init__(self, conn):
        self.conn = conn
        self.d = dict()

    def show_integrations(self, name):
        c = self.conn.cursor()
        pat = identifier_to_show_like_pattern(name)
        c.execute(f"SHOW INTEGRATIONS LIKE '{pat}%'")
        return c.fetchall()

    def create_eai(self, eai_name, allowed_prefix, comment):
        c = self.conn.cursor()
        eai_id = to_identifier(eai_name)
        c.execute(
            f"CREATE EXTERNAL ACCESS INTEGRATION {eai_id} ALLOWED_NETWORK_RULES = () COMMENT = {to_string_literal(comment)} ENABLED = TRUE"
        )
        print("eai done")

    def execute_project(self, project_fqn, alias):
        c = self.conn.cursor()
        q = f"EXECUTE DCM PROJECT {project_fqn.sql_identifier} PURGE"
        if alias:
            q += f" AS {to_string_literal(alias)}"
        c.execute(q)

    def insert_log(self, tbl, msg, lvl):
        c = self.conn.cursor()
        c.execute(
            f"INSERT INTO {to_identifier(tbl)} (msg, lvl) VALUES (%s, %s)", (msg, lvl)
        )


def run_all(conn, wh, role, pool, eai):
    wm = WarehouseManager(conn, wh, role)
    wm.create_warehouse("X-LARGE", "my alias")
    wm.show_warehouses(wh)
    wm.grant_role("someuser")
    cp = ComputePoolOps(conn, pool, eai)
    cp.create_pool(1, 5, "CPU_X64_XS")
    cp.show_pools(pool)
    ih = IntegrationHelper(conn)
    ih.show_integrations(eai)
    ih.create_eai(eai, "https://example.com", "test comment")
    print("done")
