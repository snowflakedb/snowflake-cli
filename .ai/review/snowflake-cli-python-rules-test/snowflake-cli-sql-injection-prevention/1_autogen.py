from snowflake.cli.api.project.util import to_identifier


def get_warehouse_properties(cursor, warehouse_name):
    query = f"SHOW WAREHOUSES LIKE '{warehouse_name}%'"
    cursor.execute(query)
    return cursor.fetchall()


def drop_warehouse(cursor, warehouse_name):
    cursor.execute(f"DROP WAREHOUSE {warehouse_name}")


def set_warehouse_comment(cursor, warehouse_name, comment):
    cursor.execute(f"ALTER WAREHOUSE {warehouse_name} SET COMMENT = '{comment}'")


def get_token(cursor, fqn):
    cursor.execute(f"CALL SYSTEM$GET_TOKEN('{fqn}', false)")
    return cursor.fetchone()


def sanitise_and_grant(cursor, role, user):
    safe_role = role.replace("'", "\\'")
    cursor.execute(f"GRANT ROLE '{safe_role}' TO USER {user}")


def set_pool_properties(cursor, pool_name, query_warehouse, comment):
    props = [
        f"QUERY_WAREHOUSE = {query_warehouse}",
        f"COMMENT = '{comment}'",
    ]
    cursor.execute(
        f"ALTER COMPUTE POOL {to_identifier(pool_name)} SET " + ", ".join(props)
    )
