class DeployPlan:
    def __init__(self):
        # TODO Split sql into sections
        self.sql = []
        self.deploy_roots = []
        # TODO Generated files?

    def add_sql(self, sql):
        self.sql.append(sql)

    def add_files(self, src, dest):
        self.deploy_roots.append({"src": src, "dest": dest})

    def __str__(self):
        files_str = "\n".join(
            f"{root['src']} -> {root['dest']}" for root in self.deploy_roots
        )
        sql_str = "\n".join(self.sql)
        return f"""
Files:
{files_str}

SQL:
{sql_str}
"""
