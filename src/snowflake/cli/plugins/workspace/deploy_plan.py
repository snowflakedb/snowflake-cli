class DeployPlan:
    def __init__(self):
        # TODO Split sql into sections
        self.sql = []
        self.stages = {}
        # TODO Generated files?

    def add_sql(self, sql):
        self.sql.append(sql)

    def add_artifact(self, artifact, stage_name):
        if stage_name not in self.stages:
            self.stages[stage_name] = []
        self.stages[stage_name].append(artifact)

    def __str__(self):
        stages_str = ""
        for stage_name, stage in self.stages.items():
            stages_str += f"{stage_name}:\n"
            stages_str += "\n".join(
                f"- {artifact['dest']} ({artifact['src']})" for artifact in stage
            )
        sql_str = "\n".join(self.sql)
        return f"""
Stages:
{stages_str}

SQL:

-- Upload files to stages
CREATE STAGE IF NOT EXISTS db_name.schema_name.stage_name;
PUT file://local_file_path @db_name.schema_name.stage_name/path;

{sql_str}
"""
