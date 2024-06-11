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

    def create_deploy_plan_sql(self):
        put_files_sql = "\n".join(
            f"PUT file://output/deploy/stage1/{artifact['dest']} @TEST_APP.PUBLIC.stage1 auto_compress=false overwrite=True;"
            for artifact in self.stages["stage1"]
        )
        sql_lines = "\n".join(self.sql)
        return f"""-- Upload files to stages
CREATE STAGE IF NOT EXISTS TEST_APP.PUBLIC.stage1;
{put_files_sql}

{sql_lines}"""

    def __str__(self):
        stages_str = ""
        for stage_name, artifacts in self.stages.items():
            stages_str += stage_name + ":\n"
            stages_str += "\n".join(
                f"- {artifact['dest']} ({artifact['src']})" for artifact in artifacts
            )
        return f"""
Stages:
{stages_str}

SQL:
{self.create_deploy_plan_sql()}
"""
