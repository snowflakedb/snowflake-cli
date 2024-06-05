from __future__ import annotations

from snowflake.cli.plugins.workspace.entities.entity_base import Entity


class StreamlitEntity(Entity):
    def __init__(self, entity_config):
        super().__init__(entity_config)

    def create_deploy_plan(self, ctx, plan, parent=None, *args, **kwargs):
        if not parent and "stage" in self.config:
            stage = self.config["stage"]
        else:
            stage = ctx.get_stage_name()
        self.compile_artifacts(ctx, plan, stage)
        plan.add_sql(f"-- <{self.config['key']}>")
        plan.add_sql(f"USE SCHEMA {ctx.get_schema_name()};")
        if parent:
            plan.add_sql("-- Do something else if deployed as a child")
        plan.add_sql(f"CREATE STAGE IF NOT EXISTS {stage};")
        plan.add_sql(f"CREATE STREAMLIT IF NOT EXISTS {self.config['name']};")
        plan.add_sql(f"-- </{self.config['key']}>")
