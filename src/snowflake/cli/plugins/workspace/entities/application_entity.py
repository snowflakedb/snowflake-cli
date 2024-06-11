from __future__ import annotations

from snowflake.cli.plugins.workspace.entities.entity_base import Entity


class ApplicationEntity(Entity):
    def __init__(self, entity_config):
        super().__init__(entity_config)

    def create_deploy_plan_impl(self, ctx, plan, *args, **kwargs):
        app_name = self.config["name"]
        pkg_entity = ctx.get_entity(self.config["from"])
        app_pkg_name = pkg_entity.config["name"]
        db = ctx.get_db_name()
        schema = ctx.get_schema_name()
        stage = ctx.get_stage_name()

        plan.add_sql(
            f"CREATE APPLICATION IF NOT EXISTS {app_name} FROM application package {app_pkg_name} USING '@{db}.{schema}.{stage}';"
        )
