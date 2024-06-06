from __future__ import annotations

from snowflake.cli.plugins.workspace.entities.entity_base import Entity


class ApplicationPackageEntity(Entity):
    def __init__(self, entity_config):
        super().__init__(entity_config)

    def create_deploy_plan_impl(self, ctx, plan, *args, **kwargs):
        plan.add_sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.get_schema_name()};")

        for child_config in self.config["children"]:
            child_entity = ctx.get_entity(child_config["key"])
            # TODO Create new context
            child_entity.create_deploy_plan(ctx, plan, self)

        plan.add_sql(f"CREATE APPLICATION PACKAGE IF NOT EXISTS {self.config['name']};")
