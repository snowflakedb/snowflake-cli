from __future__ import annotations

from snowflake.cli.plugins.nativeapp.constants import SPECIAL_COMMENT
from snowflake.cli.plugins.workspace.deploy_plan import DeployPlan
from snowflake.cli.plugins.workspace.entities.entity_base import Entity


class ApplicationPackageEntity(Entity):
    def __init__(self, entity_config):
        super().__init__(entity_config)

    def create_deploy_plan_impl(self, ctx, plan, *args, **kwargs):
        plan.add_sql(f"CREATE SCHEMA IF NOT EXISTS {ctx.get_schema_name()};")

        children_plan = DeployPlan()
        for child_config in self.config["children"]:
            child_entity = ctx.get_entity(child_config["key"])
            # TODO Create new context
            child_entity.create_deploy_plan(ctx, children_plan, self)

        # Add children artifacts to the main plan
        for stage_name, artifacts in children_plan.stages.items():
            for artifact in artifacts:
                plan.add_artifact(artifact, stage_name)

        # Modify setup script
        setup_script_path = f"output/deploy/{ctx.get_stage_name()}/setup_script.sql"
        with open(setup_script_path, "w") as setup_script_file:
            setup_script_file.write(
                f"""
-- Existing code
-- ...
-- ...

-- Generated
CREATE APPLICATION ROLE IF NOT EXISTS app_public;
CREATE OR ALTER VERSIONED SCHEMA core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_public;
"""
            )
            # Add children sql to setup script
            for sql in children_plan.sql:
                setup_script_file.write(sql)
                setup_script_file.write("\n")
            # TODO
            setup_script_file.write(
                "GRANT USAGE ON STREAMLIT core.ui TO APPLICATION ROLE app_public;"
            )

        # Existing code:
        # 1. NativeAppRunProcessor.build_bundle()
        # 2. NativeAppManager.deploy()
        #    - create_app_package()
        #    - _apply_package_scripts()
        #    - sync_deploy_root_with_stage()
        # 3. NativeAppRunProcessor._create_dev_app()

        package_name = self.config["name"]
        plan.add_sql(
            f"""CREATE APPLICATION PACKAGE IF NOT EXISTS {package_name}
    comment = {SPECIAL_COMMENT}
    distribution = internal;"""
        )
