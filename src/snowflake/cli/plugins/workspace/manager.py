from __future__ import annotations

from typing import Dict

from snowflake.cli.plugins.workspace.deploy_context import DeployContext
from snowflake.cli.plugins.workspace.deploy_plan import DeployPlan


class WorkspaceManager:
    def __init__(self, workspace_definition: Dict):
        self.ctx = DeployContext(workspace_definition)
        for key, entity_config in workspace_definition["entities"].items():
            self.ctx.register_entity(key, entity_config)
        # TODO Register nested entities
        self.ctx.register_entity(
            "pkg.ui", workspace_definition["entities"]["pkg"]["children"][0]
        )

    def deploy(self, key: str):
        # TODO Build dependency graph & topological sort
        if key == "ui":
            deploy_order = [
                self.ctx.get_entity("ui"),
            ]
        elif key == "app":
            deploy_order = [
                self.ctx.get_entity("pkg"),
                self.ctx.get_entity("app"),
            ]
        elif key == "pkg":
            deploy_order = [
                self.ctx.get_entity("pkg"),
            ]

        plan = DeployPlan()
        for entity in deploy_order:
            entity.create_deploy_plan(self.ctx, plan)
        return plan
