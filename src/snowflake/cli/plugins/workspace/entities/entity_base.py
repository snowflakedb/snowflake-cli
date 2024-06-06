from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict


class Entity(ABC):
    def __init__(self, entity_config: Dict):
        self.config = entity_config

    def create_deploy_plan(self, ctx, plan, *args, **kwargs):
        plan.add_sql(f"-- <{self.config['key']}>")
        self.compile_artifacts(ctx, plan)
        self.create_deploy_plan_impl(ctx, plan)
        plan.add_sql(f"-- </{self.config['key']}>\n")

    @abstractmethod
    def create_deploy_plan_impl(self, ctx, plan, *args, **kwargs):
        pass

    def compile_artifacts(self, ctx, plan, stage=None):
        if "meta" in self.config and "files" in self.config["meta"]:
            if not stage:
                stage = ctx.get_stage_name()
            artifacts = self.config["meta"]["files"]
            for artifact in artifacts:
                plan.add_artifact(artifact, stage)
