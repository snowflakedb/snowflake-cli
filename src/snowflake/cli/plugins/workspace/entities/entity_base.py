from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict


class Entity(ABC):
    def __init__(self, entity_config: Dict):
        self.config = entity_config

    @abstractmethod
    def create_deploy_plan(self, ctx, plan, *args, **kwargs):
        pass

    def compile_artifacts(self, ctx, plan, stage=None):
        if "meta" in self.config and "files" in self.config["meta"]:
            if not stage:
                stage = ctx.get_stage_name()
            source = self.config["meta"]["files"]
            plan.add_files(source, f"output/deploy/{stage}")
