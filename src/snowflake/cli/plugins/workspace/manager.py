from pathlib import Path
from typing import Dict

from snowflake.cli.api.exceptions import InvalidProjectDefinitionVersion20Error
from snowflake.cli.api.project.schemas.entities.entities import (
    Entity,
    v2_entity_model_to_entity_map,
)
from snowflake.cli.api.project.schemas.project_definition import DefinitionV20


class WorkspaceManager:
    def __init__(self, project_definition: DefinitionV20, project_root: Path):
        if project_definition.definition_version != "2":
            raise InvalidProjectDefinitionVersion20Error(
                project_definition.definition_version
            )
        self._entities_cache: Dict[str, Entity] = {}
        self._pdf = project_definition

    def get_entity(self, key: str):
        if key not in self._entities_cache:
            if key not in self._pdf.entities:
                raise ValueError(f"No such entity key: {key}")
            entity_model_cls = self._pdf.entities[key].__class__
            entity_cls = v2_entity_model_to_entity_map[entity_model_cls]
            self._entities_cache[key] = entity_cls()
        return self._entities_cache[key]

    def bundle(self, key: str):
        entity = self.get_entity(key)
        if callable(getattr(entity, "bundle", None)):
            entity.bundle()
        else:
            raise ValueError("This entity type does not support bundling")
