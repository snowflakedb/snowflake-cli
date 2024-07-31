from pathlib import Path

from snowflake.cli.api.exceptions import InvalidProjectDefinitionVersion20Error
from snowflake.cli.api.project.schemas.entities.entities import (
    v2_entity_model_to_entity_map,
)
from snowflake.cli.api.project.schemas.project_definition import DefinitionV20


class WorkspaceManager:
    def __init__(self, project_definition: DefinitionV20, project_root: Path):
        if project_definition.definition_version != "2":
            raise InvalidProjectDefinitionVersion20Error(
                project_definition.definition_version
            )
        self._pdf = project_definition

    def get_entity(self, key: str):
        entity_schema = self._pdf.entities[key]  # TODO Verify
        entity = v2_entity_model_to_entity_map[entity_schema.__class__]  # TODO Verify
        return entity

    def bundle(self, key: str):
        entity = self.get_entity(key)
        if callable(getattr(entity, "bundle", None)):
            entity.bundle()
        else:
            raise ValueError("This entity type does not support bundling")
