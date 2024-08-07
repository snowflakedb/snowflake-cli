from pathlib import Path
from typing import Dict

from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.exceptions import InvalidProjectDefinitionVersionError
from snowflake.cli.api.project.schemas.entities.entities import (
    Entity,
    v2_entity_model_to_entity_map,
)
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV20,
    ProjectDefinition,
)


class WorkspaceManager:
    """
    Instantiates entity instances from entity models, providing higher-order functionality on entity compositions.
    """

    def __init__(self, project_definition: ProjectDefinition, project_root: Path):
        if not project_definition.meets_version_requirement("2"):
            raise InvalidProjectDefinitionVersionError(
                "2.x", project_definition.definition_version
            )
        self._entities_cache: Dict[str, Entity] = {}
        self._project_definition: DefinitionV20 = project_definition

    def get_entity(self, entity_id: str):
        """
        Returns an entity instance with the given ID. If exists, reuses the previously returned instance, or instantiates a new one otherwise.
        """
        if entity_id in self._entities_cache:
            return self._entities_cache[entity_id]
        if entity_id not in self._project_definition.entities:
            raise ValueError(f"No such entity ID: {entity_id}")
        entity_model_cls = self._project_definition.entities[entity_id].__class__
        entity_cls = v2_entity_model_to_entity_map[entity_model_cls]
        self._entities_cache[entity_id] = entity_cls()
        return self._entities_cache[entity_id]

    def perform_action(self, entity_id: str, action: EntityActions):
        """
        Instantiates an entity of the given ID and calls the given action on it.
        """
        entity = self.get_entity(entity_id)
        if entity.supports(action):
            getattr(entity, action)()
        else:
            raise ValueError(f'This entity type does not support "{action.value}"')
