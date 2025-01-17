from functools import cached_property
from pathlib import Path
from typing import Dict

from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.entities.utils import EntityActions, get_sql_executor
from snowflake.cli.api.exceptions import InvalidProjectDefinitionVersionError
from snowflake.cli.api.project.definition import default_role
from snowflake.cli.api.project.schemas.entities.entities import (
    Entity,
    v2_entity_model_to_entity_map,
)
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV20,
    ProjectDefinition,
)
from snowflake.cli.api.project.util import to_identifier


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
        self._project_root = project_root

    def get_entity(self, entity_id: str):
        """
        Returns an entity instance with the given ID. If exists, reuses the previously returned instance, or instantiates a new one otherwise.
        """
        if entity_id in self._entities_cache:
            return self._entities_cache[entity_id]
        entity_model = self._project_definition.entities.get(entity_id, None)
        if entity_model is None:
            raise ValueError(f"No such entity ID: {entity_id}")
        entity_model_cls = entity_model.__class__
        entity_cls = v2_entity_model_to_entity_map[entity_model_cls]
        workspace_ctx = WorkspaceContext(
            console=cc,
            project_root=self.project_root,
            get_default_role=_get_default_role,
            get_default_warehouse=_get_default_warehouse,
        )
        self._entities_cache[entity_id] = entity_cls(entity_model, workspace_ctx)
        return self._entities_cache[entity_id]

    def perform_action(self, entity_id: str, action: EntityActions, *args, **kwargs):
        """
        Instantiates an entity of the given ID and calls the given action on it.
        """
        entity = self.get_entity(entity_id)
        if entity.supports(action):
            return entity.perform(action, self.action_ctx, *args, **kwargs)
        else:
            raise ValueError(f'This entity type does not support "{action.value}"')

    @property
    def project_root(self) -> Path:
        return self._project_root

    @cached_property
    def action_ctx(self) -> ActionContext:
        return ActionContext(
            get_entity=self.get_entity,
        )


def _get_default_role() -> str:
    role = default_role()
    if role is None:
        role = get_sql_executor().current_role()
    return role


def _get_default_warehouse() -> str | None:
    warehouse = get_cli_context().connection.warehouse
    if warehouse:
        warehouse = to_identifier(warehouse)
    return warehouse
