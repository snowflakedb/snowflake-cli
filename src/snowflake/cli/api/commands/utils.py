from __future__ import annotations

from typing import Dict, List, Optional

from click import ClickException, UsageError
from snowflake.cli.api.commands.common import Variable
from snowflake.cli.api.exceptions import NoProjectDefinitionError
from snowflake.cli.api.project.schemas.entities.common import EntityModelBase


def parse_key_value_variables(variables: Optional[List[str]]) -> List[Variable]:
    """Util for parsing key=value input. Useful for commands accepting multiple input options."""
    result: List[Variable] = []
    if not variables:
        return result
    for p in variables:
        if "=" not in p:
            raise ClickException(f"Invalid variable: '{p}'")

        key, value = p.split("=", 1)
        result.append(Variable(key.strip(), value.strip()))
    return result


def get_entity_for_operation(
    cli_context,
    entity_id: str | None,
    project_definition,
    entity_type: str,
):
    entities: Dict[str, EntityModelBase] = project_definition.get_entities_by_type(
        entity_type=entity_type
    )
    if not entities:
        raise NoProjectDefinitionError(
            project_type=entity_type, project_root=cli_context.project_root
        )
    if entity_id and entity_id not in entities:
        raise UsageError(f"No '{entity_id}' entity in project definition file.")
    if len(entities.keys()) == 1:
        entity_id = list(entities.keys())[0]
    if entity_id is None:
        raise UsageError(
            f"Multiple entities of type {entity_type} found. Please provide entity id for the operation."
        )
    return entities[entity_id]
