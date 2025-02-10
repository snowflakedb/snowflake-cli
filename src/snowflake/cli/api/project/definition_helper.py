from pathlib import Path
from typing import Optional

from click import UsageError
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import NoProjectDefinitionError


def get_entity(
    pd, project_root: Path, entity_type: ObjectType, entity_id: Optional[str] = None
):
    entities = pd.get_entities_by_type(entity_type=entity_type.value.cli_name)

    if not entities:
        raise NoProjectDefinitionError(
            project_type=entity_type.value.sf_name, project_root=project_root
        )

    if entity_id and entity_id not in entities:
        raise UsageError(f"No '{entity_id}' entity in project definition file.")
    elif len(entities.keys()) == 1:
        entity_id = list(entities.keys())[0]

    if entity_id is None:
        raise UsageError(
            f"Multiple {entity_type.value.sf_plural_name} found. Please provide entity id for the operation."
        )
    return entities[entity_id]
