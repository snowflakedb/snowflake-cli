from __future__ import annotations

from typing import Any, Dict

import inquirer


def select_entity_prompt(
    entities: Dict[str, Any], provided_entity_id: str | None = None
) -> str:
    if provided_entity_id:
        return provided_entity_id

    entities_ids = list(entities)
    # If there are multiple entities of the same type, ask which should be selected
    if len(entities_ids) > 1:
        return inquirer.prompt(
            [
                inquirer.List("entity_id", message="Entity ID", choices=list(entities)),
            ],
            raise_keyboard_interrupt=True,
        )["entity_id"]

    # If there's only one entity then pick it
    return entities_ids[0]
