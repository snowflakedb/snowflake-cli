# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path

from click import ClickException
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.schemas.entities.entities import (
    Entity,
    EntityModel,
    v2_entity_model_to_entity_map,
    v2_entity_model_types_map,
)


class EntityCommandGroup(SnowTyperFactory):
    target_id: str
    model_type: EntityModel
    entity_type: Entity

    def __init__(self, target_id: str, model_type_str: str):
        super().__init__(
            name=f"@{target_id}",
            help=f"Commands to interact with the {target_id} entity defined in {DefinitionManager.BASE_DEFINITION_FILENAME}.",
        )
        self.target_id = target_id
        self.model_type_str = model_type_str
        self.model_type = v2_entity_model_types_map[model_type_str]
        self.entity_type = v2_entity_model_to_entity_map[self.model_type]

    @property
    def supported_actions(self):
        return sorted(
            [action for action in EntityActions if self.entity_type.supports(action)]
        )

    def register_commands(self):
        for action in self.supported_actions:
            verb = action.value.split("action_")[1]
            action_callable = getattr(self.entity_type, action)

            # TODO: get args for action and turn into typer options
            # TODO: what message result are we returning? do we throw them away for multi-step actions (i.e deps?)
            # TODO: how do we know if a command needs connection?

            @self.command(verb)
            @with_project_definition()
            def _action_executor(**options) -> CommandResult:
                cli_context = get_cli_context()
                ws = WorkspaceManager(
                    project_definition=cli_context.project_definition,
                    project_root=cli_context.project_root,
                )
                # entity = ws.get_entity(self.target_id)
                ws.perform_action(self.target_id, action)
                return MessageResult(
                    f"Successfully performed {verb} on {self.target_id}."
                )

            _action_executor.__doc__ = action_callable.__doc__


def generate_entity_commands(
    ws: SnowTyperFactory, project_root: Path | str | None = None
):
    """
    Introspect the current snowflake.yml file, generating @<id> command groups
    for each found entity. Throws a fatal ClickException if templating is used
    in the basic definition of entities (i.e. the type discriminator field).
    """
    dm = DefinitionManager(str(project_root) if project_root is not None else None)

    if not dm.has_definition_file:
        return

    root = dm.unrendered_project_definition
    for (target_id, model) in root.entities.items():
        if not model.type in v2_entity_model_types_map:
            raise ClickException(
                f'Cannot parse {DefinitionManager.BASE_DEFINITION_FILENAME}: entity "{target_id}" has unknown type: {model.type}'
            )

        subgroup = EntityCommandGroup(target_id, model.type)
        subgroup.register_commands()
        ws.add_typer(subgroup)
