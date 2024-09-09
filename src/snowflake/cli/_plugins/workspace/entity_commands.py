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

import logging
from pathlib import Path
from typing import Callable

from click import ClickException
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.snow_typer import (
    SnowTyper,
    SnowTyperCommandData,
    SnowTyperFactory,
)
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.project.schemas.entities.entities import (
    v2_entity_model_to_entity_map,
    v2_entity_model_types_map,
)

logger = logging.getLogger(__name__)


class EntityCommandGroup(SnowTyperFactory):
    help: str  # noqa: A003
    target_id: str
    _tree_path: list[str]
    _command_map: dict[str, SnowTyperCommandData]
    _subtree_map: dict[str, "EntityCommandGroup"]

    def __init__(
        self,
        name: str,
        target_id: str,
        help_text: str | None = None,
        tree_path: list[str] = [],
    ):
        super().__init__(name=name, help=help_text)
        self.target_id = target_id
        self._tree_path = tree_path
        self._command_map = {}
        self._subtree_map = {}

    def command(self, name: str, *args, **kwargs):
        """Assume the first arg is the command name, unlike superclass"""

        def decorator(command):
            cmd_data = SnowTyperCommandData(command, args=[name, *args], kwargs=kwargs)
            self.commands_to_register.append(cmd_data)
            self._command_map[name] = cmd_data
            return command

        return decorator

    def create_instance(self) -> SnowTyper:
        """Provides a default help value generated based on sub-groups and commands."""
        if not self.help:
            subcommands = sorted(
                [
                    *[f"`{x}`" for x in self._subtree_map.keys()],
                    *self._command_map.keys(),
                ]
            )
            self.help = "\+ " + ", ".join(subcommands)

        return super().create_instance()

    def new_subtree(self, atom: str) -> "EntityCommandGroup":
        if atom in self._subtree_map:
            logger.error("Duplicate subtree attempted to be created: %s", atom)
        else:
            subtree = EntityCommandGroup(
                atom, target_id=self.target_id, tree_path=[*self._tree_path, atom]
            )
            self._subtree_map[atom] = subtree
            self.add_typer(subtree)

        return self._subtree_map[atom]

    def _get_subtree(self, group_path: list[str]) -> "EntityCommandGroup":
        """
        Gets a group subtree factory for a sub-tree of this command group.
        Creates groups on-the-fly (i.e. mkdir -p semantics).
        """
        subtree = self
        for atom in group_path:
            if atom in self._subtree_map:
                subtree = self._subtree_map[atom]
            else:
                subtree = subtree.new_subtree(atom)
        return subtree

    def register_command_leaf(
        self, name: str, action: EntityActions, action_callable: Callable
    ):
        """Registers the provided action at the given name"""

        @self.command(name)
        @with_project_definition()
        def _action_executor(**options) -> CommandResult:
            # TODO: get args for action and turn into typer options
            # TODO: what message result are we returning? do we throw them away for multi-step actions (i.e deps?)
            # TODO: how do we know if a command needs connection?

            cli_context = get_cli_context()
            ws = WorkspaceManager(
                project_definition=cli_context.project_definition,
                project_root=cli_context.project_root,
            )
            # entity = ws.get_entity(self.target_id)
            ws.perform_action(self.target_id, action)
            return MessageResult(
                f"Successfully performed {action.verb} on {self.target_id}."
            )

        _action_executor.__doc__ = action_callable.__doc__

    def register_command_in_tree(
        self, action: EntityActions, action_callable: Callable
    ):
        """
        Recurses into subtrees created on-demand to register
        an action based on its command path and implementation.
        """
        [*group_path, verb] = action.command_path
        subtree = self._get_subtree(group_path)
        subtree.register_command_leaf(verb, action, action_callable)


def generate_entity_commands(
    ws: SnowTyperFactory, project_root: Path | str | None = None
):
    """
    Introspect the current snowflake.yml file, generating @<id> command groups
    for each found entity. Throws a fatal ClickException if templating is used
    in the basic definition of entities (i.e. the type discriminator field) or
    the type is not found in the entity types map.
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

        tree_group = EntityCommandGroup(
            f"@{target_id}",
            target_id=target_id,
            help_text=f"Commands to interact with the {target_id} entity defined in {DefinitionManager.BASE_DEFINITION_FILENAME}.",
        )

        model_type = v2_entity_model_types_map[model.type]
        entity_type = v2_entity_model_to_entity_map[model_type]
        supported_actions = sorted(
            [action for action in EntityActions if entity_type.supports(action)]
        )
        for action in supported_actions:
            tree_group.register_command_in_tree(
                action, entity_type.get_action_callable(action)
            )

        # TODO: hide, by default
        ws.add_typer(tree_group)
