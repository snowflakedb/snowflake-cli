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

"""Notebook plugin implementation."""

from __future__ import annotations

import typer
from click import UsageError

from snowflake.cli._plugins.notebook.interface import NotebookHandler
from snowflake.cli._plugins.notebook.manager import NotebookManager
from snowflake.cli._plugins.notebook.notebook_entity_model import NotebookEntityModel
from snowflake.cli._plugins.notebook.types import NotebookStagePath
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.utils import get_entity_for_operation
from snowflake.cli.api.entities.common import EntityActions
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult, MessageResult


class NotebookHandlerImpl(NotebookHandler):

    def execute(self, identifier: FQN) -> CommandResult:
        _ = NotebookManager().execute(notebook_name=identifier)
        return MessageResult(f"Notebook {identifier} executed.")

    def get_url(self, identifier: FQN) -> CommandResult:
        url = NotebookManager().get_url(notebook_name=identifier)
        return MessageResult(message=url)

    def open_notebook(self, identifier: FQN) -> CommandResult:
        url = NotebookManager().get_url(notebook_name=identifier)
        typer.launch(url)
        return MessageResult(message=url)

    def create(
        self, identifier: FQN, notebook_file: NotebookStagePath
    ) -> CommandResult:
        notebook_url = NotebookManager().create(
            notebook_name=identifier,
            notebook_file=notebook_file,
        )
        return MessageResult(message=notebook_url)

    def deploy(
        self, entity_id: str, replace: bool, prune: bool
    ) -> CommandResult:
        cli_context = get_cli_context()
        pd = cli_context.project_definition
        if not pd.meets_version_requirement("2"):
            raise UsageError(
                "This command requires project definition of version at least 2."
            )

        notebook: NotebookEntityModel = get_entity_for_operation(
            cli_context=cli_context,
            entity_id=entity_id,
            project_definition=pd,
            entity_type="notebook",
        )
        ws = WorkspaceManager(
            project_definition=cli_context.project_definition,
            project_root=cli_context.project_root,
        )
        notebook_url = ws.perform_action(
            notebook.entity_id,
            EntityActions.DEPLOY,
            replace=replace,
            prune=prune,
        )
        return MessageResult(
            f"Notebook successfully deployed and available under {notebook_url}"
        )
