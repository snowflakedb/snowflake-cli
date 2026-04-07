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

"""Notebook plugin interface.

Defines the command surface for ``snow notebook`` commands.
This file is reviewed independently before implementation begins.

Commands
--------
- ``snow notebook execute <identifier>``  — Execute a notebook headlessly.
- ``snow notebook get-url <identifier>``  — Get the notebook's Snowsight URL.
- ``snow notebook open <identifier>``     — Open the notebook in a browser.
- ``snow notebook create <identifier> --notebook-file <path>`` — Create from stage.
- ``snow notebook deploy [entity_id] --replace --prune`` — Deploy from project definition.
"""

from __future__ import annotations

from abc import abstractmethod

from snowflake.cli._plugins.notebook.types import NotebookStagePath
from snowflake.cli.api.commands.flags import IdentifierType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    ParamDef,
    ParamKind,
    REQUIRED,
)

# ---------------------------------------------------------------------------
# Command surface (reviewable spec)
# ---------------------------------------------------------------------------

NOTEBOOK_SPEC = CommandGroupSpec(
    name="notebook",
    help="Manages notebooks in Snowflake.",
    parent_path=(),
    commands=(
        CommandDef(
            name="execute",
            help="Executes a notebook in a headless mode.",
            handler_method="execute",
            requires_connection=True,
            params=(
                ParamDef(
                    name="identifier",
                    type=FQN,
                    kind=ParamKind.ARGUMENT,
                    help="Identifier of the notebook; for example: MY_NOTEBOOK",
                    show_default=False,
                    click_type=IdentifierType(),
                ),
            ),
            output_type="MessageResult",
        ),
        CommandDef(
            name="get-url",
            help="Return a url to a notebook.",
            handler_method="get_url",
            requires_connection=True,
            params=(
                ParamDef(
                    name="identifier",
                    type=FQN,
                    kind=ParamKind.ARGUMENT,
                    help="Identifier of the notebook; for example: MY_NOTEBOOK",
                    show_default=False,
                    click_type=IdentifierType(),
                ),
            ),
            output_type="MessageResult",
        ),
        CommandDef(
            name="open",
            help="Opens a notebook in default browser.",
            handler_method="open_notebook",
            requires_connection=True,
            params=(
                ParamDef(
                    name="identifier",
                    type=FQN,
                    kind=ParamKind.ARGUMENT,
                    help="Identifier of the notebook; for example: MY_NOTEBOOK",
                    show_default=False,
                    click_type=IdentifierType(),
                ),
            ),
            output_type="MessageResult",
        ),
        CommandDef(
            name="create",
            help="Creates notebook from stage.",
            handler_method="create",
            requires_connection=True,
            params=(
                ParamDef(
                    name="identifier",
                    type=FQN,
                    kind=ParamKind.ARGUMENT,
                    help="Identifier of the notebook; for example: MY_NOTEBOOK",
                    show_default=False,
                    click_type=IdentifierType(),
                ),
                ParamDef(
                    name="notebook_file",
                    type=NotebookStagePath,
                    kind=ParamKind.OPTION,
                    cli_names=("--notebook-file", "-f"),
                    help="Stage path with notebook file. "
                    "For example `@stage/path/to/notebook.ipynb`",
                    show_default=False,
                ),
            ),
            output_type="MessageResult",
        ),
        CommandDef(
            name="deploy",
            help="Uploads a notebook and required files to a stage "
            "and creates a Snowflake notebook.",
            handler_method="deploy",
            requires_connection=True,
            decorators=("with_project_definition",),
            params=(
                ParamDef(
                    name="entity_id",
                    type=str,
                    kind=ParamKind.ARGUMENT,
                    help="ID of notebook entity.",
                    required=False,
                    default=None,
                ),
                ParamDef(
                    name="replace",
                    type=bool,
                    kind=ParamKind.OPTION,
                    cli_names=("--replace",),
                    help="Replace notebook object if it already exists. "
                    "It only uploads new and overwrites existing files, "
                    "but does not remove any files already on the stage.",
                    is_flag=True,
                    default=False,
                    required=False,
                ),
                ParamDef(
                    name="prune",
                    type=bool,
                    kind=ParamKind.OPTION,
                    cli_names=("--prune/--no-prune",),
                    help="Delete files that exist in the stage, "
                    "but not in the local filesystem.",
                    is_flag=True,
                    default=False,
                    required=False,
                    show_default=True,
                ),
            ),
            output_type="MessageResult",
        ),
    ),
)


# ---------------------------------------------------------------------------
# Handler contract (ABC)
# ---------------------------------------------------------------------------


class NotebookHandler(CommandHandler):
    """Handler contract for notebook commands.

    Each abstract method corresponds to a ``CommandDef`` above via
    ``handler_method``.  The implementation class fills in the bodies.
    """

    @abstractmethod
    def execute(self, identifier: FQN) -> CommandResult:
        """Execute a notebook in headless mode."""
        ...

    @abstractmethod
    def get_url(self, identifier: FQN) -> CommandResult:
        """Return a notebook URL."""
        ...

    @abstractmethod
    def open_notebook(self, identifier: FQN) -> CommandResult:
        """Open a notebook in the browser."""
        ...

    @abstractmethod
    def create(
        self, identifier: FQN, notebook_file: NotebookStagePath
    ) -> CommandResult:
        """Create a notebook from a stage file."""
        ...

    @abstractmethod
    def deploy(
        self, entity_id: str, replace: bool, prune: bool
    ) -> CommandResult:
        """Deploy a notebook from project definition."""
        ...
