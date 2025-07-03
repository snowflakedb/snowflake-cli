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

import typer
from snowflake.cli._plugins.auth.workflow_identity.manager import (
    WorkflowIdentityManager,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    SingleQueryResult,
)

# Main workflow-identity app
app = SnowTyperFactory(
    name="workflow-identity",
    help="Manages GitHub workflow identity federation authentication.",
)


def _setup_command(
    github_repository: str = typer.Option(
        ...,
        "--github-repository",
        help="GitHub repository in format 'owner/repo'",
        prompt="Enter GitHub repository (owner/repo)",
    ),
    **options,
):
    """
    Sets up GitHub workflow identity federation for authentication.
    """
    WorkflowIdentityManager().setup(github_repository=github_repository)
    return MessageResult("GitHub workflow identity federation setup completed.")


def _status_command(**options) -> CommandResult:
    """
    Shows the status of GitHub workflow identity federation configuration.
    """
    result = WorkflowIdentityManager().status()
    return MessageResult(f"GitHub workflow identity federation status: {result}")


def _remove_command(**options) -> CommandResult:
    """
    Removes the GitHub workflow identity federation configuration.
    """
    return SingleQueryResult(WorkflowIdentityManager().remove())


# Register commands on both apps
@app.command("setup", requires_connection=True)
def setup(
    github_repository: str = typer.Option(
        ...,
        "--github-repository",
        help="GitHub repository in format 'owner/repo'",
        prompt="Enter GitHub repository (owner/repo)",
    ),
    **options,
):
    return _setup_command(github_repository=github_repository, **options)


@app.command("status", requires_connection=True)
def status(**options):
    return _status_command(**options)


@app.command("remove", requires_connection=True)
def remove(**options):
    return _remove_command(**options)
