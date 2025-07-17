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
from snowflake.cli._app.auth.oidc_providers import (
    OidcProviderType,
)
from snowflake.cli._plugins.auth.workload_identity.manager import (
    WorkloadIdentityManager,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult

app = SnowTyperFactory(
    name="workload-identity",
    help="Manages GitHub workload identity federation authentication.",
)


@app.command("setup", requires_connection=True)
def setup(
    github_repository: str = typer.Option(
        ...,
        "--github-repository",
        show_default=False,
        help="GitHub repository in format 'owner/repo'",
        prompt="Enter GitHub repository (owner/repo)",
    ),
    **options,
):
    """
    Sets up GitHub workload identity federation for authentication.
    """
    result = WorkloadIdentityManager().setup(github_repository=github_repository)
    return MessageResult(result)


@app.command("read", requires_connection=False)
def read(
    _type: str = typer.Option(
        "auto",
        "--type",
        help=f"Type of read operation to perform (e.g., '{OidcProviderType.GITHUB.value}', 'auto')",
    ),
    **options,
):
    """
    Reads OIDC token based on the specified type.
    Use 'auto' to auto-detect available providers.
    """
    result = WorkloadIdentityManager().read(provider_type=_type)
    return MessageResult(result)
