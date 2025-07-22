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
from snowflake.cli.api.output.types import MessageResult, QueryResult

app = SnowTyperFactory(
    name="workload-identity",
    help="Manages workload identity federation authentication.",
)

# Option definitions
FederatedUserOption = typer.Option(
    ...,
    "--federated-user",
    show_default=False,
    help="Name for the federated user to create",
    prompt="Enter federated user name",
)

FederatedUserDeleteOption = typer.Option(
    ...,
    "--federated-user",
    show_default=False,
    help="Name of the federated user to delete",
    prompt="Enter federated user name to delete",
)

SubjectOption = typer.Option(
    ...,
    "--subject",
    show_default=False,
    help="OIDC subject string",
    prompt="Enter OIDC subject string",
)

DefaultRoleOption = typer.Option(
    ...,
    "--default-role",
    show_default=False,
    help="Default role to assign to the federated user",
    prompt="Enter default role",
)

ProviderTypeOption = typer.Option(
    ...,
    "--type",
    help=f"Type of OIDC provider to use (e.g., '{OidcProviderType.GITHUB.value}')",
    prompt="Enter OIDC provider type",
    show_default=False,
)


@app.command("setup", requires_connection=True)
def setup(
    _type: str = ProviderTypeOption,
    federated_user: str = FederatedUserOption,
    subject: str = SubjectOption,
    default_role: str = DefaultRoleOption,
    **options,
):
    """
    Sets up workload identity federation for authentication.
    Creates a federated user with the specified configuration.
    """
    result = WorkloadIdentityManager().setup(
        user=federated_user,
        subject=subject,
        default_role=default_role,
        provider_type=_type,
    )
    return MessageResult(result)


@app.command("delete", requires_connection=True)
def delete(
    federated_user: str = FederatedUserDeleteOption,
    **options,
):
    """
    Deletes a federated user.
    """
    result = WorkloadIdentityManager().delete(user=federated_user)
    return MessageResult(result)


@app.command("read-token", requires_connection=False)
def read_token(
    _type: str = ProviderTypeOption,
    **options,
):
    """
    Reads OIDC token based on the specified type.
    Use 'auto' to auto-detect available providers.
    """
    result = WorkloadIdentityManager().read(provider_type=_type)
    return MessageResult(result)


@app.command("list", requires_connection=True)
def list_users(
    **options,
):
    """
    Lists users with workload identity enabled.
    """
    result = WorkloadIdentityManager().get_users_list()
    return QueryResult(result)
