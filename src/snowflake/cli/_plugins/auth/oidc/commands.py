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
    OidcProviderTypeWithAuto,
)
from snowflake.cli._plugins.auth.oidc.manager import OidcManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.types import MessageResult

app = SnowTyperFactory(
    name="oidc",
    help="Manages OIDC authentication.",
)


UserNameOption = typer.Option(
    ...,
    "--user-name",
    show_default=False,
    help="Name for the user to create",
)

UserNameArgument = typer.Argument(
    ...,
    help="Name for the user to drop",
    show_default=False,
)

SubjectOption = typer.Option(
    ...,
    "--subject",
    show_default=False,
    help="OIDC subject string",
)

DefaultRoleOption = typer.Option(
    None,
    "--default-role",
    show_default=False,
    help="Default role to assign to the user",
)

ProviderTypeOption = typer.Option(
    ...,
    "--type",
    help=f"Type of OIDC provider to use",
    show_default=False,
)

AutoProviderTypeOption = typer.Option(
    OidcProviderTypeWithAuto.AUTO.value,
    "--type",
    help=f"Type of OIDC provider to use",
    show_default=False,
)

IssuerURLOption = typer.Option(
    ...,
    "--issuer",
    help="An issuer URL.",
    show_default=False,
)


@app.command("create-user", requires_connection=True)
def create_user(
    user_name: str = UserNameOption,
    issuer: str = IssuerURLOption,
    subject: str = SubjectOption,
    default_role: str = DefaultRoleOption,
    **options,
):
    """
    Sets up OIDC authentication.
    Creates a user with the specified configuration.
    """
    if user_name is None or not (user := user_name.strip()):
        raise CliError("User cannot be empty")
    if issuer is None or not (issuer := issuer.strip()):
        raise CliError("Issuer cannot be empty")
    if subject is None or not (subject := subject.strip()):
        raise CliError("Subject cannot be empty")
    if default_role is not None and not (default_role := default_role.strip()):
        raise CliError("Default role cannot be empty")

    result = OidcManager().create_user(
        user_name=user,
        issuer=issuer,
        subject=subject,
        default_role=default_role,
    )
    return MessageResult(result)


@app.command("delete", requires_connection=True)
def delete(
    user_name=UserNameArgument,
    **options,
):
    """
    Deletes a user.
    """
    result = OidcManager().delete(user=user_name)
    return MessageResult(result)


@app.command("read-token", requires_connection=False)
def read_token(
    _type: OidcProviderTypeWithAuto = AutoProviderTypeOption,
    **options,
):
    """
    Reads OIDC token based on the specified type.
    Use 'auto' to auto-detect available providers.
    """
    result = OidcManager().read_token(provider_type=_type)
    return MessageResult(result)
