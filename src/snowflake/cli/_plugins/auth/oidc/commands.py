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
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult

app = SnowTyperFactory(
    name="oidc",
    help="Manages OIDC authentication.",
)


AutoProviderTypeOption = typer.Option(
    OidcProviderTypeWithAuto.AUTO.value,
    "--type",
    help=f"Type of OIDC provider to use",
    show_default=False,
)


@app.command(
    "read-token",
    requires_connection=False,
    docs=CommandDocs(
        related=(
            link("/developer-guide/snowflake-cli/index"),
            link(
                "/developer-guide/snowflake-cli/command-reference/auth-commands/overview",
                "Snowflake CLI command reference",
            ),
        ),
        usage_notes=(
            plain_text(
                "The ",
                code("snow auth read-token"),
                " command displays the OIDC token, which can be used for "
                "authentication in Snowflake operations. This command is primarily "
                "for retrieving the authentication token and must run within the "
                "supported CI/CD runner.",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "Display the OIDC token in the current CI/CD environment:"
                ),
                command="snow auth oidc read-token --type github",
            ),
        ),
    ),
)
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
