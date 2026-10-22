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

from __future__ import annotations

from snowflake.cli._plugins.auth.manager import AuthManager
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult


def register_list(app: SnowTyperFactory) -> None:
    @app.command(
        "list",
        requires_connection=False,
        docs=CommandDocs(
            related=(
                link(
                    "/user-guide/oauth-local-applications",
                    "Snowflake OAuth for local applications",
                ),
                link("/developer-guide/snowflake-cli/index"),
                link(
                    "/developer-guide/snowflake-cli/command-reference/connection-commands/list-connections",
                    "snow connection list",
                ),
            ),
            usage_notes=(
                plain_text(
                    "A connection that sets ",
                    code("oauth_client_id"),
                    " or ",
                    code("oauth_client_secret"),
                    " uses its own OAuth client and stays on ",
                    code("snow connection list"),
                    ".",
                ),
            ),
            examples=(Example(command="snow auth list"),),
        ),
    )
    def list_connections(**options) -> CommandResult:
        """Lists saved connections that use Snowflake OAuth for local applications."""
        return AuthManager().list_connections()
