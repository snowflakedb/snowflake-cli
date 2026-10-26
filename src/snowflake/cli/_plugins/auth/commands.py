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

from typing import Optional

import typer
from snowflake.cli._plugins.auth.manager import AuthManager
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.overrideable_parameter import OverrideableOption
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult


def register_login(app: SnowTyperFactory) -> None:
    @app.command(
        "login",
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
                    "Login changes the default connection. The next command uses "
                    "the Snowflake OAuth connection this command writes."
                ),
                plain_text(
                    "Pass an account. An account URL or the ",
                    code("--host"),
                    " flag sets a custom account host. A bare account uses the "
                    "public account host.",
                ),
            ),
            examples=(
                Example(
                    description=plain_text(
                        "Sign in and change the default connection:"
                    ),
                    command="snow auth login myorg-myaccount",
                    output=(
                        "Wrote connection myorg-myaccount to "
                        "<user-home>/.snowflake/config.toml and changed the "
                        "default connection to myorg-myaccount."
                    ),
                ),
            ),
        ),
    )
    def login(
        account: Optional[str] = typer.Argument(
            None,
            help="Account to sign in to. A URL sets the account host.",
            show_default=False,
        ),
        host: Optional[str] = typer.Option(
            None,
            "--host",
            help=("Custom account host. A bare account uses the public account host."),
            show_default=False,
        ),
        role: Optional[str] = OverrideableOption(
            None,
            "--role",
            help="Connection role. Omitted leaves the stored role unchanged.",
            show_default=False,
            mutually_exclusive=["clear_role"],
        )(),
        warehouse: Optional[str] = OverrideableOption(
            None,
            "--warehouse",
            help=(
                "Connection warehouse. Omitted leaves the stored warehouse unchanged."
            ),
            show_default=False,
            mutually_exclusive=["clear_warehouse"],
        )(),
        clear_role: bool = OverrideableOption(
            False,
            "--clear-role",
            help="Remove the stored connection role.",
            is_flag=True,
            mutually_exclusive=["role"],
        )(),
        clear_warehouse: bool = OverrideableOption(
            False,
            "--clear-warehouse",
            help="Remove the stored connection warehouse.",
            is_flag=True,
            mutually_exclusive=["warehouse"],
        )(),
        database: Optional[str] = OverrideableOption(
            None,
            "--database",
            help="Connection database. Omitted leaves the stored database unchanged.",
            show_default=False,
            mutually_exclusive=["clear_database"],
        )(),
        schema: Optional[str] = OverrideableOption(
            None,
            "--schema",
            help="Connection schema. Omitted leaves the stored schema unchanged.",
            show_default=False,
            mutually_exclusive=["clear_schema"],
        )(),
        clear_database: bool = OverrideableOption(
            False,
            "--clear-database",
            help="Remove the stored connection database.",
            is_flag=True,
            mutually_exclusive=["database"],
        )(),
        clear_schema: bool = OverrideableOption(
            False,
            "--clear-schema",
            help="Remove the stored connection schema.",
            is_flag=True,
            mutually_exclusive=["schema"],
        )(),
        **options,
    ) -> CommandResult:
        """Signs in with Snowflake OAuth for local applications and changes the default connection."""
        if account is None:
            account = typer.prompt("Enter account")
        return AuthManager().login(
            account,
            host,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            clear_role=clear_role,
            clear_warehouse=clear_warehouse,
            clear_database=clear_database,
            clear_schema=clear_schema,
        )


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
