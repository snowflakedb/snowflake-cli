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

from abc import abstractmethod
from enum import IntEnum
from typing import Optional

from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.flags import SecretTypeParser
from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.api.plugins.command.interface import (
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    ParamDef,
    ParamKind,
)
from snowflake.cli.api.secret import SecretType


class ConnectionTestExitCode(IntEnum):
    r"""Process exit code returned by `snow postgres connection test`.

    Codes start at 5410 (54 prefix for Postgres, then 10 for this command) and
    stay clear of the CLI's own exit codes in `snowflake.cli.api.exceptions`.
    Checks run in order and stop at the first failure; the exit code identifies
    that failure and the remaining checks are reported as SKIP. The same code
    is reported in the `code` column of the result, because an OS exit status
    is truncated to 8 bits (5410 is seen as 34 by a shell).
    """

    SUCCESS = 0
    DNS_RESOLUTION_FAILED = 5410
    TCP_TIMEOUT = 5411
    TCP_REFUSED = 5412
    TLS_HANDSHAKE_FAILED = 5413
    AUTHENTICATION_FAILED = 5414


_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link(
        "/developer-guide/snowflake-cli/command-reference/overview",
        "Snowflake CLI command reference",
    ),
)

CONNECTION_TEST_DOCS = CommandDocs(
    related=_RELATED,
    usage_notes=(
        plain_text(
            "Tests network connectivity from this machine to a Snowflake Postgres "
            "instance. The result has one row per check with the columns "
            "check, status (PASS, FAIL, or SKIP), code, and detail. The code "
            "is 0 for PASS and SKIP, and the failure code below for FAIL."
        ),
        plain_text(
            "The instance is identified by an optional URL argument, which may be "
            "fully qualified (postgres://user:password@host:5432/database) or "
            "partial (for example just a host name). Anything the URL leaves out "
            "can be given with ",
            code("--host"),
            ", ",
            code("--port"),
            ", ",
            code("--database"),
            ", ",
            code("--username"),
            " and ",
            code("--password"),
            ". When both are given, the option wins over the URL. A host is "
            "required, and the port defaults to 5432.",
        ),
        plain_text("Checks run in order and stop at the first failure:"),
        bullet_list(
            bullet(plain_text("DNS resolution of the host.")),
            bullet(plain_text("TCP reachability of the host and port.")),
            bullet(plain_text("TLS handshake.")),
            bullet(
                plain_text(
                    "Authenticated login, only when both a user name and a "
                    "password are supplied, in the URL or as options.",
                )
            ),
        ),
        plain_text(
            "The exit code identifies the failed check: 5410 DNS resolution, "
            "5411 TCP timeout, 5412 TCP connection refused, 5413 TLS handshake, "
            "5414 authentication. Exit code 0 means every check passed. Operating "
            "systems keep only the low 8 bits of a process exit status, so a "
            "shell sees these values modulo 256 (for example 34 for 5410). "
            "Scripts should read the code column with --format json instead."
        ),
    ),
    examples=(
        Example(
            description=plain_text("Test connectivity using a host name:"),
            command="snow postgres connection test abc123.postgres.snowflake.app",
        ),
        Example(
            description=plain_text("Test connectivity and login using a full URL:"),
            command="snow postgres connection test postgres://snowflake_admin:...@abc123.postgres.snowflake.app:5432/postgres",
        ),
        Example(
            description=plain_text("Supply each part as a separate option:"),
            command="snow postgres connection test --host abc123.postgres.snowflake.app --port 5432 --database postgres --username snowflake_admin --password ...",
        ),
    ),
)

POSTGRES_SPEC = CommandGroupSpec(
    name="postgres",
    help="Manages Snowflake Postgres instances.",
    parent_path=(),
    subgroups=(
        CommandGroupSpec(
            name="connection",
            help="Tests and troubleshoots connections to Snowflake Postgres.",
            parent_path=("postgres",),
            commands=(
                CommandDef(
                    name="test",
                    help="Tests network connectivity to a Snowflake Postgres instance.",
                    handler_method="connection_test",
                    requires_connection=False,
                    params=(
                        ParamDef(
                            name="url",
                            type=Optional[SecretType],
                            kind=ParamKind.ARGUMENT,
                            help="Full or partial URL, for example postgres://user:password@host:5432/database or just a host name. Options override URL parts.",
                            default=None,
                            show_default=False,
                            click_type=SecretTypeParser(),
                        ),
                        ParamDef(
                            name="host",
                            type=Optional[str],
                            kind=ParamKind.OPTION,
                            cli_names=("--host",),
                            help="Host name of the Postgres instance. Required unless given in the URL.",
                            default=None,
                        ),
                        ParamDef(
                            name="port",
                            type=Optional[int],
                            kind=ParamKind.OPTION,
                            cli_names=("--port",),
                            help="Port of the Postgres instance. Defaults to 5432.",
                            default=None,
                            show_default=False,
                        ),
                        ParamDef(
                            name="database",
                            type=Optional[str],
                            kind=ParamKind.OPTION,
                            cli_names=("--database",),
                            help="Database for the authentication check. Defaults to postgres.",
                            default=None,
                            show_default=False,
                        ),
                        ParamDef(
                            name="username",
                            type=Optional[str],
                            kind=ParamKind.OPTION,
                            cli_names=("--username",),
                            help="User name for the authentication check. The check is skipped without a user name and password.",
                            default=None,
                        ),
                        ParamDef(
                            name="password",
                            type=Optional[SecretType],
                            kind=ParamKind.OPTION,
                            cli_names=("--password",),
                            help="Password for the authentication check. The check is skipped without a user name and password.",
                            default=None,
                            show_default=False,
                            click_type=SecretTypeParser(),
                        ),
                        ParamDef(
                            name="timeout",
                            type=int,
                            kind=ParamKind.OPTION,
                            cli_names=("--timeout",),
                            help="Timeout in seconds for each check.",
                            default=10,
                        ),
                    ),
                    output_type="CollectionResult",
                    docs=CONNECTION_TEST_DOCS,
                ),
            ),
        ),
    ),
)


class PostgresHandler(CommandHandler):
    @abstractmethod
    def connection_test(
        self,
        url: Optional[SecretType],
        host: Optional[str],
        port: Optional[int],
        database: Optional[str],
        username: Optional[str],
        password: Optional[SecretType],
        timeout: int,
    ) -> CommandResult:
        ...
