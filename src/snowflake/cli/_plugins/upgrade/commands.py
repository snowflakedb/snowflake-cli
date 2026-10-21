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

import time

import typer
from snowflake.cli._app.version_check import suppress_new_version_banner
from snowflake.cli._plugins.upgrade.manager import (
    append_upgrade_restriction_note,
    plan_upgrade,
)
from snowflake.cli._plugins.upgrade.telemetry import (
    TRIGGER_MANUAL,
    record_upgrade_event,
)
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult, ObjectResult

app = SnowTyperFactory()


@app.command(
    name="upgrade",
    requires_connection=False,
    docs=CommandDocs(
        related=(
            link("/developer-guide/snowflake-cli/index"),
            link(
                "/developer-guide/snowflake-cli/command-reference/overview",
                "Snowflake CLI command reference",
            ),
            link(
                "/developer-guide/snowflake-cli/installation/installation",
                "Installing Snowflake CLI",
            ),
        ),
        usage_notes=(
            plain_text(
                code("snow upgrade"),
                " updates a direct install of Snowflake CLI to the latest "
                "same-major version. It writes a new version directory, verifies "
                "the signed manifest and tarball checksum, and retargets the shim. "
                "The next ",
                code("snow"),
                " in the same session is the new version.",
            ),
            plain_text(
                "The command works only when ",
                code("snow --info"),
                " reports ",
                code("installation_source"),
                " as ",
                code("direct-install"),
                ". Other channels refuse and print the direct-install command. "
                "Those installs are not modified:",
            ),
            bullet_list(
                bullet(
                    code("pypi"),
                    " (pip, pipx, uv, and the Homebrew formula) refuses.",
                ),
                bullet(
                    code("binary"),
                    " (macOS pkg, Homebrew cask, Windows MSI, and Linux deb or rpm) "
                    "refuses.",
                ),
            ),
            plain_text(
                "A newer major version is reported and not installed. If the "
                "install is already on the latest same-major version, the command "
                "reports that and does nothing."
            ),
            plain_text(
                code("--dry-run"),
                " shows what would change without downloading or retargeting the "
                "shim. ",
                code("--revert"),
                " points the shim at the previous direct-install version. Revert "
                "is available only after a successful upgrade, does not download "
                "a new version, and does not change configuration in ",
                code("~/.snowflake"),
                ".",
            ),
            plain_text(
                "Upgrades are disabled when ",
                code("cli.air_gapped"),
                " is set in the configuration file or ",
                code("SNOWFLAKE_CLI_AIR_GAPPED"),
                " is set.",
            ),
        ),
        examples=(
            Example(
                description=plain_text("Upgrade a direct install:"),
                command="snow upgrade",
                output="Upgraded 3.12.0 → 3.13.1.",
            ),
            Example(
                description=plain_text(
                    "Show the upgrade without downloading or retargeting the shim:"
                ),
                command="snow upgrade --dry-run",
            ),
            Example(
                description=plain_text(
                    "Point the shim at the previous direct-install version:"
                ),
                command="snow upgrade --revert",
            ),
        ),
    ),
)
def upgrade(
    revert: bool = typer.Option(
        False,
        "--revert",
        help="Point the shim at the previous direct install version.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would change without downloading or retargeting the shim.",
    ),
    **options,
) -> CommandResult:
    """Upgrade a direct install of Snowflake CLI."""
    suppress_new_version_banner()
    started = time.monotonic()
    decision = plan_upgrade(dry_run=dry_run, revert=revert)
    duration_ms = int((time.monotonic() - started) * 1000)
    payload = decision.payload
    record_upgrade_event(
        trigger=TRIGGER_MANUAL,
        status=str(payload.get("status", "")),
        duration_ms=duration_ms,
        from_version=payload.get("from"),
        to_version=payload.get("to"),
    )
    message = append_upgrade_restriction_note(decision.message)
    if get_cli_context().output_format.is_json:
        return ObjectResult(decision.payload)
    return MessageResult(message)
