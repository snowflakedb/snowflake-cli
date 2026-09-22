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

import typer
from snowflake.cli._app.version_check import suppress_new_version_banner
from snowflake.cli._plugins.upgrade.manager import plan_upgrade
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyper, SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import CommandResult, MessageResult, ObjectResult


class _UpgradeTyperFactory(SnowTyperFactory):
    def create_instance(self) -> SnowTyper:
        hidden = FeatureFlag.ENABLE_SNOW_UPGRADE.is_disabled()
        for command in self.commands_to_register:
            command.kwargs["hidden"] = hidden
        return super().create_instance()


app = _UpgradeTyperFactory(
    is_hidden=FeatureFlag.ENABLE_SNOW_UPGRADE.is_disabled,
)


@app.command(name="upgrade", requires_connection=False)
def upgrade(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would change without downloading or retargeting the shim.",
    ),
    **options,
) -> CommandResult:
    """Upgrade the snowflake-managed distribution of Snowflake CLI."""
    suppress_new_version_banner()
    decision = plan_upgrade(dry_run=dry_run)
    if get_cli_context().output_format.is_json:
        return ObjectResult(decision.payload)
    return MessageResult(decision.message)
