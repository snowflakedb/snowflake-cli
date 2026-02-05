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

from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.output.types import CommandResult, MessageResult

app = SnowTyperFactory(
    name="apps",
    help="Manages Snowflake Apps.",
    is_hidden=FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled,
)


def _check_feature_enabled():
    if FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled():
        raise CliError("This feature is not available yet.")


@app.command(requires_connection=False)
def create(**options) -> CommandResult:
    """
    Creates a Snowflake App.
    """
    _check_feature_enabled()
    return MessageResult("snow apps create")


@app.command(requires_connection=False)
def build(**options) -> CommandResult:
    """
    Builds a Snowflake App.
    """
    _check_feature_enabled()
    return MessageResult("snow apps build")


@app.command(requires_connection=False)
def deploy(**options) -> CommandResult:
    """
    Deploys a Snowflake App.
    """
    _check_feature_enabled()
    return MessageResult("snow apps deploy")
