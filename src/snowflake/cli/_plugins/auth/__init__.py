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

from snowflake.cli._plugins.auth.commands import register_list, register_login
from snowflake.cli._plugins.auth.oidc.commands import (
    app as oidc_app,
)
from snowflake.cli.api.commands.snow_typer import SnowTyper, SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag


class _AuthTyperFactory(SnowTyperFactory):
    def create_instance(self) -> SnowTyper:
        # Read the flag when the command is registered, not when this module loads.
        # Subgroups added with add_typer, including oidc, are not in this list.
        hidden = FeatureFlag.ENABLE_AUTH_COMMANDS.is_disabled()
        for command in self.commands_to_register:
            command.kwargs["hidden"] = hidden
        return super().create_instance()


app = _AuthTyperFactory(
    name="auth",
    help="Manages authentication methods.",
)

register_login(app)
register_list(app)
app.add_typer(oidc_app)
