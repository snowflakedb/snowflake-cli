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

import logging

from snowflake.cli.api.commands.snow_typer import SnowTyperFactory

log = logging.getLogger(__name__)
app = SnowTyperFactory(
    name="remote",
    help="Manages remote development environments.",
)


@app.command("list", requires_connection=True)
def list_services(**options) -> None:
    """
    List remote development environments.

    This is a placeholder command for the remote plugin.
    Full functionality will be implemented in subsequent PRs.
    """
    log.info("Remote plugin registered successfully")
    log.info("Remote development environments plugin is registered.")
    log.info("Full functionality will be available in upcoming releases.")
