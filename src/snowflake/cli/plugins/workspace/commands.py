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

from snowflake.cli.api.commands.flags import (
    project_type_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import MessageResult

ws = SnowTyper(
    name="ws",
    hidden=True,
    help="Deploy and interact with snowflake.yml-based entities.",
)


@ws.command(requires_connection=True)
def validate(
    project_definition: Optional[str] = project_type_option(None),
    **options,
):
    """Validates the project definition file."""
    return MessageResult("Project definition is valid.")
