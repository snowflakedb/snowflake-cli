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

from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.plugins.lsp.utils import (
    load_lsp_plugins,
)

app = SnowTyper(name="lsp", help="Manages a Snowflake LSP server.", hidden=True)

log = logging.getLogger(__name__)


@app.command("start")
def lsp_start(
    **options,
) -> CommandResult:
    """
    Starts the LSP language server in the foreground.
    """
    load_lsp_plugins()
    return MessageResult(f"LSP server process ended.")
