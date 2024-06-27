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
from typing import Optional, TypedDict

from pygls.server import LanguageServer
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.plugins.nativeapp.lsp_commands import lsp_plugin
from snowflake.cli.plugins.rpc.manager import (
    ConnectionParams,
    RpcManager,
)

app = SnowTyper(
    name="rpc",
    help="Manages a RPC server for LSP.",
)

log = logging.getLogger(__name__)


class ConnectionDict(TypedDict):
    sessionToken: Optional[str]
    masterToken: Optional[str]
    account: Optional[str]


# requires connection for now, but in the future we should be able to start it without one and add one later
@app.command("start", requires_connection=True)
def rpc_start(
    **options,
) -> CommandResult:
    """
    Initializes the RPC LSP language server.
    """
    RpcManager()

    return MessageResult(f"RPC LSP began.")


@lsp_plugin(
    name="rpc",
    version="0.0.1",
    capabilities={
        "updateConnection": True,
    },
)
def rpc_lsp_plugin(server: LanguageServer):
    @server.command("updateConnection")
    def update_connection(params: list[ConnectionDict]):
        connection_params = ConnectionParams(
            session_token=params[0]["sessionToken"],
            master_token=params[0]["masterToken"],
            account=params[0]["account"],
        )
        # do nothing for now
        # ctx.update_connection(connection_params)
        return "Connection ctx updated."
