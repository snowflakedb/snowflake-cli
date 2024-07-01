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
from dataclasses import dataclass
from typing import Optional

from pygls.server import LanguageServer

logging.basicConfig(level=logging.DEBUG)


@dataclass
class ConnectionParams:
    session_token: Optional[str]
    master_token: Optional[str]
    account: Optional[str]


# make this a function thanks
class RpcManager:
    """
    Base class LSP Controller
    """

    def __init__(self):
        super().__init__()
        server = LanguageServer(name="lsp_controller", version="v0.0.1")
        # dynamic import to avoid semi circular imports if at top level
        from snowflake.cli.app.commands_registration.command_plugins_loader import (
            load_only_builtin_command_plugins,
        )

        plugins = load_only_builtin_command_plugins()
        logging.info(plugins)
        for plugin_func in plugins:
            if getattr(plugin_func, "lsp_spec", None) is not None:
                logging.info(plugin_func)
                logging.info(plugin_func.lsp_spec)

                plugin_func.lsp_spec(server)
        server.start_io()
