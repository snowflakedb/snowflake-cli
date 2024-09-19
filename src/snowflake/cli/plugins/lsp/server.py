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

from typing import Callable

from pygls.server import LanguageServer

from snowflake.cli.__about__ import VERSION

def lsp_plugin(name: str, capabilities: dict) -> Callable:
    """
    Defines the root of an LSP plugin.
    The intention is for each Snowflake CLI plugin to have at most 1 LSP plugin (namespace).
    """
    def _decorator(func: Callable) -> Callable:
        setattr(
            func,
            "_lsp_context",
            {
                "lsp_plugin_name": name,
                "lsp_plugin_capabilities": capabilities,
            },
        )
        return func

    return _decorator


def start_lsp_server():
    """
    Find and initialize all LSP plugins, then start the LSP server.    
    """

    # N.B. LSP server version is aligned with Snowflake CLI version
    server = LanguageServer(name="lsp_controller", version=VERSION)

    for plugin_func in iter_lsp_plugins():
        plugin_func.lsp_spec(server)

    server.start_io()
    return server


def iter_lsp_plugins():
    """
    Dynamically discover all plugin functions that implement the LSP spec
    (i.e. all functions decorated with @lsp_plugin).
    """

    # Use a dynamic import to avoid semi-circular imports if at top level
    from snowflake.cli.app.commands_registration.command_plugins_loader import (
        load_only_builtin_command_plugins,
    )

    plugins = load_only_builtin_command_plugins()
    for plugin_func in plugins:
        if getattr(plugin_func, "lsp_spec", None) is not None:
            yield plugin_func


