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

import inspect
from typing import Callable, TypedDict

from pygls.server import LanguageServer
from snowflake.cli.app.snow_connector import connect_to_snowflake


class ConnectionParams(TypedDict):
    session_token: str
    master_token: str
    account: str
    connection_name: str
    params: dict


def lsp_plugin(name: str, version: str, capabilities: dict) -> Callable:
    def _decorator(func: Callable) -> Callable:
        setattr(
            func,
            "_lsp_context",
            {
                "lsp_plugin_name": name,
                "lsp_plugin_version": version,
                "lsp_plugin_capabilities": capabilities,
            },
        )
        return func

    return _decorator


def server_command(server: LanguageServer, command_name: str):
    """
    Wrap the pygls @server.command to provide the Snowflake connection
    and to unpack the parameters from the list of positional arguments
    """

    def _decorator(func):
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        if params and params[0].annotation == "SnowflakeConnection":

            @server.command(command_name)
            def wrapper(params: list[ConnectionParams]):
                connection_attributes = {
                    "account": params[0]["account"],
                    "session_token": params[0]["session_token"],
                    "master_token": params[0]["master_token"],
                }
                parameters = params[0]["params"]
                connection = connect_to_snowflake(
                    temporary_connection=True, **connection_attributes
                )
                return func(connection, **parameters)

        else:

            @server.command(command_name)
            def wrapper(params: list[ConnectionParams]):
                parameters = params[0]["params"]
                return func(**parameters)

        return wrapper

    return _decorator


def load_lsp_plugins():
    server = LanguageServer(name="lsp_controller", version="v0.0.1")

    # Use a dynamic import to avoid semi-circular imports if at top level
    from snowflake.cli.app.commands_registration.command_plugins_loader import (
        load_only_builtin_command_plugins,
    )

    plugins = load_only_builtin_command_plugins()
    for plugin_func in plugins:
        if getattr(plugin_func, "lsp_spec", None) is not None:
            plugin_func.lsp_spec(server)
    server.start_io()
