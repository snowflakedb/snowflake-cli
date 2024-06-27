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

from dataclasses import dataclass
from typing import Dict, TypedDict

from pygls.server import LanguageServer
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.app.snow_connector import connect_to_snowflake
from snowflake.cli.plugins.nativeapp.manager import NativeAppManager


@dataclass
class LspContext:
    lsp_plugin_name: str
    lsp_plugin_version: str
    lsp_plugin_capabilities: Dict[bool, str]


def lsp_plugin(name: str, version: str, capabilities: Dict[str, bool]):
    def _decorator(func):
        lsp_context: LspContext = {
            "lsp_plugin_name": name,
            "lsp_plugin_version": version,
            "lsp_plugin_capabilities": capabilities,
        }
        setattr(func, "_lsp_context", lsp_context)
        return func

    return _decorator


class OpenApplication(TypedDict):
    project_path: str


class ConnectionParams(TypedDict):
    session_token: str
    master_token: str
    account: str
    connection_name: str
    params: dict


def wrapped_server_command(server: LanguageServer, command_name: str):
    def _decorator(func):
        @server.command(command_name)
        def wrapper(params: list[ConnectionParams]):
            connection_attributes = {
                "account": params[0]["account"],
                "session_token": params[0]["session_token"],
                "master_token": params[0]["master_token"],
            }
            parameters = params[0]["params"]
            # introspect func and see if first param takes a connection and if it does, go down conn route, if it doesn't then don''t
            connection = connect_to_snowflake(
                temporary_connection=True, **connection_attributes
            )
            return func(connection, **parameters)  # kwargs or args??

        return wrapper

    return _decorator


@lsp_plugin(
    name="nativeapp",
    version="0.0.1",
    capabilities={
        "openApplication": True,
    },
)
def nade_lsp_plugin(server: LanguageServer):
    @wrapped_server_command(server, "openApplication")
    def open_app(connection, params: OpenApplication):  # foo, bar):
        server.show_message_log(repr(params))
        project_path = params["project_path"]
        dm = DefinitionManager(project_path)
        project_definition = getattr(dm.project_definition, "native_app", None)
        project_root = dm.project_root
        manager = NativeAppManager(
            project_definition=project_definition,
            project_root=project_root,
            connection=connection,
        )
        if manager.get_existing_app_info():
            url = manager.get_snowsight_url()
            return MessageResult(f"{url}")
        else:
            return MessageResult(
                'Snowflake Native App not yet deployed! Please run "snow app run" first.'
            )
