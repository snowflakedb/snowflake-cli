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
from snowflake.cli.plugins.nativeapp.manager import NativeAppManager
from snowflake.cli.plugins.rpc.manager import LSPPluginContext


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


@lsp_plugin(
    name="nativeapp",
    version="0.0.1",
    capabilities={
        "openApplication": True,
    },
)
def nade_lsp_plugin(server: LanguageServer, ctx: LSPPluginContext):
    @server.command("openApplication")
    def open_app(params: list[OpenApplication]):
        server.show_message_log(repr(params))
        project_path = params[0]["project_path"]
        dm = DefinitionManager(project_path)
        project_definition = getattr(dm.project_definition, "native_app", None)
        project_root = dm.project_root
        manager = NativeAppManager(
            project_definition=project_definition,
            project_root=project_root,
            connection=ctx.get_connection(),
        )
        if manager.get_existing_app_info():
            url = manager.get_snowsight_url()
            return MessageResult(f"{url}")
        else:
            return MessageResult(
                'Snowflake Native App not yet deployed! Please run "snow app run" first.'
            )
