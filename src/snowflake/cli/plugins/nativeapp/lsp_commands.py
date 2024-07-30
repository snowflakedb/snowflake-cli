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

from pygls.server import LanguageServer
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.plugins.lsp.utils import lsp_plugin, server_command
from snowflake.cli.plugins.nativeapp.manager import NativeAppManager
from snowflake.connector import SnowflakeConnection


@lsp_plugin(
    name="nativeapp",
    capabilities={
        "openApplication": True,
    },
)
def nade_lsp_plugin(server: LanguageServer):
    @server_command(server, "openApplication")
    def open_app(connection: SnowflakeConnection, project_path: str):
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
                'Snowflake Native App not yet deployed! Please run "runApplication" first.'
            )
