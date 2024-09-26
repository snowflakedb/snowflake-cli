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
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.plugins.lsp.models.cmd_poc_input import CmdPocInput
from snowflake.cli.plugins.lsp.models.cmd_poc_output import CmdPocOutput
from snowflake.cli.plugins.lsp.server import lsp_plugin
from snowflake.cli._plugins.nativeapp.manager import NativeAppManager


@lsp_plugin(
    name="nativeapp",
    capabilities={
        "openApplication": True,
    },
)
def nade_lsp_plugin(server: LanguageServer):
    # FIXME: can't parametrize iter_lsp_plugins() if this is top-level ?
    from snowflake.cli.plugins.lsp.interface import workspace_command

    @workspace_command(server, "openApplication")
    def open_app() -> MessageResult:

        ctx = get_cli_context()

        dm = DefinitionManager(ctx.project_root)
        project_definition = getattr(dm.project_definition, "native_app", None)
        project_root = dm.project_root
        manager = NativeAppManager(
            project_definition=project_definition,
            project_root=project_root,
        )
        if manager.get_existing_app_info():
            url = manager.get_snowsight_url()
            return MessageResult(f"{url}")
        else:
            return MessageResult(
                'Snowflake Native App not yet deployed! Please run "runApplication" first.'
            )

    @workspace_command(server, "poc_cmd")
    def poc_cmd(input: CmdPocInput) -> CmdPocOutput:
        return CmdPocOutput(
            output_1=input.input_1,
            output_2=input.input_2,
        )
