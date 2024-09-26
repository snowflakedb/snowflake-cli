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
from typing import Any, Callable, Dict, List, Optional, Union, get_type_hints, is_typeddict
from pydantic import TypeAdapter, ValidationError
from snowflake.cli.api.cli_global_context import CliContextArguments, fork_cli_context

from pygls.server import LanguageServer

from snowflake.cli.plugins.snowflake.cli.plugins.lsp.models.context import Context


def workspace_command(
    server: LanguageServer,
    name: str,
    requires_connection: bool = False,
    requires_project: bool = False,
):
    """
    Wrap a function with pygls' @server.command.
    Ensures that the command invocation provides a valid connection / project context
    (if required) as well as ensuring arguments are in the required format.
    """
    def _decorator(func):
        """
        validate once during load time that func
        - contains 0 or 1 argument
            - must be typed and contain a "from_dict" method
        """
        sig = inspect.signature(func)
        func_params = list(sig.parameters.values())
        if len(func_params) > 1:
            raise TypeError("func is not expected to have more than 1 argument")

        input_cmd_model_type = None
        if len(func_params) == 1:
            input_cmd_model_type = func_params[0].annotation
            if not hasattr(input_cmd_model_type, "from_dict"):
                raise TypeError("func argument is expected to have a from_dict method")

        @server.command(name)
        def wrapper(arguments: List[Dict]):
            if len(arguments) > 1:
                raise ValueError("Expected exactly one CommandArguments object")

            try:
                lsp_raw_payload = arguments[0]
                context = Context.from_dict(lsp_raw_payload["context"])

                if requires_connection and not context.connection:
                    raise ValueError("connection missing, but requires_connection=True")
                
                if requires_project and not context.project_path:
                    raise ValueError("project_path missing, but requires_connection=True")

                # TODO: validation of args.args / args.kwargs based on shape of actual command...

                with fork_cli_context(
                    connection_overrides=context.connection,
                    project_env_overrides=context.env,
                    project_path=context.project_path,
                ):
                    if input_cmd_model_type:
                        return func(input_cmd_model_type.from_dict(lsp_raw_payload["cmd"]))
                    return func()
                
            except ValidationError as exc: 
                raise ValueError(f"ERROR: Invalid schema: {exc}")

        return wrapper

    return _decorator
