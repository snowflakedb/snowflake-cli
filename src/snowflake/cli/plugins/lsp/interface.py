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
from typing_extensions import TypedDict, NotRequired
from snowflake.cli.api.cli_global_context import CliContextArguments, fork_cli_context

from pygls.server import LanguageServer


ORIGINAL_FUNCTION_KEY = "__lsp_original_function__"


TypeDef = Dict[str, Union[str, 'TypeDef']]

class CommandArguments(CliContextArguments):
    """
    The arguments that can be passed to a workspace command.

    """
    args: NotRequired[List[Any]]
    kwargs: NotRequired[Dict[str, Any]]



class CommandSignature(TypedDict):
    name: str
    args: List[str]
    arg_types: TypeDef
    arg_defaults: Dict[str, Any]
    return_type: str


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
        @server.command(name)
        def wrapper(params: List[CommandArguments]):
            if len(params) > 1:
                raise ValueError("Expected exactly one CommandArguments object")

            try:
                args = TypeAdapter(CommandArguments).validate_python(params[0] if len(params) == 1 else {})

                if requires_connection and "connection" not in args:
                    raise ValueError("connection missing, but requires_connection=True")
                
                if requires_project and "project_path" not in args:
                    raise ValueError("project_path missing, but requires_connection=True")

                with fork_cli_context(**args):
                    return func(*args.get("args", []), **args.get("kwargs", {}))
                
            except ValidationError as exc: 
                raise ValueError(f"ERROR: Invalid schema: {exc}")
            

        setattr(wrapper, ORIGINAL_FUNCTION_KEY, func)
        return wrapper

    return _decorator


def to_typedef(annotation: any) -> str:
    # if not isinstance(annotation, type):
    #     raise ValueError(f"expected {annotation} to be a type!")
    
    # return type(annotation).__name__

    if is_typeddict(annotation):
        print('hi')
    elif is_typeddict(type(annotation)):
        print('hiback')

    return annotation

    # print(annotation)

    # if hasattr(annotation, '__annotations__'):
    #     tuples = annotation.__annotations__
    #     print(tuples)


def generate_signature(func: Callable) -> CommandSignature:    
    signature = inspect.signature(func)
    args = list(signature.parameters.keys())
    arg_types = { k: to_typedef(v.annotation) for k, v in signature.parameters.items() if v.annotation is not inspect.Parameter.empty }
    arg_defaults = { k: v.default for k, v in signature.parameters.items() if v.default is not inspect.Parameter.empty }
    return_type = signature.return_annotation if signature.return_annotation is not inspect.Signature.empty else None

    print(signature)
    print(get_type_hints(func))

    return CommandSignature(
        name=func.__name__,
        args=args,
        arg_types=arg_types,
        arg_defaults=arg_defaults,
        return_type=return_type
    )

class Ghi(TypedDict):
    x: List[str]
    y: Dict[str, int]

class Abc(TypedDict):
    d: int
    e: str
    f: Optional[Ghi]

def myfunc(a: int, b: int | None, c: Ghi, d: Abc, e: Union[Abc, Ghi]) -> Abc:
    pass
