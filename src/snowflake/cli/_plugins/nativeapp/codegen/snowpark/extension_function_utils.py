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

import ast
from typing import (
    List,
    Optional,
    Sequence,
    Union,
)

from click.exceptions import ClickException
from snowflake.cli._plugins.nativeapp.codegen.snowpark.models import (
    ExtensionFunctionTypeEnum,
    NativeAppExtensionFunction,
)
from snowflake.cli.api.project.schemas.snowpark.argument import Argument
from snowflake.cli.api.project.util import (
    is_valid_identifier,
    is_valid_string_literal,
    to_identifier,
    to_string_literal,
)

ASTDefNode = Union[ast.FunctionDef, ast.ClassDef]


class MalformedExtensionFunctionError(ClickException):
    """Required extension function attribute is missing."""

    def __init__(self, message: str):
        super().__init__(message=message)


def get_sql_object_type(extension_fn: NativeAppExtensionFunction) -> Optional[str]:
    if extension_fn.function_type == ExtensionFunctionTypeEnum.PROCEDURE:
        return "PROCEDURE"
    elif extension_fn.function_type in (
        ExtensionFunctionTypeEnum.FUNCTION,
        ExtensionFunctionTypeEnum.TABLE_FUNCTION,
    ):
        return "FUNCTION"
    elif extension_fn.function_type == extension_fn.function_type.AGGREGATE_FUNCTION:
        return "AGGREGATE FUNCTION"
    else:
        return None


def get_sql_argument_signature(arg: Argument) -> str:
    formatted = f"{arg.name} {arg.arg_type}"
    if arg.default is not None:
        formatted = f"{formatted} DEFAULT {arg.default}"
    return formatted


def get_function_type_signature_for_grant(
    extension_fn: NativeAppExtensionFunction,
) -> str:
    """
    Returns the type signature for the specified function, e.g. "int, varchar", suitable for inclusion in a GRANT statement.
    """
    return ", ".join([arg.arg_type for arg in extension_fn.signature])


def get_qualified_object_name(extension_fn: NativeAppExtensionFunction) -> str:
    qualified_name = to_identifier(extension_fn.name)
    if extension_fn.schema_name:
        if is_valid_identifier(extension_fn.schema_name):
            qualified_name = f"{extension_fn.schema_name}.{qualified_name}"
        else:
            full_schema = ".".join(
                [
                    to_identifier(schema_part)
                    for schema_part in extension_fn.schema_name.split(".")
                ]
            )
            qualified_name = f"{full_schema}.{qualified_name}"

    return qualified_name


def ensure_string_literal(value: str) -> str:
    """
    Returns the string literal representation of the given value, or the value itself if
    it was already a valid string literal.
    """
    if is_valid_string_literal(value):
        return value
    return to_string_literal(value)


def ensure_all_string_literals(values: Sequence[str]) -> List[str]:
    """
    Ensures that all provided values are valid string literals.

    Returns:
        A list with all values transformed to be valid string literals (as necessary).
    """
    return [ensure_string_literal(value) for value in values]


class _SnowparkHandlerAccumulator(ast.NodeVisitor):
    """
    A NodeVisitor that collects AST nodes corresponding to a provided list of Snowpark external functions.
    The returned nodes are filtered using the handlers provided for each of the Snowpark functions.
    Returned definitions can be either function definition or class definition AST nodes.
    """

    def __init__(self, functions: Sequence[NativeAppExtensionFunction]):
        self._wanted_handlers_by_name = {
            fn.handler.split(".")[-1]: fn for fn in functions
        }
        self.definitions: List[ASTDefNode] = []

    def visit_FunctionDef(self, node: ast.FunctionDef):  # noqa: N802
        if self._want(node):
            self.definitions.append(node)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef):  # noqa: N802
        if self._want(node):
            self.definitions.append(node)
        self.generic_visit(node)

    def _want(self, node: ASTDefNode) -> bool:
        if not node.decorator_list:
            # No decorators for this definition, ignore it
            return False

        return node.name in self._wanted_handlers_by_name


def _get_decorator_id(node: ast.AST) -> Optional[str]:
    """
    Returns the fully qualified identifier for a decorator, e.g. "foo" or "foo.bar".
    """
    if isinstance(node, ast.Name):
        return node.id
    elif isinstance(node, ast.Attribute):
        return f"{_get_decorator_id(node.value)}.{node.attr}"
    elif isinstance(node, ast.Call):
        return _get_decorator_id(node.func)
    else:
        return None


def _collect_ast_handler_definitions(
    tree: ast.AST, extension_functions: Sequence[NativeAppExtensionFunction]
) -> Sequence[ASTDefNode]:
    accumulator = _SnowparkHandlerAccumulator(extension_functions)
    accumulator.visit(tree)
    return accumulator.definitions


def deannotate_module_source(
    module_source: str,
    extension_functions: Sequence[NativeAppExtensionFunction],
    annotations_to_preserve: Sequence[str] = (),
) -> str:
    """
    Removes annotations from a set of specified extension functions.

    Arguments:
        module_source (str): The source code of the module to deannotate.
        extension_functions (Sequence[NativeAppExtensionFunction]): The list of extension functions
         to deannotate. Other functions encountered will be ignored.
        annotations_to_preserve (Sequence[str], optional): The list of annotations to preserve. The
         names should appear as they are found in the source code, e.g. "foo" for @foo or
         "annotations.bar" for @annotations.bar.

    Returns:
        A de-annotated version of the module source if any match was found. In order to preserve
        line numbers, annotations are simply commented out instead of completely removed.
    """

    tree = ast.parse(module_source)

    definitions = _collect_ast_handler_definitions(tree, extension_functions)
    if not definitions:
        return module_source

    module_lines = module_source.splitlines()
    for definition in definitions:
        # Comment out all decorators. As per the python grammar, decorators must be terminated by a
        # new line, so the line ranges can't overlap.
        for decorator in definition.decorator_list:
            decorator_id = _get_decorator_id(decorator)
            if decorator_id is None:
                continue
            if annotations_to_preserve and decorator_id in annotations_to_preserve:
                continue

            # AST indices are 1-based
            start_lineno = decorator.lineno - 1
            if decorator.end_lineno is not None:
                end_lineno = decorator.end_lineno - 1
            else:
                end_lineno = start_lineno

            for lineno in range(start_lineno, end_lineno + 1):
                module_lines[lineno] = "#: " + module_lines[lineno]

    # we're writing files in text mode, so we should use '\n' regardless of the platform
    return "\n".join(module_lines)
