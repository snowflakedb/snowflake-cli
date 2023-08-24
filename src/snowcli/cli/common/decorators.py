from __future__ import annotations

import inspect
from functools import wraps
from inspect import Signature
from typing import Callable, Optional, get_type_hints, List

from snowcli.cli import loggers
from snowcli.cli.common.flags import (
    ConnectionOption,
    AccountOption,
    UserOption,
    DatabaseOption,
    SchemaOption,
    RoleOption,
    WarehouseOption,
    PasswordOption,
    OutputFormatOption,
    VerboseOption,
    DebugOption,
)
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.output.formats import OutputFormat


def global_options(func: Callable):
    """
    Decorator providing default flags for overriding global parameters. Values are
    updated in global SnowCLI state.

    To use this decorator your command needs to accept **options as last argument.
    """
    return _options_decorator_factory(func, GLOBAL_OPTIONS)


def global_options_with_connection(func: Callable):
    """
    Decorator providing default flags including connection flags for overriding
    global parameters. Values are updated in global SnowCLI state.

    To use this decorator your command needs to accept **options as last argument.
    """
    return _options_decorator_factory(
        func, [*GLOBAL_CONNECTION_OPTIONS, *GLOBAL_OPTIONS]
    )


def _execute_before_command():
    global_context = snow_cli_global_context_manager.get_global_context_copy()
    loggers.create_loggers(global_context.verbose, global_context.enable_tracebacks)


def _options_decorator_factory(
    func: Callable, additional_options: List[inspect.Parameter]
):
    @wraps(func)
    def wrapper(**options):
        _execute_before_command()
        return func(**options)

    wrapper.__signature__ = _extend_signature_with_global_options(func, additional_options)  # type: ignore
    return wrapper


GLOBAL_CONNECTION_OPTIONS = [
    inspect.Parameter(
        "connection",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=ConnectionOption,
    ),
    inspect.Parameter(
        "account",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=AccountOption,
    ),
    inspect.Parameter(
        "user",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=UserOption,
    ),
    inspect.Parameter(
        "password",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=PasswordOption,
    ),
    inspect.Parameter(
        "database",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=DatabaseOption,
    ),
    inspect.Parameter(
        "schema",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=SchemaOption,
    ),
    inspect.Parameter(
        "role",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=RoleOption,
    ),
    inspect.Parameter(
        "warehouse",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=WarehouseOption,
    ),
]

GLOBAL_OPTIONS = [
    inspect.Parameter(
        "format",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=OutputFormat,
        default=OutputFormatOption,
    ),
    inspect.Parameter(
        "verbose",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=VerboseOption,
    ),
    inspect.Parameter(
        "debug",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=DebugOption,
    ),
]


def _extend_signature_with_global_options(
    func: Callable, additional_options: List[inspect.Parameter]
) -> Signature:
    """Extends function signature with global options"""
    sig = inspect.signature(func)

    # Remove **options from signature
    existing_parameters = tuple(sig.parameters.values())[:-1]

    type_hints = get_type_hints(func)
    existing_parameters_with_evaluated_types = [
        inspect.Parameter(
            name=p.name,
            kind=p.kind,
            annotation=type_hints.get(p.name),
            default=p.default,
        )
        for p in existing_parameters
    ]
    parameters = [
        *existing_parameters_with_evaluated_types,
        *additional_options,
    ]
    sig = sig.replace(parameters=parameters)
    return sig
