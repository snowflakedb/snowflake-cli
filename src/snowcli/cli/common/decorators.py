from __future__ import annotations

import inspect
from functools import wraps
from inspect import Signature
from typing import Callable, Optional, get_type_hints, List, Dict

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
    AuthenticatorOption,
    PrivateKeyPathOption,
    OutputFormatOption,
    VerboseOption,
    DebugOption,
    experimental_option,
)
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.output.formats import OutputFormat


def global_options(func: Callable):
    """
    Decorator providing default flags for overriding global parameters. Values are
    updated in global SnowCLI state.

    To use this decorator your command needs to accept **options as last argument.
    """
    return _global_options_decorator_factory(func, GLOBAL_OPTIONS)


def global_options_with_connection(func: Callable):
    """
    Decorator providing default flags including connection flags for overriding
    global parameters. Values are updated in global SnowCLI state.

    To use this decorator your command needs to accept **options as last argument.
    """
    return _global_options_decorator_factory(
        func, [*GLOBAL_CONNECTION_OPTIONS, *GLOBAL_OPTIONS]
    )


def with_experimental_behaviour(
    experimental_behaviour_description: Optional[str] = None,
):
    """
    Decorator providing --experimental flag, which allows to use experimental behaviour in commands implementation.

    To use this decorator your command needs to accept **options as last argument.
    """
    additional_options = [
        inspect.Parameter(
            "experimental",
            inspect.Parameter.KEYWORD_ONLY,
            annotation=Optional[bool],
            default=experimental_option(experimental_behaviour_description),
        ),
    ]

    def decorator(func: Callable):
        return _options_decorator_factory(
            func=func,
            additional_options=additional_options,
            execute_before_command_using_new_options=lambda: None,
        )

    return decorator


def _execute_before_command_using_global_options():
    global_context = snow_cli_global_context_manager.get_global_context_copy()
    loggers.create_loggers(global_context.verbose, global_context.enable_tracebacks)


def _global_options_decorator_factory(
    func: Callable, additional_options: List[inspect.Parameter]
):
    return _options_decorator_factory(
        func=func,
        additional_options=additional_options,
        execute_before_command_using_new_options=_execute_before_command_using_global_options,
    )


def _options_decorator_factory(
    func: Callable,
    additional_options: List[inspect.Parameter],
    execute_before_command_using_new_options: Callable,
):
    @wraps(func)
    def wrapper(**options):
        execute_before_command_using_new_options()
        return func(**options)

    wrapper.__signature__ = _extend_signature_with_additional_options(func, additional_options)  # type: ignore
    return wrapper


def _extend_signature_with_additional_options(
    func: Callable, additional_options: List[inspect.Parameter]
) -> Signature:
    """Extends function signature with additional options"""
    sig = inspect.signature(func)

    # Remove **options from signature
    existing_parameters = tuple(
        [p for p in sig.parameters.values() if not _is_options_parameter(p)]
    )

    type_hints = get_type_hints(func)
    existing_parameters_with_evaluated_types = [
        _evaluate_param_type(p, type_hints) for p in existing_parameters
    ]
    parameters = [
        *existing_parameters_with_evaluated_types,
        *additional_options,
    ]
    return sig.replace(parameters=parameters)


def _is_options_parameter(param: inspect.Parameter) -> bool:
    return param.kind == inspect.Parameter.VAR_KEYWORD and param.name == "options"


def _evaluate_param_type(
    param: inspect.Parameter, type_hints: Dict[str, type]
) -> inspect.Parameter:
    type_annotation = (
        type_hints.get(param.annotation)
        if isinstance(param.annotation, str)
        else param.annotation
    )
    return inspect.Parameter(
        name=param.name,
        kind=param.kind,
        annotation=type_annotation,
        default=param.default,
    )


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
        "authenticator",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=AuthenticatorOption,
    ),
    inspect.Parameter(
        "private_key_path",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=PrivateKeyPathOption,
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
