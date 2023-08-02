from __future__ import annotations

import inspect
from functools import wraps
from typing import Callable, Optional, get_type_hints

from snowcli.cli.common.flags import (
    ConnectionOption,
    AccountOption,
    UserOption,
    DatabaseOption,
    SchemaOption,
    RoleOption,
    WarehouseOption,
    PasswordOption,
)


def global_options(func: Callable):
    """
    Decorator providing default flags for overriding global parameters. Values are
    updated in global SnowCLI state.

    To use this decorator your command needs to accept **kwargs as last argument.
    """

    @wraps(func)
    def wrapper(**kwargs):
        return func(**kwargs)

    wrapper.__signature__ = _extend_signature_with_global_options(func)  # type: ignore
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


def _extend_signature_with_global_options(func):
    """Extends function signature with global options"""
    sig = inspect.signature(func)

    # Remove **kwargs from signature
    existing_parameters = tuple(sig.parameters.values())[:-1]

    type_hints = get_type_hints(func)
    existing_parameters_with_evaluated_types = [
        inspect.Parameter(
            name=p.name,
            kind=p.kind,
            annotation=type_hints[p.name],
            default=p.default,
        )
        for p in existing_parameters
    ]
    sig = sig.replace(
        parameters=[
            *existing_parameters_with_evaluated_types,
            *GLOBAL_CONNECTION_OPTIONS,
        ]
    )
    return sig
