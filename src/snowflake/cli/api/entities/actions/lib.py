import textwrap
from dataclasses import dataclass, replace
from inspect import Parameter, Signature
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TypeVar

import typer
from snowflake.cli.api.console.abc import AbstractConsole

T = TypeVar("T")


class InvalidActionDefintionError(Exception):
    """
    The action implementation in question is not a valid action definition.
    """

    pass


@dataclass
class HelpText:
    """
    Annotation metadata to register help text for a specific option / argument.
    """

    value: str
    strip: bool = True
    dedent: bool = True

    def __post_init__(self):
        if self.dedent:
            self.value = textwrap.dedent(self.value)
        if self.strip:
            self.value = self.value.strip()


@dataclass
class ParameterDeclarations:
    """
    Annotation metadata to declare argument names on the command line, e.g.
    ParameterDeclarations("--interactive/--no-interactive", "-i")
    """

    decls: List[str]


@dataclass
class ActionContext:
    """
    An object that is passed to each action when called by WorkspaceManager.
    """

    console: AbstractConsole
    project_root: Path
    default_role: str
    default_warehouse: Optional[str]

    def clone(self, **kwargs) -> "ActionContext":
        return replace(self, **kwargs)


def _get_metadata(param: Parameter, cls: Type[T]) -> T | None:
    """
    Returns the instance of a specified class that exists in a parameter's
    annotations, or None if no such instance exists.
    """
    if hasattr(param.annotation, "__metadata__"):
        for metadatum in param.annotation.__metadata__:
            if isinstance(metadatum, cls):
                return metadatum
    return None


def _is_param_of_type(param: Parameter, cls: Type) -> bool:
    """
    Is the given parameter of the given type?
    Works with typing.Annotated and typing.Optional.
    """
    if param.annotation == cls:
        return True
    if not hasattr(param.annotation, "__args__"):
        return False
    return cls in param.annotation.__args__


def signature_to_typer_params(sig: Signature) -> List[Parameter]:
    """
    Takes an entity action implementation (callable) and generates a list of
    typer.Parameters combined with typer.Option / typer.Argument to be passed
    into _options_decorator_factory or otherwise merged in with typer command
    implementations.
    """

    [ctx_param, *rest_params] = sig.parameters.values()
    if ctx_param.annotation != ActionContext:
        raise InvalidActionDefintionError(
            "Invalid action definition: first argument must be ActionContext"
        )

    typer_params = []
    for param in rest_params:
        if param.kind in [Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD]:
            raise InvalidActionDefintionError(
                "Invalid action definition: variable (kw)args are not supported"
            )

        typer_factory = (
            typer.Argument if param.kind == Parameter.POSITIONAL_ONLY else typer.Option
        )
        typer_args = []
        typer_kwargs: Dict[str, Any] = {}

        # help text for this option
        if help_text := _get_metadata(param, HelpText):
            typer_kwargs["help"] = help_text.value

        # overrides for argument name
        if param_decls := _get_metadata(param, ParameterDeclarations):
            typer_args.extend(param_decls.decls)

        # FIXME: is this necessary?
        if _is_param_of_type(param, bool):
            typer_kwargs["is_flag"] = True

        typer_params.append(
            param.replace(
                default=typer_factory(
                    param.default,
                    *typer_args,
                    **typer_kwargs,
                )
            )
        )

    return typer_params
