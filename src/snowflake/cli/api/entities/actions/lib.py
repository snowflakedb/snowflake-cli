import functools
import textwrap
from dataclasses import dataclass
from typing import Callable, List, TypeVar

T = TypeVar["Value"]

@dataclass
class HelpText:
    value: str

    def __init__(self, value: str, *args, dedent=True, strip=True, **kwargs):
        if dedent:
            value = textwrap.dedent(value)
        if strip:
            value = value.strip()
        super().__init__(value=value, **args, **kwargs)


@dataclass
class ParameterDeclarations:
    decls: List[str]


@dataclass
class DefaultValue[T]:
    """
    Represents a default value for the argument list of an action invocation,
    either via regular python __call__ or typer command.
    """
    _value: T | None
    factory: Callable[[], T]| None

    @functools.cached_property
    def value(self) -> T:
        return self._value or self.factory()
