from copy import deepcopy
from dataclasses import dataclass
from typing import Callable


@dataclass
class SnowCliGlobalContext:
    """
    Global state accessible in whole CLI code.
    """

    enable_tracebacks: bool


class SnowCliGlobalContextManager:
    """
    A manager responsible for retrieving and updating global state.
    """

    def __init__(self, global_context_with_default_values: SnowCliGlobalContext):
        self._global_context = deepcopy(global_context_with_default_values)

    def get_global_context_copy(self) -> SnowCliGlobalContext:
        """
        Returns deep copy of global state.
        """
        return deepcopy(self._global_context)

    def update_global_context(
        self, update: Callable[[SnowCliGlobalContext], SnowCliGlobalContext]
    ) -> None:
        """
        Updates global state using provided function.
        The resulting object will be deep copied before storing in the manager.
        """
        self._global_context = deepcopy(update(self.get_global_context_copy()))


def _create_snow_cli_global_context_manager_with_default_values() -> SnowCliGlobalContextManager:
    """
    Creates a manager with global state filled with default values.
    """
    return SnowCliGlobalContextManager(SnowCliGlobalContext(enable_tracebacks=True))


snow_cli_global_context_manager = (
    _create_snow_cli_global_context_manager_with_default_values()
)
