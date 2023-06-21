from copy import deepcopy
from dataclasses import dataclass
from typing import Callable


@dataclass
class SnowCliGlobalContext:
    enable_tracebacks: bool


class SnowCliGlobalContextManager:
    def __init__(self, global_context_with_default_values: SnowCliGlobalContext):
        self._global_context = deepcopy(global_context_with_default_values)

    def get_global_context_copy(self) -> SnowCliGlobalContext:
        return deepcopy(self._global_context)

    def update_global_context(
        self, update: Callable[[SnowCliGlobalContext], SnowCliGlobalContext]
    ) -> None:
        self._global_context = deepcopy(update(self.get_global_context_copy()))


def _create_snow_cli_global_context_manager_with_default_values() -> SnowCliGlobalContextManager:
    return SnowCliGlobalContextManager(SnowCliGlobalContext(enable_tracebacks=False))


snow_cli_global_context_manager = (
    _create_snow_cli_global_context_manager_with_default_values()
)
