from dataclasses import dataclass
from typing import Callable, List

from snowcli.app.commands_registration.command_plugins_loader import (
    load_only_builtin_command_plugins,
)
from snowcli.app.commands_registration.typer_registration import (
    register_commands_from_plugins,
)
from snowcli.utils import ThreadsafeCounter


@dataclass
class CommandRegistrationConfig:
    enable_external_command_plugins: bool


class CommandsRegistrationWithCallbacks:
    def __init__(self):
        self._counter_of_callbacks_required_before_registration: ThreadsafeCounter = (
            ThreadsafeCounter(0)
        )
        self._counter_of_callbacks_invoked_before_registration: ThreadsafeCounter = (
            ThreadsafeCounter(0)
        )
        self._callbacks_after_registration: List[Callable[[], None]] = []
        self._commands_registration_config: CommandRegistrationConfig = (
            CommandRegistrationConfig(enable_external_command_plugins=True)
        )
        self._commands_already_registered: bool = False

    def register_commands_if_ready_and_not_registered_yet(self):
        all_required_callbacks_executed = (
            self._counter_of_callbacks_required_before_registration.value
            == self._counter_of_callbacks_invoked_before_registration.value
        )
        if all_required_callbacks_executed and not self._commands_already_registered:
            self._register_commands_from_plugins()

    def _register_commands_from_plugins(self) -> None:
        self._register_builtin_plugin_commands()

        if self._commands_registration_config.enable_external_command_plugins:
            self._register_external_plugin_commands()

        self._commands_already_registered = True
        for callback in self._callbacks_after_registration:
            callback()

    @staticmethod
    def _register_builtin_plugin_commands() -> None:
        loaded_builtin_command_plugins = load_only_builtin_command_plugins()
        register_commands_from_plugins(loaded_builtin_command_plugins)

    def _register_external_plugin_commands(self) -> None:
        pass  # TODO: To be done in a PR with external plugins

    def disable_external_command_plugins(self):
        self._commands_registration_config.enable_external_command_plugins = False

    def before(self, callback):
        def enriched_callback(value):
            self._counter_of_callbacks_invoked_before_registration.increment()
            callback(value)
            self.register_commands_if_ready_and_not_registered_yet()

        self._counter_of_callbacks_required_before_registration.increment()
        return enriched_callback

    def after(self, callback):
        def delayed_callback(value):
            self._callbacks_after_registration.append(lambda: callback(value))

        return delayed_callback

    def reset_running_instance_registration_state(self):
        self._commands_already_registered = False
        self._counter_of_callbacks_invoked_before_registration.set(0)
        self._callbacks_after_registration.clear()
        self._commands_registration_config.enable_external_command_plugins = True
