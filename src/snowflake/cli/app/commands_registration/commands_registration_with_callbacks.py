from dataclasses import dataclass
from typing import Callable, List

from snowflake.cli.api.plugins.plugin_config import PluginConfigProvider
from snowflake.cli.app.commands_registration.command_plugins_loader import (
    load_builtin_and_external_command_plugins,
    load_only_builtin_command_plugins,
)
from snowflake.cli.app.commands_registration.threadsafe import ThreadsafeCounter
from snowflake.cli.app.commands_registration.typer_registration import (
    register_commands_from_plugins,
)


@dataclass
class CommandRegistrationConfig:
    enable_external_command_plugins: bool


class CommandsRegistrationWithCallbacks:
    def __init__(self, plugin_config_provider: PluginConfigProvider):
        self._plugin_config_provider = plugin_config_provider
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
        if self._commands_registration_config.enable_external_command_plugins:
            self._register_builtin_and_enabled_external_plugin_commands()
        else:
            self._register_only_builtin_plugin_commands()

        self._commands_already_registered = True
        for callback in self._callbacks_after_registration:
            callback()

    @staticmethod
    def _register_only_builtin_plugin_commands() -> None:
        loaded_command_plugins = load_only_builtin_command_plugins()
        register_commands_from_plugins(loaded_command_plugins)

    def _register_builtin_and_enabled_external_plugin_commands(self):
        enabled_external_plugins = (
            self._plugin_config_provider.get_enabled_plugin_names()
        )
        loaded_command_plugins = load_builtin_and_external_command_plugins(
            enabled_external_plugins
        )
        register_commands_from_plugins(loaded_command_plugins)

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
