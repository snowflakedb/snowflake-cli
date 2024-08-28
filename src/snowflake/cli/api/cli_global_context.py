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

import re
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional

from snowflake.cli.api.config import ConnectionConfig
from snowflake.cli.api.exceptions import InvalidSchemaError
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.connector import SnowflakeConnection
from snowflake.connector.compat import IS_WINDOWS

if TYPE_CHECKING:
    from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition

SET_PREFIX = "set_"

schema_pattern = re.compile(r".+\..+")


class _ConnectionContext:
    """
    A wrapper around ConnectionConfig that contains additional CLI-specific
    values we need to pass to connect_to_snowflake.
    Maintains its own cached connection.
    """

    _cached_connection: Optional[SnowflakeConnection] = None
    _config: ConnectionConfig = ConnectionConfig()

    connection_name: Optional[str] = None
    temporary_connection: bool = False
    mfa_passcode: Optional[str] = None
    enable_diag: bool = False
    diag_log_path: Optional[Path] = None
    diag_allowlist_path: Optional[Path] = None

    @classmethod
    def _is_own_attr(cls, key: str) -> bool:
        """
        Is this an attribute that we store on this object directly
        and will pass to connect_to_snowflake, or does it live inside
        of our ConnectionConfig?
        """
        return key in cls.__dict__ and not callable(getattr(cls, key))

    def __getattr__(self, key: str):
        """
        Delegation to ConnectionConfig and automatic creation of missing
        set_* methods for both local attrs and configuration values.
        """
        if key.startswith(SET_PREFIX):
            # defines dynamic setters for all attrs/config that don't have explicit setters.
            # this allows us to call e.g. _ConnectionCotext.set_connection_name(name)
            attr_key = key[len(SET_PREFIX) :]

            def setter(value):
                setattr(self, attr_key, value)

            return setter

        elif hasattr(self._config, key):
            # delegate to ConnectionConfig
            return getattr(self._config, key)

        return getattr(self, key)

    def __setattr__(self, key: str, value):
        """
        Sets the given attribute in either the ConnectionConfig or locally.
        We invalidate connection cache every time connection attributes change.
        """
        if self._is_own_attr(key):
            super().__setattr__(key, value)
        else:
            setattr(self._config, key, value)

        if key != "_cached_connection":
            # FIXME: should we close the connection here?
            self._cached_connection = None

    def set_schema(self, value: Optional[str]):
        # overrides the dynamic setters registered above
        if (
            value
            and not (value.startswith('"') and value.endswith('"'))
            # if schema is fully qualified name (db.schema)
            and schema_pattern.match(value)
        ):
            raise InvalidSchemaError(value)
        self.schema = value

    @property
    def connection(self) -> SnowflakeConnection:
        if not self._cached_connection:
            self._cached_connection = self._connect()
        return self._cached_connection

    def _connect(self):
        from snowflake.cli._app.snow_connector import connect_to_snowflake

        # Ignore warnings about bad owner or permissions on Windows
        # Telemetry omit our warning filter from config.py
        if IS_WINDOWS:
            warnings.filterwarnings(
                action="ignore",
                message="Bad owner or permissions.*",
                module="snowflake.connector.config_manager",
            )

        return connect_to_snowflake(
            temporary_connection=self.temporary_connection,
            mfa_passcode=self.mfa_passcode,
            enable_diag=self.enable_diag,
            diag_log_path=self.diag_log_path,
            diag_allowlist_path=self.diag_allowlist_path,
            connection_name=self.connection_name,
            **self._config.to_dict_of_all_non_empty_values(),
        )


class _CliGlobalContextManager:
    def __init__(self):
        self._connection_context = _ConnectionContext()
        self._enable_tracebacks = True
        self._output_format = OutputFormat.TABLE
        self._verbose = False
        self._experimental = False
        self._project_definition = None
        self._project_root = None
        self._project_path_arg = None
        self._project_env_overrides_args = {}
        self._typer_pre_execute_commands = []
        self._template_context = None
        self._silent: bool = False

    def reset(self):
        self.__init__()

    @property
    def enable_tracebacks(self) -> bool:
        return self._enable_tracebacks

    def set_enable_tracebacks(self, value: bool):
        self._enable_tracebacks = value

    @property
    def output_format(self) -> OutputFormat:
        return self._output_format

    def set_output_format(self, value: OutputFormat):
        self._output_format = value

    @property
    def verbose(self) -> bool:
        return self._verbose

    def set_verbose(self, value: bool):
        self._verbose = value

    @property
    def experimental(self) -> bool:
        return self._experimental

    def set_experimental(self, value: bool):
        self._experimental = value

    @property
    def project_definition(self) -> Optional[ProjectDefinition]:
        return self._project_definition

    def set_project_definition(self, value: ProjectDefinition):
        self._project_definition = value

    @property
    def project_root(self):
        return self._project_root

    def set_project_root(self, project_root: Path):
        self._project_root = project_root

    @property
    def project_path_arg(self) -> Optional[str]:
        return self._project_path_arg

    def set_project_path_arg(self, project_path_arg: str):
        self._project_path_arg = project_path_arg

    @property
    def project_env_overrides_args(self) -> dict[str, str]:
        return self._project_env_overrides_args

    def set_project_env_overrides_args(
        self, project_env_overrides_args: dict[str, str]
    ):
        self._project_env_overrides_args = project_env_overrides_args

    @property
    def template_context(self) -> dict:
        return self._template_context

    def set_template_context(self, template_context: dict):
        self._template_context = template_context

    @property
    def typer_pre_execute_commands(self) -> list[Callable[[], None]]:
        return self._typer_pre_execute_commands

    def add_typer_pre_execute_commands(
        self, typer_pre_execute_command: Callable[[], None]
    ):
        self._typer_pre_execute_commands.append(typer_pre_execute_command)

    @property
    def connection_context(self) -> _ConnectionContext:
        return self._connection_context

    @property
    def connection(self) -> SnowflakeConnection:
        return self.connection_context.connection

    @property
    def silent(self) -> bool:
        return self._silent

    def set_silent(self, value: bool):
        self._silent = value


class _CliGlobalContextAccess:
    def __init__(self, manager: _CliGlobalContextManager):
        self._manager = manager

    @property
    def connection(self) -> SnowflakeConnection:
        return self._manager.connection

    @property
    def connection_context(self) -> _ConnectionContext:
        return self._manager.connection_context

    @property
    def enable_tracebacks(self) -> bool:
        return self._manager.enable_tracebacks

    @property
    def output_format(self) -> OutputFormat:
        return self._manager.output_format

    @property
    def verbose(self) -> bool:
        return self._manager.verbose

    @property
    def experimental(self) -> bool:
        return self._manager.experimental

    @property
    def project_definition(self) -> ProjectDefinition | None:
        return self._manager.project_definition

    @property
    def project_root(self) -> Path:
        return Path(self._manager.project_root)

    @property
    def template_context(self) -> dict:
        return self._manager.template_context

    @property
    def silent(self) -> bool:
        if self._should_force_mute_intermediate_output:
            return True
        return self._manager.silent

    @property
    def _should_force_mute_intermediate_output(self) -> bool:
        """Computes whether cli_console output should be muted."""
        return self._manager.output_format == OutputFormat.JSON


_CLI_CONTEXT_MANAGER: _CliGlobalContextManager | None = None
_CLI_CONTEXT: _CliGlobalContextAccess | None = None


def get_cli_context_manager() -> _CliGlobalContextManager:
    global _CLI_CONTEXT_MANAGER
    if _CLI_CONTEXT_MANAGER is None:
        _CLI_CONTEXT_MANAGER = _CliGlobalContextManager()
    return _CLI_CONTEXT_MANAGER


def get_cli_context() -> _CliGlobalContextAccess:
    global _CLI_CONTEXT
    if _CLI_CONTEXT is None:
        _CLI_CONTEXT = _CliGlobalContextAccess(get_cli_context_manager())
    return _CLI_CONTEXT
