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

from snowflake.cli.api.exceptions import InvalidSchemaError
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.connector import SnowflakeConnection
from snowflake.connector.compat import IS_WINDOWS

if TYPE_CHECKING:
    from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition

schema_pattern = re.compile(r".+\..+")


class _ConnectionContext:
    def __init__(self):
        self._cached_connection: Optional[SnowflakeConnection] = None

        self._connection_name: Optional[str] = None
        self._account: Optional[str] = None
        self._database: Optional[str] = None
        self._role: Optional[str] = None
        self._schema: Optional[str] = None
        self._user: Optional[str] = None
        self._password: Optional[str] = None
        self._authenticator: Optional[str] = None
        self._private_key_file: Optional[str] = None
        self._warehouse: Optional[str] = None
        self._mfa_passcode: Optional[str] = None
        self._enable_diag: Optional[bool] = False
        self._diag_log_path: Optional[Path] = None
        self._diag_allowlist_path: Optional[Path] = None
        self._temporary_connection: bool = False
        self._session_token: Optional[str] = None
        self._master_token: Optional[str] = None
        self._token_file_path: Optional[Path] = None

    def __setattr__(self, key, value):
        """
        We invalidate connection cache every time connection attributes change.
        """
        super().__setattr__(key, value)
        if key != "_cached_connection":
            self._cached_connection = None

    @property
    def connection_name(self) -> Optional[str]:
        return self._connection_name

    def set_connection_name(self, value: Optional[str]):
        self._connection_name = value

    @property
    def account(self) -> Optional[str]:
        return self._account

    def set_account(self, value: Optional[str]):
        self._account = value

    @property
    def database(self) -> Optional[str]:
        return self._database

    def set_database(self, value: Optional[str]):
        self._database = value

    @property
    def role(self) -> Optional[str]:
        return self._role

    def set_role(self, value: Optional[str]):
        self._role = value

    @property
    def schema(self) -> Optional[str]:
        return self._schema

    def set_schema(self, value: Optional[str]):
        if (
            value
            and not (value.startswith('"') and value.endswith('"'))
            # if schema is fully qualified name (db.schema)
            and schema_pattern.match(value)
        ):
            raise InvalidSchemaError(value)
        self._schema = value

    @property
    def user(self) -> Optional[str]:
        return self._user

    def set_user(self, value: Optional[str]):
        self._user = value

    @property
    def password(self) -> Optional[str]:
        return self._password

    def set_password(self, value: Optional[str]):
        self._password = value

    @property
    def authenticator(self) -> Optional[str]:
        return self._authenticator

    def set_authenticator(self, value: Optional[str]):
        self._authenticator = value

    @property
    def private_key_file(self) -> Optional[str]:
        return self._private_key_file

    def set_private_key_file(self, value: Optional[str]):
        self._private_key_file = value

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    def set_warehouse(self, value: Optional[str]):
        self._warehouse = value

    @property
    def mfa_passcode(self) -> Optional[str]:
        return self._mfa_passcode

    def set_mfa_passcode(self, value: Optional[str]):
        self._mfa_passcode = value

    @property
    def enable_diag(self) -> Optional[bool]:
        return self._enable_diag

    def set_enable_diag(self, value: Optional[bool]):
        self._enable_diag = value

    @property
    def diag_log_path(self) -> Optional[Path]:
        return self._diag_log_path

    def set_diag_log_path(self, value: Optional[Path]):
        self._diag_log_path = value

    @property
    def diag_allowlist_path(self) -> Optional[Path]:
        return self._diag_allowlist_path

    def set_diag_allowlist_path(self, value: Optional[Path]):
        self._diag_allowlist_path = value

    @property
    def temporary_connection(self) -> bool:
        return self._temporary_connection

    def set_temporary_connection(self, value: bool):
        self._temporary_connection = value

    @property
    def session_token(self) -> Optional[str]:
        return self._session_token

    def set_session_token(self, value: Optional[str]):
        self._session_token = value

    @property
    def master_token(self) -> Optional[str]:
        return self._master_token

    def set_master_token(self, value: Optional[str]):
        self._master_token = value

    @property
    def token_file_path(self) -> Optional[Path]:
        return self._token_file_path

    def set_token_file_path(self, value: Optional[Path]):
        self._token_file_path = value

    @property
    def connection(self) -> SnowflakeConnection:
        if not self._cached_connection:
            self._cached_connection = self._build_connection()
        return self._cached_connection

    def _collect_not_empty_connection_attributes(self):
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "authenticator": self.authenticator,
            "private_key_file": self.private_key_file,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
            "warehouse": self.warehouse,
            "session_token": self.session_token,
            "master_token": self.master_token,
            "token_file_path": self.token_file_path,
        }

    def _build_connection(self):
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
            mfa_passcode=self._mfa_passcode,
            enable_diag=self._enable_diag,
            diag_log_path=self._diag_log_path,
            diag_allowlist_path=self._diag_allowlist_path,
            connection_name=self.connection_name,
            **self._collect_not_empty_connection_attributes(),
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
