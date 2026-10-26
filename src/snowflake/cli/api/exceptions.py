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

from pathlib import Path
from typing import Any, List, Optional

from click.exceptions import ClickException, UsageError
from snowflake.cli.api.constants import IS_WINDOWS, ObjectType


class BaseCliError(ClickException):
    """Base Cli Exception.

    0 Everything ran smoothly.
    1 Something went wrong with the client.
    2 Something went wrong with command-line arguments.
    3 Cli could not connect to server.
    4 Cli could not communicate properly with server.
    5 The enhanced_exit_codes parameter was set and Cli exited because of error.
    """

    def __init__(self, *args, **kwargs):
        from snowflake.cli.api.cli_global_context import get_cli_context

        if not get_cli_context().enhanced_exit_codes:
            self.exit_code = kwargs.pop("exit_code", 1)
        super().__init__(*args, **kwargs)


class CliError(BaseCliError):
    """Generic Cli Error - to be used in favour of ClickException."""

    exit_code = 1


class CliArgumentError(BaseCliError):
    exit_code = 2


class CliConnectionError(BaseCliError):
    exit_code = 3


class CliCommunicationError(BaseCliError):
    exit_code = 4


class CliSqlError(BaseCliError):
    exit_code = 5


class EnvironmentVariableNotFoundError(CliError):
    def __init__(self, env_variable_name: str):
        super().__init__(f"Environment variable {env_variable_name} not found")


class MissingConfigurationError(CliError):
    pass


class CycleDetectedError(CliError):
    pass


class InvalidTemplateError(CliError):
    pass


class InvalidConnectionConfigurationError(CliError):
    def format_message(self):
        return f"Invalid connection configuration. {self.message}"


class InvalidLogsConfigurationError(CliError):
    def format_message(self):
        return f"Invalid logs configuration. {self.message}"


class InvalidPluginConfigurationError(CliError):
    def format_message(self):
        return f"Invalid plugin configuration. {self.message}"


class PluginNotInstalledError(CliError):
    def __init__(self, plugin_name, installed_plugins: List[str]):
        super().__init__(
            f"Plugin {plugin_name} is not installed. Available plugins: {', '.join(installed_plugins)}."
        )


class SnowflakeConnectionError(CliError):
    def __init__(self, snowflake_err: Exception):
        super().__init__(f"Could not connect to Snowflake. Reason: {snowflake_err}")


class UnsupportedConfigSectionTypeError(Exception):
    def __init__(self, section_type: type):
        super().__init__(f"Unsupported configuration section type {section_type}")


class OutputDataTypeError(CliError):
    def __init__(self, got_type: type, expected_type: type):
        super().__init__(f"Got {got_type} type but expected {expected_type}")


class CommandReturnTypeError(CliError):
    def __init__(self, got_type: type):
        super().__init__(f"Commands have to return OutputData type, but got {got_type}")


class SnowflakeSQLExecutionError(CliError):
    """
    Could not successfully execute the Snowflake SQL statements.
    """

    def __init__(self, queries: Optional[str] = None):
        super().__init__(
            f"""
                {self.__doc__}
                {queries if queries else ""}
            """
        )


class ObjectAlreadyExistsError(CliError):
    def __init__(
        self,
        object_type: ObjectType,
        name: str,
        replace_available: bool = False,
    ):
        msg = f"{str(object_type).capitalize()} {name} already exists."
        if replace_available:
            msg += " Use --replace flag to update objects."
        super().__init__(msg)


class NoProjectDefinitionError(CliError):
    def __init__(self, project_type: str, project_root: str | Path):
        super().__init__(
            f"No {project_type} project definition found in {project_root}"
        )


class InvalidProjectDefinitionVersionError(CliError):
    def __init__(self, expected_version: str, actual_version: str):
        super().__init__(
            f"This command only supports definition version {expected_version}, got {actual_version}."
        )


class InvalidSchemaError(CliError):
    def __init__(self, schema: str):
        super().__init__(f"Invalid schema {schema}")


class SecretsWithoutExternalAccessIntegrationError(CliError):
    def __init__(self, object_name: str):
        super().__init__(
            f"{object_name} defined with secrets but without external integration."
        )


class FileTooLargeError(CliError):
    def __init__(self, path: Path, size_limit_in_kb: int):
        super().__init__(
            f"File {path} is too large (size limit: {size_limit_in_kb} KB)"
        )


class DirectoryIsNotEmptyError(CliError):
    def __init__(self, path: Path):
        super().__init__(f"Directory '{path}' is not empty")


class ConfigFileTooWidePermissionsError(CliError):
    """Config file permissions are too open.

    The remediation command is not part of the message. Typer prints Click
    errors in a Rich panel that hard-wraps and draws a border on every row,
    so a wrapped chmod or icacls line cannot be copied.
    """

    plain_remediation_commands: tuple[str, ...]

    def __init__(self, path: Path):
        quoted_path = f'"{path}"'
        if IS_WINDOWS:
            self.plain_remediation_commands = (
                f"icacls {quoted_path} /deny <USER_ID>:F",
                f"icacls {quoted_path}",
            )
            instruction = (
                "Run the first command below for every user except you "
                "and administrators. "
                "The second command lists who currently has access."
            )
        else:
            self.plain_remediation_commands = (f"chmod 0600 {quoted_path}",)
            instruction = "Run the command below exactly as printed."
        super().__init__(
            f"Configuration file {path} has too wide permissions. {instruction}"
        )

    def __str__(self) -> str:
        # Some paths print str(exception) and never reach the Rich panel.
        # Keep the command out of format_message(), but on the next line here.
        return "\n".join((self.message, *self.plain_remediation_commands))

    def show(self, file: Optional[Any] = None) -> None:
        super().show(file)
        _write_remediation_commands(self, file)


def _write_remediation_commands(
    exc: ConfigFileTooWidePermissionsError, file: Optional[Any] = None
) -> None:
    # Local import: console -> config -> exceptions.
    from snowflake.cli.api.console.console import write_raw_line

    for command in exc.plain_remediation_commands:
        write_raw_line(command, file=file)


_PREVIOUS_ERROR_FORMATTER = "_previous_error_formatter"


def error_renderer_already_installed(formatter: Any, flag: str) -> bool:
    """True when ``flag`` is set on this formatter or one it calls.

    Each installer used to look only at the outermost function. Once the
    other wrapper sat outside, the flag was hidden and every ``snow``
    command stacked another pair. Formatting an error then recursed.
    """
    seen: set[int] = set()
    current = formatter
    while current is not None and id(current) not in seen:
        if getattr(current, flag, False):
            return True
        seen.add(id(current))
        current = getattr(current, _PREVIOUS_ERROR_FORMATTER, None)
    return False


def link_error_renderer(wrapper: Any, flag: str, previous: Any) -> None:
    setattr(wrapper, flag, True)
    setattr(wrapper, _PREVIOUS_ERROR_FORMATTER, previous)


def install_plain_remediation_error_renderer() -> None:
    """Append remediation commands after Typer's error panel.

    Typer calls rich_format_error instead of ClickException.show.
    """
    import typer.rich_utils as rich_utils

    current = rich_utils.rich_format_error
    if error_renderer_already_installed(current, "_plain_remediation_renderer"):
        return

    def _format_error(error: ClickException) -> None:
        current(error)
        if isinstance(error, ConfigFileTooWidePermissionsError):
            _write_remediation_commands(error)

    link_error_renderer(_format_error, "_plain_remediation_renderer", current)
    rich_utils.rich_format_error = _format_error


class DatabaseNotProvidedError(CliError):
    def __init__(self):
        super().__init__(
            "Database not specified. Please update connection to add `database` parameter, or re-run command using `--database` option. Use `snow connection list` to list existing connections."
        )


class SchemaNotProvidedError(CliError):
    def __init__(self):
        super().__init__(
            "Schema not specified. Please update connection to add `schema` parameter, or re-run command using `--schema` option. Use `snow connection list` to list existing connections."
        )


class FQNNameError(CliError):
    def __init__(self, name: str):
        super().__init__(f"Specified name '{name}' is not valid name.")


class FQNInconsistencyError(CliError):
    def __init__(self, part: str, name: str):
        super().__init__(
            f"{part.capitalize()} provided but name '{name}' is fully qualified name."
        )


class IncompatibleParametersError(UsageError):
    def __init__(self, options: list[str]):
        options_with_quotes = [f"'{option}'" for option in options]
        comma_separated_options = ", ".join(options_with_quotes[:-1])
        super().__init__(
            f"Parameters {comma_separated_options} and {options_with_quotes[-1]} are incompatible and cannot be used simultaneously."
        )


class UnmetParametersError(UsageError):
    def __init__(self, options: list[str]):
        options_with_quotes = [f"'{option}'" for option in options]
        comma_separated_options = ", ".join(options_with_quotes[:-1])
        super().__init__(
            f"Parameters {comma_separated_options} and {options_with_quotes[-1]} must be used simultaneously."
        )


class NoWarehouseSelectedInSessionError(CliError):
    def __init__(self, msg: str):
        super().__init__(
            "Received the following error message while executing SQL statement:\n"
            f"'{msg}'\n"
            "Please provide a warehouse for the active session role in your project definition file, config.toml file, or via command line."
        )


class DoesNotExistOrUnauthorizedError(CliError):
    def __init__(self, msg: str):
        super().__init__(
            "Received the following error message while executing SQL statement:\n"
            f"'{msg}'\n"
            "Please check the name of the resource you are trying to query or the permissions of the role you are using to run the query."
        )


class CouldNotUseObjectError(CliError):
    def __init__(self, object_type: ObjectType, name: str):
        super().__init__(
            f"Could not use {object_type} {name}. Object does not exist, or operation cannot be performed."
        )


class ShowSpecificObjectMultipleRowsError(RuntimeError):
    def __init__(self, show_obj_query: str):
        super().__init__(
            f"Received multiple rows from result of SQL statement: {show_obj_query}. Usage of 'show_specific_object' may not be properly scoped."
        )


class CouldNotSetKeyPairError(CliError):
    def __init__(self):
        super().__init__(
            "The public key is set already. Use the rotate command instead."
        )
