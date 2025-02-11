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
from typing import Optional

from click.exceptions import ClickException, UsageError
from snowflake.cli.api.constants import ObjectType
from snowflake.connector.compat import IS_WINDOWS


class EnvironmentVariableNotFoundError(ClickException):
    def __init__(self, env_variable_name: str):
        super().__init__(f"Environment variable {env_variable_name} not found")


class MissingConfiguration(ClickException):
    pass


class CycleDetectedError(ClickException):
    pass


class InvalidTemplate(ClickException):
    pass


class InvalidConnectionConfiguration(ClickException):
    def format_message(self):
        return f"Invalid connection configuration. {self.message}"


class InvalidLogsConfiguration(ClickException):
    def format_message(self):
        return f"Invalid logs configuration. {self.message}"


class InvalidPluginConfiguration(ClickException):
    def format_message(self):
        return f"Invalid plugin configuration. {self.message}"


class SnowflakeConnectionError(ClickException):
    def __init__(self, snowflake_err: Exception):
        super().__init__(f"Could not connect to Snowflake. Reason: {snowflake_err}")


class UnsupportedConfigSectionTypeError(Exception):
    def __init__(self, section_type: type):
        super().__init__(f"Unsupported configuration section type {section_type}")


class OutputDataTypeError(ClickException):
    def __init__(self, got_type: type, expected_type: type):
        super().__init__(f"Got {got_type} type but expected {expected_type}")


class CommandReturnTypeError(ClickException):
    def __init__(self, got_type: type):
        super().__init__(f"Commands have to return OutputData type, but got {got_type}")


class SnowflakeSQLExecutionError(ClickException):
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


class ObjectAlreadyExistsError(ClickException):
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


class NoProjectDefinitionError(ClickException):
    def __init__(self, project_type: str, project_root: str | Path):
        super().__init__(
            f"No {project_type} project definition found in {project_root}"
        )


class InvalidProjectDefinitionVersionError(ClickException):
    def __init__(self, expected_version: str, actual_version: str):
        super().__init__(
            f"This command only supports definition version {expected_version}, got {actual_version}."
        )


class InvalidSchemaError(ClickException):
    def __init__(self, schema: str):
        super().__init__(f"Invalid schema {schema}")


class SecretsWithoutExternalAccessIntegrationError(ClickException):
    def __init__(self, object_name: str):
        super().__init__(
            f"{object_name} defined with secrets but without external integration."
        )


class FileTooLargeError(ClickException):
    def __init__(self, path: Path, size_limit_in_kb: int):
        super().__init__(
            f"File {path} is too large (size limit: {size_limit_in_kb} KB)"
        )


class DirectoryIsNotEmptyError(ClickException):
    def __init__(self, path: Path):
        super().__init__(f"Directory '{path}' is not empty")


class ConfigFileTooWidePermissionsError(ClickException):
    def __init__(self, path: Path):
        change_permissons_command = (
            f'icacls "{path}" /deny <USER_ID>:F'
            if IS_WINDOWS
            else f'chmod 0600 "{path}"'
        )
        msg = f"Configuration file {path} has too wide permissions, run `{change_permissons_command}`."
        if IS_WINDOWS:
            msg += (
                f'\nTo check which users have access to the file run `icacls "{path}"`.'
                "Run the above command for all users except you and administrators."
            )
        super().__init__(msg)


class DatabaseNotProvidedError(ClickException):
    def __init__(self):
        super().__init__(
            "Database not specified. Please update connection to add `database` parameter, or re-run command using `--database` option. Use `snow connection list` to list existing connections."
        )


class SchemaNotProvidedError(ClickException):
    def __init__(self):
        super().__init__(
            "Schema not specified. Please update connection to add `schema` parameter, or re-run command using `--schema` option. Use `snow connection list` to list existing connections."
        )


class FQNNameError(ClickException):
    def __init__(self, name: str):
        super().__init__(f"Specified name '{name}' is not valid name.")


class FQNInconsistencyError(ClickException):
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


class NoWarehouseSelectedInSessionError(ClickException):
    def __init__(self, msg: str):
        super().__init__(
            "Received the following error message while executing SQL statement:\n"
            f"'{msg}'\n"
            "Please provide a warehouse for the active session role in your project definition file, config.toml file, or via command line."
        )


class DoesNotExistOrUnauthorizedError(ClickException):
    def __init__(self, msg: str):
        super().__init__(
            "Received the following error message while executing SQL statement:\n"
            f"'{msg}'\n"
            "Please check the name of the resource you are trying to query or the permissions of the role you are using to run the query."
        )


class CouldNotUseObjectError(ClickException):
    def __init__(self, object_type: ObjectType, name: str):
        super().__init__(
            f"Could not use {object_type} {name}. Object does not exist, or operation cannot be performed."
        )


class ShowSpecificObjectMultipleRowsError(RuntimeError):
    def __init__(self, show_obj_query: str):
        super().__init__(
            f"Received multiple rows from result of SQL statement: {show_obj_query}. Usage of 'show_specific_object' may not be properly scoped."
        )
