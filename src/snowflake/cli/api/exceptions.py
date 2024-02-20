from __future__ import annotations

from pathlib import Path
from typing import Optional

from click.exceptions import ClickException
from snowflake.cli.api.constants import ObjectType


class EnvironmentVariableNotFoundError(ClickException):
    def __init__(self, env_variable_name: str):
        super().__init__(f"Environment variable {env_variable_name} not found")


class MissingConfiguration(ClickException):
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
    def __init__(self, project_type: str, project_file: str):
        super().__init__(
            f"No {project_type} project definition found in {project_file}"
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
        super().__init__(
            f'Configuration file {path} has too wide permissions, run `chmod 0600 "{path}"`'
        )


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
