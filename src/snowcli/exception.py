from textwrap import dedent
from click.exceptions import ClickException
from typing import Optional

from snowcli.cli.constants import ObjectType


class EnvironmentVariableNotFoundError(ClickException):
    def __init__(self, env_variable_name: str):
        super().__init__(f"Environment variable {env_variable_name} not found")


class MissingConfiguration(ClickException):
    pass


class InvalidConnectionConfiguration(ClickException):
    def format_message(self):
        return f"Invalid connection configuration. {self.message}"


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
        super().__init__(f"Commads have to return OutputData type, but got {got_type}")


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
    def __init__(self, object_type: ObjectType, name: str):
        super().__init__(f"{object_type.value.capitalize()} {name} already exists.")


class MissingWarehouseError(ClickException):
    def __init__(self, errno: str, err_message: str):
        super().__init__(
            dedent(
                f"""\
            Could not execute SQL statement due to error: '{err_message}' with error code {errno}.
            Please add a warehouse for the active session role in your project definition file,
            config.toml file, or via command line.
            """
            )
        )
