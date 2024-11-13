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
from typing import NoReturn

from click import ClickException
from snowflake.cli._plugins.nativeapp.sf_facade_constants import UseObjectType
from snowflake.connector import DatabaseError, Error, ProgrammingError


def handle_unclassified_error(err: Error | Exception, context: str) -> NoReturn:
    """
    Handles exceptions that are not caught by categorized exceptions in SQLFacade
    @param err: connector error or base exception
    @param context: message to add context to exception
    """
    message = f"{context} {str(err)}"
    if isinstance(err, ProgrammingError):
        raise InvalidSQLError(message) from err

    if isinstance(err, DatabaseError):
        raise UnknownSQLError(message) from err

    if isinstance(err, Error):
        raise UnknownConnectorError(message) from err

    # Not a connector error
    raise Exception(message) from err


class _BaseFacadeError(Exception):
    """Base class for SnowflakeFacade Exceptions"""

    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)

    def __str__(self):
        return self.msg


class InvalidSQLError(_BaseFacadeError):
    """Raised when Snowflake executed a SQL command but encountered an error, for example due to syntax or logical errors"""

    def __init__(self, msg):
        super().__init__(f"Invalid SQL error occurred. {msg}")


class UnknownSQLError(_BaseFacadeError):
    """Raised when Snowflake could not execute the SQL command"""

    def __init__(self, msg):
        super().__init__(f"Unknown SQL error occurred. {msg}")


class UnknownConnectorError(_BaseFacadeError):
    """Raised when there was a problem reaching Snowflake to execute a SQL command"""

    def __init__(self, msg):
        super().__init__(f"Unknown error occurred. {msg}")


class UnexpectedResultError(_BaseFacadeError):
    """Raised when an unexpected result was returned from execution of a SQL command"""

    def __init__(self, msg):
        super().__init__(f"Received unexpected result from query. {msg}")


class UserScriptError(ClickException):
    """Exception raised when user-provided scripts fail."""

    def __init__(self, script_name, msg):
        super().__init__(f"Failed to run script {script_name}. {msg}")


class UserInputError(ClickException):
    """Exception raised when execution of SQL with user input fails."""

    pass


class CouldNotUseObjectError(UserInputError):
    def __init__(self, object_type: UseObjectType, name: str):
        super().__init__(
            f"Could not use {object_type} {name}. Object does not exist, or operation cannot be performed."
        )


class InsufficientPrivilegesError(ClickException):
    """Raised when user does not have sufficient privileges to perform an operation"""

    def __init__(
        self,
        message,
        *,
        role: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ):
        if schema:
            message += f" in schema: {schema}"
        if database:
            message += f" in database: {database}"
        if role:
            message += f" using role: {role}"
        super().__init__(message)
