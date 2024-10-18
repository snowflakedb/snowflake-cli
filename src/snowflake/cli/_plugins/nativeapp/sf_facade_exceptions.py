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

from click import ClickException
from snowflake.cli.api.constants import UseObjectType
from snowflake.connector import DatabaseError, Error


def handle_unclassified_error(err: Error | Exception, context: str):
    """
    Handles exceptions that are not caught by categorized exceptions in SQLFacade
    @param err: connector error or base exception
    @param context: message to add context to exception
    """
    message = f"{context} {str(err)}"
    if isinstance(err, DatabaseError):
        raise UnknownSQLError(message) from err

    if isinstance(err, Error):
        raise UnknownConnectorError(message) from err

    # Not a connector error
    raise Exception(f"Unclassified exception occurred. {message}") from err


class UnknownSQLError(Exception):
    """Exception raised when the root of the DatabaseError is unidentified."""

    def __init__(self, msg):
        self.msg = f"Unknown SQL error occurred. {msg}"
        super().__init__(self.msg)

    def __str__(self):
        return self.msg


class UnknownConnectorError(Exception):
    """Exception raised when the root of the error thrown by connector is unidentified."""

    def __init__(self, msg):
        self.msg = f"Unknown error occurred. {msg}"
        super().__init__(self.msg)

    def __str__(self):
        return self.msg


class UserScriptError(ClickException):
    """Exception raised when user-provided scripts fail."""

    def __init__(self, script_name, msg):
        super().__init__(f"Failed to run script {script_name}. {msg}")


class SQLWithUserInputError(ClickException):
    """Exception raised when execution of SQL with user input fails."""

    pass


class CouldNotUseObjectError(SQLWithUserInputError):
    def __init__(self, object_type: UseObjectType, name: str):
        super().__init__(
            f"Could not use {object_type} {name}. Object does not exist, or operation cannot be performed."
        )
