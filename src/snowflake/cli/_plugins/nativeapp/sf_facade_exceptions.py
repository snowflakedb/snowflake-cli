from click import ClickException
from snowflake.cli.api.constants import ObjectType
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
    def __init__(self, object_type: ObjectType, name: str):
        super().__init__(
            f"Could not use {object_type} {name}. Object does not exist, or operation cannot be performed."
        )
