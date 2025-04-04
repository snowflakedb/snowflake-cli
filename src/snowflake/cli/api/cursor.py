from snowflake.cli.api.exceptions import CliSqlError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


class ExtendedExitCodes:
    def execute(self, command, *args, **kwargs):
        try:
            super().execute(command, *args, **kwargs)
        except ProgrammingError as pex:
            raise CliSqlError(pex.msg) from pex


class CliSnowflakeCursor(SnowflakeCursor, ExtendedExitCodes):
    """Cli Cursor with override of execution for custom exception handling."""


class CliDictCursor(DictCursor, ExtendedExitCodes):
    """Cli Dict Cursor with override of execution for custom exception handling."""
