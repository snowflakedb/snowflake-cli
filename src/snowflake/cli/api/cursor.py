from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.exceptions import CliSqlError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector.errors import ProgrammingError


# todo: remove usage of custom cursor & handle resolution in SnowTyper.exception_handler
class ExtendedExitCodes:
    def execute(self, command, *args, **kwargs):
        try:
            super().execute(command, *args, **kwargs)
        except ProgrammingError as pex:
            raise pex
            if get_cli_context().enhanced_exit_codes:
                raise CliSqlError(pex.msg) from pex
            raise pex


class CliSnowflakeCursor(ExtendedExitCodes, SnowflakeCursor):
    """Cli Cursor with override of execution for custom exception handling."""


class CliDictCursor(ExtendedExitCodes, DictCursor):
    """Cli Dict Cursor with override of execution for custom exception handling."""
