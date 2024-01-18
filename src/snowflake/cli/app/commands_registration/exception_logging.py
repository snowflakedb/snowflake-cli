from logging import Logger
from typing import Callable

from snowflake.cli.api.cli_global_context import cli_context


def exception_logging(logger: Logger) -> Callable[[str, Exception], None]:
    def log_error(msg: str, exception: Exception) -> None:
        exc_info = exception if cli_context.enable_tracebacks else None
        logger.error(msg=msg, exc_info=exc_info)

    return log_error
