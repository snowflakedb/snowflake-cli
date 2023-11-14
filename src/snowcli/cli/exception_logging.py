from logging import Logger
from typing import Callable

from snowcli.cli.common.snow_cli_global_context import global_context


def exception_logging(logger: Logger) -> Callable[[str, Exception], None]:
    def log_error(msg: str, exception: Exception) -> None:
        exc_info = exception if global_context.enable_tracebacks else None
        logger.error(msg=msg, exc_info=exc_info)

    return log_error
