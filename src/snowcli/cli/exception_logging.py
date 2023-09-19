from logging import Logger
from typing import Callable

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager


def exception_logging(logger: Logger) -> Callable[[str, Exception], None]:
    def log_error(msg: str, exception: Exception) -> None:
        enable_exc_info = (
            snow_cli_global_context_manager.get_global_context_copy().enable_tracebacks
        )
        exc_info = exception if enable_exc_info else None
        logger.error(msg=msg, exc_info=exc_info)

    return log_error
