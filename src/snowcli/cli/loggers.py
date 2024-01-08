import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import typer
from snowcli.config import cli_config
from snowcli.exception import InvalidLogsConfiguration

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
DEBUG_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
FILE_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_CONSOLE_OUTPUT = "console output"


class LogsConfig:
    def __init__(self, debug: bool) -> None:
        config = cli_config.get_logs_config()

        self.path: Path = Path(config["path"])
        self.save_logs: bool = config["save_logs"]
        self.file_log_level: int = logging.getLevelName(config["level"].upper())
        if debug:
            self.file_log_level = logging.DEBUG

        self._check_log_level(config)
        self._check_logs_directory_exists()

    def _check_logs_directory_exists(self):
        if not self.path.exists():
            if cli_config.is_default_logs_path(self.path):
                self.path.mkdir()
            else:
                raise InvalidLogsConfiguration(
                    f"Directory '{self.path}' does not exist"
                )

    def _check_log_level(self, config):
        possible_log_levels = [
            logging.DEBUG,
            logging.INFO,
            logging.WARN,
            logging.ERROR,
            logging.CRITICAL,
        ]
        if self.file_log_level not in possible_log_levels:
            raise InvalidLogsConfiguration(
                f"Invalid 'level' value set in [logs] section: {config['level']}. "
                f"'level' should be one of: {' / '.join(logging.getLevelName(lvl) for lvl in possible_log_levels)}"
            )


def remove_console_output_handler_from_logs() -> None:
    logger = logging.getLogger("snowcli")
    for handler in logger.handlers:
        if handler.name == _CONSOLE_OUTPUT:
            logger.removeHandler(handler)


def add_console_output_handler_to_logs(
    log_level: int, formatter: logging.Formatter
) -> None:
    logger = logging.getLogger("snowcli")
    console = logging.StreamHandler()
    console.set_name(_CONSOLE_OUTPUT)
    console.setFormatter(formatter)
    console.setLevel(log_level)
    logger.addHandler(console)


def create_loggers(verbose: bool, debug: bool):
    """Creates a logger depending on the SnowCLI parameters and config file.
    verbose == True - print info and higher logs in default format
    debug == True - print debug and higher logs in debug format
    none of above - print only error logs in default format
    """
    config = LogsConfig(debug=debug)

    if verbose and debug:
        raise typer.BadParameter("Only one parameter `verbose` or `debug` is possible")
    elif debug:
        console_log_format = DEBUG_LOG_FORMAT
        console_log_level = logging.DEBUG
    elif verbose:
        console_log_format = DEFAULT_LOG_FORMAT
        console_log_level = logging.INFO
    else:
        console_log_format = DEFAULT_LOG_FORMAT
        console_log_level = logging.ERROR

    if console_log_level < config.file_log_level:
        global_log_level = console_log_level
    else:
        global_log_level = config.file_log_level

    logger = logging.getLogger("snowcli")
    logger.setLevel(global_log_level)
    logging.getLogger("snowflake").setLevel(global_log_level)

    add_console_output_handler_to_logs(
        console_log_level, formatter=logging.Formatter(console_log_format, DATE_FORMAT)
    )

    if config.save_logs:
        filename = config.path / "snowcli.log"
        file = TimedRotatingFileHandler(filename=filename, when="midnight")
        file.setFormatter(logging.Formatter(FILE_LOG_FORMAT, DATE_FORMAT))
        file.setLevel(config.file_log_level)
        logger.addHandler(file)
