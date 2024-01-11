import logging
import logging.config
from pathlib import Path

import typer
from snowcli.api.config import (
    get_logs_config,
    is_default_logs_path,
)
from snowcli.api.exceptions import InvalidLogsConfiguration

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
DEBUG_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
FILE_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_DEFAULT_LOG_FILENAME = "snowcli.log"


class LogsConfig:
    def __init__(self, debug: bool) -> None:
        config = get_logs_config()

        self.path: Path = Path(config["path"])
        self.save_logs: bool = config["save_logs"]
        self.file_log_level: int = logging.getLevelName(config["level"].upper())
        if debug:
            self.file_log_level = logging.DEBUG

        self._check_log_level(config)
        if self.save_logs:
            self._check_logs_directory_exists()

    def _check_logs_directory_exists(self):
        if not self.path.exists():
            if is_default_logs_path(self.path):
                self.path.mkdir(parents=True)
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

    @property
    def filename(self):
        return self.path / _DEFAULT_LOG_FILENAME


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

    enabled_handlers = ["console"]
    handlers_config = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "console_formatter",
            "level": console_log_level,
        },
    }
    if config.save_logs:
        enabled_handlers.append("file")
        handlers_config["file"] = {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": config.filename,
            "when": "midnight",
            "formatter": "file_formatter",
            "level": config.file_log_level,
        }

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "console_formatter": {
                    "class": "logging.Formatter",
                    "format": console_log_format,
                    "datefmt": DATE_FORMAT,
                },
                "file_formatter": {
                    "class": "logging.Formatter",
                    "format": FILE_LOG_FORMAT,
                    "datefmt": DATE_FORMAT,
                },
            },
            "filters": {},
            "handlers": handlers_config,
            "loggers": {
                "snowcli": {
                    "level": global_log_level,
                    "handlers": enabled_handlers,
                },
                "snowflake": {
                    "level": global_log_level,
                },
            },
        }
    )
