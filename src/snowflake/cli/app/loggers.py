import logging
import logging.config
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import typer
from snowflake.cli.api.config import (
    get_logs_config,
    is_default_logs_path,
)
from snowflake.cli.api.exceptions import InvalidLogsConfiguration

_DEFAULT_LOG_FILENAME = "snowcli.log"


@dataclass
class LogFormatterConfig:
    _format: str
    _class: str = "logging.Formatter"
    datefmt: str = "%Y-%m-%d %H:%M:%S"


@dataclass
class LoggerConfig:
    level: int = logging.NOTSET
    handlers: List[str] = field(default_factory=list)


@dataclass
class DefaultLoggingConfig:
    version: int = 1
    disable_existing_loggers: bool = True
    formatters: Dict[str, LogFormatterConfig] = field(
        default_factory=lambda: {
            "default_formatter": LogFormatterConfig(
                _format="%(asctime)s %(levelname)s %(message)s"
            ),
            "detailed_formatter": LogFormatterConfig(
                _format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
            ),
        }
    )
    filters: Dict[str, Any] = field(default_factory=dict)
    handlers: Dict[str, Any] = field(
        default_factory=lambda: {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default_formatter",
                "level": logging.ERROR,
            },
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": None,
                "when": "midnight",
                "formatter": "detailed_formatter",
                "level": logging.INFO,
            },
        },
    )
    loggers: Dict[str, Any] = field(
        default_factory=lambda: {
            "snowflake.cli": LoggerConfig(handlers=["console", "file"]),
            "snowflake": LoggerConfig(),
        }
    )


def _remove_underscore_prefixes_from_keys(d: Dict[str, Any]) -> None:
    for k, v in list(d.items()):
        if k.startswith("_"):
            d[k[1:]] = d.pop(k)
        if isinstance(v, dict):
            _remove_underscore_prefixes_from_keys(v)


class FileLogsConfig:
    def __init__(self, debug: bool) -> None:
        config = get_logs_config()

        self.path: Path = Path(config["path"])
        self.save_logs: bool = config["save_logs"]
        self.level: int = logging.getLevelName(config["level"].upper())
        if debug:
            self.level = logging.DEBUG

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
        if self.level not in possible_log_levels:
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
    config = DefaultLoggingConfig()

    if verbose and debug:
        raise typer.BadParameter("Only one parameter `verbose` or `debug` is possible")
    elif debug:
        config.handlers["console"].update(
            level=logging.DEBUG,
            formatter="detailed_formatter",
        )
    elif verbose:
        config.handlers["console"].update(level=logging.INFO)

    global_log_level = config.handlers["console"]["level"]

    file_logs_config = FileLogsConfig(debug=debug)
    if file_logs_config.save_logs:
        config.handlers["file"].update(
            level=file_logs_config.level,
            filename=file_logs_config.filename,
        )
        if file_logs_config.level < global_log_level:
            global_log_level = file_logs_config.level
    else:
        # We need to remove handler definition - otherwise it creates file even if `save_logs` is False
        del config.handlers["file"]
        config.loggers["snowflake.cli"].handlers.remove("file")

    config.loggers["snowflake.cli"].level = global_log_level
    config.loggers["snowflake"].level = global_log_level

    dict_config = asdict(config)
    _remove_underscore_prefixes_from_keys(dict_config)
    logging.config.dictConfig(dict_config)
