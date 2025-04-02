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

from __future__ import annotations

import logging
import logging.config
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List

import typer
from snowflake.cli.api.exceptions import InvalidLogsConfigurationError
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.errors import ConfigSourceError

_DEFAULT_LOG_FILENAME = "snowflake-cli.log"


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
            "snowflake.connector.telemetry": LoggerConfig(level=logging.CRITICAL),
        }
    )


@dataclass
class InitialLoggingConfig(DefaultLoggingConfig):
    loggers: Dict[str, Any] = field(
        default_factory=lambda: {
            "snowflake.cli": LoggerConfig(level=logging.INFO, handlers=["file"]),
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
        from snowflake.cli.api.config import (
            get_logs_config,
        )

        config = get_logs_config()

        self.path: SecurePath = SecurePath(config["path"])
        self.save_logs: bool = config["save_logs"]
        self.level: int = logging.getLevelName(config["level"].upper())
        if debug:
            self.level = logging.DEBUG

        self._check_log_level(config)
        if self.save_logs:
            self._create_logs_directory_if_not_exists()

    def _create_logs_directory_if_not_exists(self):
        if not self.path.exists():
            self.path.mkdir(parents=True)

    def _check_log_level(self, config):
        possible_log_levels = [
            logging.DEBUG,
            logging.INFO,
            logging.WARN,
            logging.ERROR,
            logging.CRITICAL,
        ]
        if self.level not in possible_log_levels:
            raise InvalidLogsConfigurationError(
                f"Invalid 'level' value set in [logs] section: {config['level']}. "
                f"'level' should be one of: {' / '.join(logging.getLevelName(lvl) for lvl in possible_log_levels)}"
            )

    @property
    def filename(self):
        return self.path.path / _DEFAULT_LOG_FILENAME


def create_initial_loggers():
    config = InitialLoggingConfig()
    try:
        file_logs_config = FileLogsConfig(debug=False)
        if file_logs_config.save_logs:
            config.handlers["file"]["filename"] = file_logs_config.filename
            _configurate_logging(config)
    except ConfigSourceError:
        pass


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
        # In debug mode we also want to get snowflake connector logs
        config.loggers["snowflake"].handlers = ["file", "console"]
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
        for logger in config.loggers.values():
            if "file" in logger.handlers:
                logger.handlers.remove("file")

    config.loggers["snowflake.cli"].level = global_log_level
    config.loggers["snowflake"].level = global_log_level

    _configurate_logging(config)


def _configurate_logging(config: DefaultLoggingConfig | InitialLoggingConfig) -> None:
    dict_config = asdict(config)
    _remove_underscore_prefixes_from_keys(dict_config)
    logging.config.dictConfig(dict_config)
