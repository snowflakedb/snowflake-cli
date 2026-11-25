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
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List

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
                # UTF-8 on every platform: the default is the ANSI code page on
                # Windows (e.g. cp932), which cannot encode every log line.
                # Lone surrogates are escaped instead of failing the write.
                "encoding": "utf-8",
                "errors": "backslashreplace",
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
        from snowflake.cli.api.utils.path_utils import path_resolver

        # Ensure Windows short paths are resolved to prevent cleanup issues
        resolved_path = path_resolver(str(self.path.path))
        return Path(resolved_path) / _DEFAULT_LOG_FILENAME


def create_initial_loggers():
    config = InitialLoggingConfig()
    try:
        file_logs_config = FileLogsConfig(debug=False)
        if file_logs_config.save_logs:
            config.handlers["file"]["filename"] = file_logs_config.filename
            _configurate_logging(config)
    except ConfigSourceError:
        pass


def _structured_output_format() -> bool:
    """True when stdout is a JSON or CSV document."""
    from snowflake.cli.api.cli_global_context import get_cli_context
    from snowflake.cli.api.output.formats import OutputFormat

    output_format = get_cli_context().output_format
    if not isinstance(output_format, OutputFormat):
        return False
    return output_format.is_json or output_format == OutputFormat.CSV


class _DeferredConsoleHandler(logging.Handler):
    """Hold console records until the command succeeds.

    A JSON or CSV failure must be the whole stderr document. Records emitted
    while the command runs stay in memory. ``flush_deferred_console_logs``
    writes them after a successful command. ``discard_deferred_console_logs``
    drops them before the structured error is printed.
    """

    terminator = "\n"

    def __init__(self) -> None:
        super().__init__()
        self.buffer: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.buffer.append(record)

    def discard(self) -> None:
        self.buffer.clear()

    def write_buffer(self) -> None:
        pending = self.buffer
        self.buffer = []
        if not pending:
            return
        stream = sys.stderr
        for record in pending:
            try:
                stream.write(self.format(record) + self.terminator)
            except Exception:
                self.handleError(record)
        try:
            stream.flush()
        except Exception:
            self.handleError(pending[-1])


def _deferred_console_handlers() -> Iterator[_DeferredConsoleHandler]:
    seen: set[int] = set()
    loggers = [logging.getLogger()]
    for logger in logging.Logger.manager.loggerDict.values():
        if isinstance(logger, logging.Logger):
            loggers.append(logger)
    for logger in loggers:
        for handler in logger.handlers:
            if isinstance(handler, _DeferredConsoleHandler) and id(handler) not in seen:
                seen.add(id(handler))
                yield handler


def flush_deferred_console_logs() -> None:
    """Write buffered console logs. No-op when the console handler is live."""
    for handler in _deferred_console_handlers():
        handler.write_buffer()


def discard_deferred_console_logs() -> None:
    """Drop buffered console logs so a structured error stays one document."""
    for handler in _deferred_console_handlers():
        handler.discard()


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

    # A live console handler writes log lines in front of a JSON/CSV error
    # and ``json.loads`` fails. Buffer those lines instead of dropping the
    # sink: a successful command still prints them (``app run --debug``),
    # and ``print_structured_error`` discards them on failure.
    if _structured_output_format():
        config.handlers["console"][
            "class"
        ] = "snowflake.cli._app.loggers._DeferredConsoleHandler"

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
