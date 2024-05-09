from __future__ import annotations

import platform
import sys
from enum import Enum, unique
from typing import Any, Dict, Union

import click
from snowflake.cli.__about__ import VERSION
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.config import get_feature_flags_section
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.utils.error_handling import ignore_exceptions
from snowflake.cli.app.constants import PARAM_APPLICATION_NAME
from snowflake.connector.telemetry import (
    TelemetryData,
    TelemetryField,
)
from snowflake.connector.time_util import get_time_millis


@unique
class CLITelemetryField(Enum):
    # Basic information
    SOURCE = "source"
    VERSION_CLI = "version_cli"
    VERSION_PYTHON = "version_python"
    VERSION_OS = "version_os"
    # Command execution context
    COMMAND = "command"
    COMMAND_GROUP = "command_group"
    COMMAND_FLAGS = "command_flags"
    COMMAND_OUTPUT_TYPE = "command_output_type"
    # Configuration
    CONFIG_FEATURE_FLAGS = "config_feature_flags"
    # Information
    EVENT = "event"
    ERROR_MSG = "error_msg"
    ERROR_TYPE = "error_type"


class TelemetryEvent(Enum):
    CMD_EXECUTION = "executing_command"


TelemetryDict = Dict[Union[CLITelemetryField, TelemetryField], Any]


def _find_command_info() -> TelemetryDict:
    ctx = click.get_current_context()
    command_path = ctx.command_path.split(" ")[1:]
    return {
        CLITelemetryField.COMMAND: command_path,
        CLITelemetryField.COMMAND_GROUP: command_path[0],
        CLITelemetryField.COMMAND_FLAGS: {
            k: ctx.get_parameter_source(k).name  # type: ignore[attr-defined]
            for k, v in ctx.params.items()
            if v  # noqa
        },
        CLITelemetryField.COMMAND_OUTPUT_TYPE: ctx.params.get(
            "format", OutputFormat.TABLE
        ).value,
    }


def command_info() -> str:
    info = _find_command_info()
    command = ".".join(info[CLITelemetryField.COMMAND])
    return f"{PARAM_APPLICATION_NAME}.{command}".upper()


def python_version() -> str:
    py_ver = sys.version_info
    return f"{py_ver.major}.{py_ver.minor}.{py_ver.micro}"


class CLITelemetryClient:
    def __init__(self, ctx):
        self._ctx = ctx

    @staticmethod
    def generate_telemetry_data_dict(
        telemetry_payload: TelemetryDict,
    ) -> Dict[str, Any]:
        data = {
            CLITelemetryField.SOURCE: PARAM_APPLICATION_NAME,
            CLITelemetryField.VERSION_CLI: VERSION,
            CLITelemetryField.VERSION_OS: platform.platform(),
            CLITelemetryField.VERSION_PYTHON: python_version(),
            CLITelemetryField.CONFIG_FEATURE_FLAGS: {
                k: str(v) for k, v in get_feature_flags_section().items()
            },
            **_find_command_info(),
            **telemetry_payload,
        }
        # To map Enum to string, so we don't have to use .value every time
        return {getattr(k, "value", k): v for k, v in data.items()}  # type: ignore[arg-type]

    @property
    def _telemetry(self):
        return self._ctx.connection._telemetry  # noqa

    def send(self, payload: TelemetryDict):
        if self._telemetry:
            message = self.generate_telemetry_data_dict(payload)
            telemetry_data = TelemetryData.from_telemetry_data_dict(
                from_dict=message, timestamp=get_time_millis()
            )
            self._telemetry.try_add_log_to_batch(telemetry_data)

    def flush(self):
        self._telemetry.send_batch()


_telemetry = CLITelemetryClient(ctx=cli_context)


@ignore_exceptions()
def log_command_usage():
    _telemetry.send({TelemetryField.KEY_TYPE: TelemetryEvent.CMD_EXECUTION.value})


@ignore_exceptions()
def flush_telemetry():
    _telemetry.flush()
