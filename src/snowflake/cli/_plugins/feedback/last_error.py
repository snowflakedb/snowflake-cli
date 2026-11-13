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

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import click
from snowflake.cli.api.commands.execution_metadata import ExecutionMetadata
from snowflake.cli.api.config import get_config_manager
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

_LAST_ERROR_FILE_NAME = "last_cli_error.json"


def last_error_path() -> SecurePath:
    return SecurePath(get_config_manager().file_path.parent / _LAST_ERROR_FILE_NAME)


def record_last_cli_error(
    exception: BaseException, execution: ExecutionMetadata
) -> None:
    """Write id, type, command name, and timestamp. Omit the error text.

    ``snow feedback`` is skipped so a failed submit does not replace the error
    the user is about to attach. Process exits and Ctrl-C are not CLI errors.
    """
    if not isinstance(exception, Exception):
        return
    if isinstance(exception, (click.exceptions.Exit, click.exceptions.Abort)):
        return
    command = current_command_name()
    if command == "feedback" or command.startswith("feedback "):
        return

    payload = {
        "id": execution.execution_id,
        "error_type": type(exception).__name__,
        "command": command,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    path = last_error_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        log.debug("Failed to persist last CLI error", exc_info=True)


def read_last_cli_error() -> Optional[dict]:
    """Return id and error_type from the sidecar, or None.

    A ``message`` key in the file is ignored. Callers must not send the
    command name or timestamp.
    """
    path = last_error_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(
            path.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8")
        )
    except Exception:
        log.debug("Ignoring unreadable last CLI error file", exc_info=True)
        return None
    if not isinstance(payload, dict):
        return None
    error_id = payload.get("id")
    error_type = payload.get("error_type")
    if not isinstance(error_id, str) or not error_id.strip():
        return None
    if not isinstance(error_type, str) or not error_type.strip():
        return None
    return {"id": error_id, "error_type": error_type}


def clear_last_cli_error() -> None:
    """Remove the sidecar after a successful attach.

    A failed send leaves the file so a retry can attach the same id.
    """
    try:
        last_error_path().unlink(missing_ok=True)
    except OSError:
        log.debug("Failed to remove last CLI error file", exc_info=True)


def current_command_name() -> str:
    try:
        ctx = click.get_current_context()
    except RuntimeError:
        return ""
    parts = ctx.command_path.split(" ")[1:]
    return " ".join(parts)
