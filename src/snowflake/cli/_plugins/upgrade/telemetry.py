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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from snowflake.cli._plugins.upgrade.layout import ManagedLayout
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB, TELEMETRY_PENDING_LIMIT
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.error_handling import ignore_exceptions

log = logging.getLogger(__name__)

SPOOL_NAME = ".telemetry-spool.jsonl"
SPOOL_LIMIT = TELEMETRY_PENDING_LIMIT
TRIGGER_AUTO = "auto"
TRIGGER_MANUAL = "manual"


def spool_path(layout: Optional[ManagedLayout] = None) -> Path:
    root = layout.root if layout is not None else ManagedLayout().root
    return root / SPOOL_NAME


def read_spool_records(
    layout: Optional[ManagedLayout] = None,
) -> List[Dict[str, Any]]:
    """Parsed JSONL records without consuming the file."""
    path = spool_path(layout)
    if not path.is_file():
        return []
    records: List[Dict[str, Any]] = []
    for line in _read_lines(path):
        parsed = _parse_record(line)
        if parsed is not None:
            records.append(parsed)
    return records


def read_spool_events(
    layout: Optional[ManagedLayout] = None,
) -> List[Tuple[Dict[str, Any], int]]:
    """Valid ``(message, timestamp)`` pairs on disk. Does not consume the file."""
    out: List[Tuple[Dict[str, Any], int]] = []
    for record in read_spool_records(layout):
        message = record.get("message")
        timestamp = record.get("timestamp")
        if not isinstance(message, dict) or not isinstance(timestamp, int):
            continue
        out.append((message, timestamp))
    return out


def drop_spool_events(
    events: List[Tuple[Dict[str, Any], int]],
    *,
    layout: Optional[ManagedLayout] = None,
) -> None:
    """Remove matching events after a successful upload; keep anything newer.

    Concurrent appends between read and drop stay on disk. Last-writer-wins
    races on the rewrite itself are accepted until auto-upgrade serializes
    with ``.upgrade.lock``.
    """
    remaining_to_drop = list(events)
    kept: List[Dict[str, Any]] = []
    for record in read_spool_records(layout):
        message = record.get("message")
        timestamp = record.get("timestamp")
        if isinstance(message, dict) and isinstance(timestamp, int):
            pair: Tuple[Dict[str, Any], int] = (message, timestamp)
            try:
                remaining_to_drop.remove(pair)
                continue
            except ValueError:
                pass
        kept.append(record)
    path = spool_path(layout)
    if not kept:
        if path.is_file():
            SecurePath(path).unlink(missing_ok=True)
        return
    lines = [json.dumps(record, separators=(",", ":")) for record in kept]
    SecurePath(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    SecurePath(path).chmod(0o600)


@ignore_exceptions()
def record_upgrade_event(
    *,
    trigger: str,
    status: str,
    duration_ms: int,
    from_version: Optional[str] = None,
    to_version: Optional[str] = None,
    skip_reason: Optional[str] = None,
    rollout_bucket: Optional[int] = None,
    layout: Optional[ManagedLayout] = None,
) -> None:
    """Append one upgrade event to the managed-home spool.

    Does not open a Snowflake connection. ``CLITelemetryClient.flush`` uploads
    the spool the next time a silent or already-open connection exists.
    Never writes a machine id.
    """
    from snowflake.cli._app.telemetry import (
        CLITelemetryClient,
        CLITelemetryField,
        TelemetryEvent,
    )
    from snowflake.connector.telemetry import TelemetryField
    from snowflake.connector.time_util import get_time_millis

    payload = {
        TelemetryField.KEY_TYPE: TelemetryEvent.UPGRADE.value,
        CLITelemetryField.UPGRADE_TRIGGER: trigger,
        CLITelemetryField.UPGRADE_STATUS: status,
        CLITelemetryField.UPGRADE_DURATION_MS: int(duration_ms),
    }
    if from_version:
        payload[CLITelemetryField.UPGRADE_FROM] = from_version
    if to_version:
        payload[CLITelemetryField.UPGRADE_TO] = to_version
    if skip_reason:
        payload[CLITelemetryField.UPGRADE_SKIP_REASON] = skip_reason
    if rollout_bucket is not None:
        payload[CLITelemetryField.UPGRADE_ROLLOUT_BUCKET] = int(rollout_bucket)

    message = CLITelemetryClient.generate_telemetry_data_dict(payload)
    append_spool_record(
        {"message": message, "timestamp": get_time_millis()},
        layout=layout,
    )


def append_spool_record(
    record: Dict[str, Any],
    *,
    layout: Optional[ManagedLayout] = None,
) -> None:
    # Read-modify-write so we can cap at SPOOL_LIMIT. Two concurrent writers
    # can lose a line; auto-upgrade will hold ``.upgrade.lock`` around the
    # parent write, which is the realistic race.
    path = spool_path(layout)
    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
    lines = _read_lines(path)
    lines.append(json.dumps(record, separators=(",", ":")))
    lines = lines[-SPOOL_LIMIT:]
    SecurePath(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    SecurePath(path).chmod(0o600)


def _read_lines(path: Path) -> List[str]:
    if not path.is_file():
        return []
    text = SecurePath(path).read_text(
        file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"
    )
    return [line for line in text.splitlines() if line.strip()]


def _parse_record(line: str) -> Optional[Dict[str, Any]]:
    try:
        parsed = json.loads(line)
    except json.JSONDecodeError:
        log.debug("Ignoring malformed upgrade telemetry spool line")
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed
