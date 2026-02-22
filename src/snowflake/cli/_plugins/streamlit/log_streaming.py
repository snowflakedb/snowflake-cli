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
import sys
from datetime import datetime, timezone
from typing import Tuple

from click import ClickException
from snowflake.cli._plugins.streamlit.proto.generated.developer.v1 import (
    logs_service_pb2 as pb,
)
from snowflake.cli.api.console import cli_console
from snowflake.connector import SnowflakeConnection

log = logging.getLogger(__name__)

# LogSource enum labels
_LOG_SOURCE_LABELS = {
    pb.LOG_SOURCE_UNSPECIFIED: "UNKNOWN",
    pb.LOG_SOURCE_APP: "APP",
    pb.LOG_SOURCE_MANAGER: "MGR",
}

# LogLevel enum labels
_LOG_LEVEL_LABELS = {
    pb.LOG_LEVEL_UNSPECIFIED: "UNKNOWN",
    pb.LOG_LEVEL_DEBUG: "DEBUG",
    pb.LOG_LEVEL_INFO: "INFO",
    pb.LOG_LEVEL_WARN: "WARN",
    pb.LOG_LEVEL_ERROR: "ERROR",
}

DEFAULT_TAIL_LINES = 100
MAX_TAIL_LINES = 10000

# Timeout for each ws.recv() call — mirrors the Go client's 90-second read
# deadline.  When no log entry arrives within this window, we re-issue recv()
# so the loop stays responsive to KeyboardInterrupt.
_WS_RECV_TIMEOUT_SECONDS = 90


def get_developer_api_token(conn: SnowflakeConnection, fqn: str) -> Tuple[str, str]:
    """
    Calls SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN and returns (token, resource_uri).
    """
    if "'" in fqn:
        raise ClickException(
            f"Invalid Streamlit app name: {fqn}. Name must not contain single quotes."
        )

    query = f"CALL SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN('{fqn}', false);"
    log.debug("Fetching developer API token for %s", fqn)

    cursor = conn.cursor()
    try:
        cursor.execute(query)
        row = cursor.fetchone()
        if not row:
            raise ClickException(
                "Empty response from SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN"
            )
        raw = row[0]
    finally:
        cursor.close()

    try:
        resp = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as e:
        raise ClickException(f"Failed to parse token response: {e}") from e

    token = resp.get("token", "")
    resource_uri = resp.get("resourceUri", "")

    if not token:
        raise ClickException("Empty token in developer API response")
    if not resource_uri:
        raise ClickException("Empty resourceUri in developer API response")

    log.debug("Resource URI: %s", resource_uri)
    return token, resource_uri


def build_ws_url(resource_uri: str) -> str:
    """Convert resource URI to WebSocket URL and append /logs path."""
    ws_url = resource_uri.replace("https://", "wss://", 1).replace(
        "http://", "ws://", 1
    )
    return ws_url.rstrip("/") + "/logs"


def _parse_timestamp(entry: pb.LogEntry) -> datetime:
    """Extract a timezone-aware UTC datetime from a LogEntry."""
    if entry.HasField("timestamp"):
        return entry.timestamp.ToDatetime(tzinfo=timezone.utc)
    return datetime.fromtimestamp(0, tz=timezone.utc)


def format_log_entry(entry: pb.LogEntry) -> str:
    """Format a LogEntry protobuf message into a human-readable line."""
    ts = _parse_timestamp(entry)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{ts.microsecond // 1000:03d}"

    source = _LOG_SOURCE_LABELS.get(entry.log_source, "UNKNOWN")
    level = _LOG_LEVEL_LABELS.get(entry.level, "UNKNOWN")
    return f"[{ts_str}] [{level}] [{source}] [seq:{entry.sequence}] {entry.content}"


def log_entry_to_dict(entry: pb.LogEntry) -> dict:
    """Convert a LogEntry protobuf message into a JSON-serializable dict."""
    ts = _parse_timestamp(entry)
    return {
        "timestamp": ts.isoformat(),
        "level": _LOG_LEVEL_LABELS.get(entry.level, "UNKNOWN"),
        "source": _LOG_SOURCE_LABELS.get(entry.log_source, "UNKNOWN"),
        "sequence": entry.sequence,
        "content": entry.content,
    }


_HANDSHAKE_TIMEOUT_SECONDS = 10


def stream_logs(
    conn: SnowflakeConnection,
    fqn: str,
    tail_lines: int = DEFAULT_TAIL_LINES,
    json_output: bool = False,
) -> None:
    """
    Connect to the Streamlit developer log streaming WebSocket and print
    log entries to stdout until interrupted.

    When *json_output* is True each log entry is emitted as a single-line
    JSON object (JSONL), suitable for piping to ``jq`` or other tools.
    """
    import websocket

    # 1. Get token
    cli_console.step("Fetching developer API token...")
    token, resource_uri = get_developer_api_token(conn, fqn)

    # 2. Build WebSocket URL
    ws_url = build_ws_url(resource_uri)
    cli_console.step(f"Connecting to log stream: {ws_url}")

    # 3. Connect
    header = [f'Authorization: Snowflake Token="{token}"']
    ws = websocket.WebSocket()
    ws.timeout = _WS_RECV_TIMEOUT_SECONDS

    try:
        ws.connect(ws_url, header=header, timeout=_HANDSHAKE_TIMEOUT_SECONDS)
    except Exception as e:
        raise ClickException(f"Failed to connect to log stream: {e}") from e

    try:
        # 4. Send StreamLogsRequest
        request = pb.StreamLogsRequest(tail_lines=tail_lines)
        ws.send_binary(request.SerializeToString())
        log.debug("Sent StreamLogsRequest with tail_lines=%d", tail_lines)

        cli_console.step(f"Streaming logs (tail={tail_lines}). Press Ctrl+C to stop.")
        sys.stdout.write("---\n")
        sys.stdout.flush()

        # 5. Read loop
        while True:
            try:
                opcode, data = ws.recv_data()
            except websocket.WebSocketTimeoutException:
                # No message within the timeout window — loop back so we
                # stay responsive to KeyboardInterrupt.
                continue
            except websocket.WebSocketConnectionClosedException:
                log.debug("WebSocket connection closed by server")
                break
            except Exception as e:
                log.debug("WebSocket recv error: %s", e)
                break

            if opcode == websocket.ABNF.OPCODE_BINARY:
                entry = pb.LogEntry()
                entry.ParseFromString(data)
                if json_output:
                    sys.stdout.write(json.dumps(log_entry_to_dict(entry)) + "\n")
                else:
                    sys.stdout.write(format_log_entry(entry) + "\n")
                sys.stdout.flush()
            elif opcode == websocket.ABNF.OPCODE_CLOSE:
                break
            elif opcode == websocket.ABNF.OPCODE_PING:
                ws.pong(data)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            ws.close()
        except Exception:
            pass
        sys.stdout.write("\n--- Log streaming stopped.\n")
        sys.stdout.flush()
