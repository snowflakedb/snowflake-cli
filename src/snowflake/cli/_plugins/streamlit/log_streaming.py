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

"""
WebSocket log streaming client for Streamlit developer logs.

Connects to the Streamlit container runtime's developer log service
via WebSocket and streams log entries in real time.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass

import websocket
from click import ClickException
from google.protobuf.message import DecodeError
from snowflake.cli._plugins.streamlit.proto_codec import (
    decode_log_entry,
    encode_stream_logs_request,
)
from snowflake.cli.api.console import cli_console
from snowflake.connector import SnowflakeConnection

log = logging.getLogger(__name__)

DEFAULT_TAIL_LINES = 100
MAX_TAIL_LINES = 1000

# Timeout for each ws.recv_data() call — mirrors the Go client's 90-second
# read deadline.  When no log entry arrives within this window, we re-issue
# recv_data() so the loop stays responsive to KeyboardInterrupt.
_WS_RECV_TIMEOUT_SECONDS = 90

_HANDSHAKE_TIMEOUT_SECONDS = 10


@dataclass
class DeveloperApiToken:
    token: str
    resource_uri: str


def get_developer_api_token(conn: SnowflakeConnection, fqn: str) -> DeveloperApiToken:
    """
    Calls SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN and returns a
    DeveloperApiToken with the token and resource URI.
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
    return DeveloperApiToken(token=token, resource_uri=resource_uri)


def build_ws_url(resource_uri: str) -> str:
    """Convert resource URI to WebSocket URL and append /logs path."""
    ws_url = resource_uri.replace("https://", "wss://", 1).replace(
        "http://", "ws://", 1
    )
    return ws_url.rstrip("/") + "/logs"


def validate_spcs_v2_runtime(conn: SnowflakeConnection, fqn: str) -> None:
    """
    Run DESCRIBE STREAMLIT and verify the app uses SPCSv2 runtime.

    Raises ClickException if the app does not use the SPCS Runtime V2
    (required for log streaming).
    """
    from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
        SPCS_RUNTIME_V2_NAME,
    )

    cursor = conn.cursor()
    try:
        cursor.execute(f"DESCRIBE STREAMLIT {fqn}")
        row = cursor.fetchone()
        description = cursor.description
    finally:
        cursor.close()

    if not row or not description:
        raise ClickException(
            f"Could not describe Streamlit app {fqn}. "
            "Verify the app exists and you have access."
        )

    # Build column-name -> value mapping from cursor.description
    columns = {desc[0].lower(): val for desc, val in zip(description, row)}
    runtime_name = columns.get("runtime_name")

    if runtime_name != SPCS_RUNTIME_V2_NAME:
        raise ClickException(
            f"Log streaming is only supported for Streamlit apps running on "
            f"SPCSv2 runtime ({SPCS_RUNTIME_V2_NAME}). "
            f"App '{fqn}' has runtime_name='{runtime_name}'."
        )


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
    # 1. Get token
    cli_console.step("Fetching developer API token...")
    token_info = get_developer_api_token(conn, fqn)

    # 2. Build WebSocket URL
    ws_url = build_ws_url(token_info.resource_uri)
    cli_console.step(f"Connecting to log stream: {ws_url}")

    # 3. Connect
    header = [f'Authorization: Snowflake Token="{token_info.token}"']
    ws = websocket.WebSocket()
    ws.timeout = _WS_RECV_TIMEOUT_SECONDS
    streaming = False

    try:
        try:
            ws.connect(ws_url, header=header, timeout=_HANDSHAKE_TIMEOUT_SECONDS)
        except Exception as e:
            raise ClickException(f"Failed to connect to log stream: {e}") from e

        # 4. Send StreamLogsRequest
        ws.send_binary(encode_stream_logs_request(tail_lines))
        log.debug("Sent StreamLogsRequest with tail_lines=%d", tail_lines)

        cli_console.step(f"Streaming logs (tail={tail_lines}). Press Ctrl+C to stop.")
        sys.stdout.write("---\n")
        sys.stdout.flush()
        streaming = True

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
                try:
                    entry = decode_log_entry(data)
                except (DecodeError, ValueError) as e:
                    log.warning("Failed to decode log entry: %s", e)
                    continue
                if json_output:
                    sys.stdout.write(json.dumps(entry.to_dict()) + "\n")
                else:
                    sys.stdout.write(entry.format_line() + "\n")
                sys.stdout.flush()
            elif opcode == websocket.ABNF.OPCODE_CLOSE:
                break
            elif opcode == websocket.ABNF.OPCODE_PING:
                ws.pong(data)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            ws.close(status=websocket.STATUS_NORMAL)
        except Exception:
            pass
        if streaming:
            sys.stdout.write("\n--- Log streaming stopped.\n")
            sys.stdout.flush()
