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
from dataclasses import dataclass
from typing import Generator

import websocket
from click import ClickException
from google.protobuf.message import DecodeError

from snowflake.cli._plugins.streamlit.proto_codec import (
    LogEntry,
    decode_log_entry,
    encode_stream_logs_request,
)
from snowflake.connector import SnowflakeConnection

log = logging.getLogger(__name__)

HANDSHAKE_TIMEOUT_SECONDS = 10
READ_TIMEOUT_SECONDS = 90
MAX_TAIL_LINES = 10000
DEFAULT_TAIL_LINES = 100


@dataclass
class DeveloperApiToken:
    token: str
    resource_uri: str


def get_developer_api_token(
    connection: SnowflakeConnection, fqn: str
) -> DeveloperApiToken:
    """Fetch a developer API token for the given Streamlit app.

    Calls the SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN system function
    via the existing Snowflake connection and parses the JSON response.
    """
    if "'" in fqn:
        raise ClickException(
            f"Invalid Streamlit app name: {fqn}. Name must not contain single quotes."
        )

    query = f"call SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN('{fqn}', false)"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        row = cursor.fetchone()
        if not row or not row[0]:
            raise ClickException(
                f"Empty response from SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN for {fqn}."
            )
        response = json.loads(row[0])
    finally:
        cursor.close()

    token = response.get("token", "")
    resource_uri = response.get("resourceUri", "")

    if not token:
        raise ClickException(
            f"Empty token returned for {fqn}. "
            "Ensure the app is deployed and developer mode is enabled."
        )
    if not resource_uri:
        raise ClickException(
            f"Empty resourceUri returned for {fqn}. "
            "Ensure the app is deployed and developer mode is enabled."
        )

    return DeveloperApiToken(token=token, resource_uri=resource_uri)


def build_websocket_url(resource_uri: str) -> str:
    """Convert an HTTPS resource URI to a WSS WebSocket URL for the /logs endpoint."""
    ws_url = resource_uri.replace("https://", "wss://", 1).replace(
        "http://", "ws://", 1
    )
    ws_url = ws_url.rstrip("/")
    return ws_url + "/logs"


def stream_logs(
    ws_url: str, token: str, tail_lines: int
) -> Generator[LogEntry, None, None]:
    """Connect to the log streaming WebSocket and yield LogEntry objects.

    Opens a WebSocket connection with the Snowflake developer API token,
    sends a StreamLogsRequest, and yields decoded LogEntry messages until
    the connection closes or an interrupt is received.
    """
    ws = websocket.WebSocket()
    ws.timeout = READ_TIMEOUT_SECONDS

    header = [f'Authorization: Snowflake Token="{token}"']

    try:
        ws.connect(ws_url, header=header, timeout=HANDSHAKE_TIMEOUT_SECONDS)

        request_data = encode_stream_logs_request(tail_lines)
        ws.send_binary(request_data)

        while True:
            opcode, data = ws.recv_data()
            if opcode == websocket.ABNF.OPCODE_BINARY:
                try:
                    entry = decode_log_entry(data)
                    yield entry
                except (DecodeError, ValueError) as e:
                    log.warning("Failed to decode log entry: %s", e)
            elif opcode == websocket.ABNF.OPCODE_CLOSE:
                break
            elif opcode == websocket.ABNF.OPCODE_PING:
                ws.pong(data)
    except KeyboardInterrupt:
        return
    except websocket.WebSocketConnectionClosedException:
        return
    except websocket.WebSocketTimeoutException:
        log.warning("WebSocket read timed out after %ds", READ_TIMEOUT_SECONDS)
        return
    finally:
        try:
            ws.close(status=websocket.STATUS_NORMAL)
        except Exception:
            pass
