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
from typing import Iterator

import websocket
from google.protobuf.message import DecodeError
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.streamlit.proto_codec import (
    decode_log_entry,
    encode_stream_logs_request,
)
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    SPCS_RUNTIME_V2_NAME,
)
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import (
    CliArgumentError,
    CliConnectionError,
    CliError,
    CliSqlError,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import CommandResult, MessageResult, ObjectResult
from snowflake.cli.api.secret import SecretType
from snowflake.connector import SnowflakeConnection

log = logging.getLogger(__name__)

# Timeout for each ws.recv_data() call — mirrors the Go client's 90-second
# read deadline.  When no log entry arrives within this window, we re-issue
# recv_data() so the loop stays responsive to KeyboardInterrupt.
_WS_RECV_TIMEOUT_SECONDS = 90

_HANDSHAKE_TIMEOUT_SECONDS = 10


@dataclass
class DeveloperApiToken:
    token: SecretType
    resource_uri: str


def get_developer_api_token(conn: SnowflakeConnection, fqn: str) -> DeveloperApiToken:
    """
    Calls SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN and returns a
    DeveloperApiToken with the token and resource URI.
    """
    if "'" in fqn:
        raise CliArgumentError(
            f"Invalid Streamlit app name: {fqn}. Name must not contain single quotes."
        )

    query = f"CALL SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN('{fqn}', false);"
    log.debug("Fetching developer API token for %s", fqn)

    cursor = conn.cursor()
    try:
        cursor.execute(query)
        row = cursor.fetchone()
        if not row:
            raise CliSqlError(
                "Empty response from SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN"
            )
        raw = row[0]
    finally:
        cursor.close()

    try:
        resp = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as e:
        raise CliSqlError(f"Failed to parse token response: {e}") from e

    token = resp.get("token", "")
    resource_uri = resp.get("resourceUri", "")

    if not token:
        raise CliSqlError("Empty token in developer API response")
    if not resource_uri:
        raise CliSqlError("Empty resourceUri in developer API response")

    log.debug("Resource URI: %s", resource_uri)
    return DeveloperApiToken(token=SecretType(token), resource_uri=resource_uri)


def build_ws_url(resource_uri: str) -> str:
    """Convert resource URI to WebSocket URL and append /logs path."""
    ws_url = resource_uri.replace("https://", "wss://", 1).replace(
        "http://", "ws://", 1
    )
    return ws_url.rstrip("/") + "/logs"


def validate_spcs_v2_runtime(conn: SnowflakeConnection, fqn: FQN) -> None:
    """
    Run DESCRIBE STREAMLIT and verify the app uses SPCSv2 runtime.

    Raises CliError if the app does not use the SPCS Runtime V2
    (required for log streaming).
    """
    # ObjectManager.describe wraps DESCRIBE STREAMLIT using identifier syntax
    # (not string literals) so there is no single-quote injection risk.
    cursor = ObjectManager(connection=conn).describe(
        object_type=ObjectType.STREAMLIT.value.cli_name,
        fqn=fqn,
    )
    try:
        row = cursor.fetchone()
        description = cursor.description
    finally:
        cursor.close()

    if not row or not description:
        raise CliSqlError(
            f"Could not describe Streamlit app {fqn}. "
            "Verify the app exists and you have access."
        )

    columns = {desc[0].lower(): val for desc, val in zip(description, row)}
    runtime_name = columns.get("runtime_name")

    if runtime_name != SPCS_RUNTIME_V2_NAME:
        raise CliError(
            f"Log streaming is only supported for Streamlit apps running on "
            f"SPCSv2 runtime ({SPCS_RUNTIME_V2_NAME}). "
            f"App '{fqn}' has runtime_name='{runtime_name}'."
        )


def _result_for_entry(entry, output_format: OutputFormat) -> CommandResult:
    """
    Pick the CommandResult subclass best matched to the current output format.

    - JSON / CSV: yield a dict via ObjectResult so the framework produces
      valid JSONL / CSV rows that downstream tools (e.g. ``jq``) can parse.
    - TABLE / plain: yield a pre-formatted line via MessageResult so each
      log entry renders as a single human-readable row instead of a
      two-column key/value table.
    """
    if output_format == OutputFormat.CSV or output_format.is_json:
        return ObjectResult(entry.to_dict())
    return MessageResult(entry.format_line())


def stream_logs(
    conn: SnowflakeConnection,
    fqn: str,
    tail_lines: int = 100,
) -> Iterator[CommandResult]:
    """
    Connect to the Streamlit developer log streaming WebSocket and yield one
    ``CommandResult`` per log entry until the server closes the connection or
    the user interrupts (Ctrl+C).

    The concrete result type is chosen from the current CLI output format so
    that the caller can wrap this generator in a ``StreamResult`` and let the
    standard CLI output framework render each entry as JSON, CSV, or plain
    text appropriately.

    Status messages (token fetch, connect, stop) are emitted through
    ``cli_console`` so they can be suppressed with ``--silent`` when the user
    wants clean machine-readable output on stdout.
    """
    output_format = get_cli_context().output_format or OutputFormat.TABLE

    cli_console.step("Fetching developer API token...")
    token_info = get_developer_api_token(conn, fqn)

    ws_url = build_ws_url(token_info.resource_uri)
    cli_console.step(f"Connecting to log stream: {ws_url}")

    # NOTE: Do not log `header` — it contains the auth token. Also be aware
    # that websocket.enableTrace(True) will dump headers to stderr.
    header = [f'Authorization: Snowflake Token="{token_info.token.value}"']
    ws = websocket.WebSocket()
    ws.timeout = _WS_RECV_TIMEOUT_SECONDS
    streaming = False

    try:
        try:
            ws.connect(ws_url, header=header, timeout=_HANDSHAKE_TIMEOUT_SECONDS)
        except Exception as e:
            raise CliConnectionError(f"Failed to connect to log stream: {e}") from e

        ws.send_binary(encode_stream_logs_request(tail_lines))
        log.debug("Sent StreamLogsRequest with tail_lines=%d", tail_lines)

        cli_console.step(f"Streaming logs (tail={tail_lines}). Press Ctrl+C to stop.")
        streaming = True

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
            except (websocket.WebSocketException, OSError) as e:
                log.debug("WebSocket recv error: %s", e)
                break

            if opcode == websocket.ABNF.OPCODE_BINARY:
                try:
                    entry = decode_log_entry(data)
                except (DecodeError, ValueError) as e:
                    log.warning("Failed to decode log entry: %s", e)
                    continue
                yield _result_for_entry(entry, output_format)
            elif opcode == websocket.ABNF.OPCODE_CLOSE:
                break
            elif opcode == websocket.ABNF.OPCODE_PING:
                ws.pong(data)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            ws.close(status=websocket.STATUS_NORMAL)
        except Exception as e:
            log.debug("Error closing WebSocket: %s", e)
        if streaming:
            cli_console.step("Log streaming stopped.")
