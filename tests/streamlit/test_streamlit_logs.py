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

from datetime import datetime, timezone
from unittest import mock

import pytest
from click import ClickException

from snowflake.cli._plugins.streamlit.log_streaming import (
    DeveloperApiToken,
    build_websocket_url,
    get_developer_api_token,
    stream_logs,
)
from snowflake.cli._plugins.streamlit.proto_codec import (
    LOG_SOURCE_APP,
    LOG_SOURCE_MANAGER,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARN,
    LogEntry,
    encode_stream_logs_request,
    decode_log_entry,
)


class TestBuildWebSocketUrl:
    def test_https_to_wss(self):
        url = build_websocket_url("https://my-app.snowflakecomputing.com/api/v1")
        assert url == "wss://my-app.snowflakecomputing.com/api/v1/logs"

    def test_http_to_ws(self):
        url = build_websocket_url("http://localhost:8702")
        assert url == "ws://localhost:8702/logs"

    def test_preserves_path(self):
        url = build_websocket_url("https://host.example.com/some/deep/path")
        assert url == "wss://host.example.com/some/deep/path/logs"

    def test_strips_trailing_slash(self):
        url = build_websocket_url("https://host.example.com/api/v1/")
        assert url == "wss://host.example.com/api/v1/logs"

    def test_replaces_only_first_occurrence(self):
        url = build_websocket_url("https://proxy.example.com/redirect/https://target.com")
        assert url == "wss://proxy.example.com/redirect/https://target.com/logs"


class TestGetDeveloperApiToken:
    def test_success(self):
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = (
            '{"token": "abc123", "resourceUri": "https://my-app.snowflakecomputing.com"}',
        )

        mock_conn = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor

        result = get_developer_api_token(mock_conn, "DB.SCHEMA.APP")

        assert result.token == "abc123"
        assert result.resource_uri == "https://my-app.snowflakecomputing.com"
        mock_cursor.execute.assert_called_once_with(
            "call SYSTEM$GET_STREAMLIT_DEVELOPER_API_TOKEN('DB.SCHEMA.APP', false)"
        )
        mock_cursor.close.assert_called_once()

    def test_empty_response_raises(self):
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = None

        mock_conn = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(ClickException, match="Empty response"):
            get_developer_api_token(mock_conn, "DB.SCHEMA.APP")

    def test_empty_token_raises(self):
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = (
            '{"token": "", "resourceUri": "https://example.com"}',
        )

        mock_conn = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(ClickException, match="Empty token"):
            get_developer_api_token(mock_conn, "DB.SCHEMA.APP")

    def test_empty_resource_uri_raises(self):
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = (
            '{"token": "abc", "resourceUri": ""}',
        )

        mock_conn = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(ClickException, match="Empty resourceUri"):
            get_developer_api_token(mock_conn, "DB.SCHEMA.APP")

    def test_single_quote_in_fqn_raises(self):
        mock_conn = mock.Mock()

        with pytest.raises(ClickException, match="single quotes"):
            get_developer_api_token(mock_conn, "DB.SCHEMA.APP'; DROP TABLE --")

    def test_cursor_closed_on_error(self):
        mock_cursor = mock.Mock()
        mock_cursor.execute.side_effect = Exception("SQL error")

        mock_conn = mock.Mock()
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(Exception, match="SQL error"):
            get_developer_api_token(mock_conn, "DB.SCHEMA.APP")

        mock_cursor.close.assert_called_once()


class TestEncodeStreamLogsRequest:
    def test_encodes_tail_lines(self):
        data = encode_stream_logs_request(100)
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_zero_tail_lines(self):
        data = encode_stream_logs_request(0)
        assert isinstance(data, bytes)

    def test_roundtrip_via_pb2(self):
        """Verify encoding matches what the protobuf library produces."""
        from snowflake.cli._plugins.streamlit.proto import logs_service_pb2 as pb2

        for tail_lines in [0, 1, 50, 100, 1000, 10000]:
            encoded = encode_stream_logs_request(tail_lines)
            decoded = pb2.StreamLogsRequest()
            decoded.ParseFromString(encoded)
            assert decoded.tail_lines == tail_lines


class TestDecodeLogEntry:
    def _make_pb2_log_entry(self, log_source, content, seconds, nanos, sequence, level):
        from snowflake.cli._plugins.streamlit.proto import logs_service_pb2 as pb2

        entry = pb2.LogEntry()
        entry.log_source = log_source
        entry.content = content
        entry.timestamp.seconds = seconds
        entry.timestamp.nanos = nanos
        entry.sequence = sequence
        entry.level = level
        return entry.SerializeToString()

    def test_decode_app_log(self):
        data = self._make_pb2_log_entry(
            log_source=1,  # LOG_SOURCE_APP
            content="Hello from app",
            seconds=1700000000,
            nanos=500000000,
            sequence=42,
            level=2,  # LOG_LEVEL_INFO
        )
        entry = decode_log_entry(data)

        assert entry.log_source == LOG_SOURCE_APP
        assert entry.content == "Hello from app"
        assert entry.sequence == 42
        assert entry.level == LOG_LEVEL_INFO
        assert entry.source_label == "APP"
        assert entry.level_label == "INFO"

    def test_decode_manager_log(self):
        data = self._make_pb2_log_entry(
            log_source=2,  # LOG_SOURCE_MANAGER
            content="Manager message",
            seconds=1700000000,
            nanos=0,
            sequence=1,
            level=3,  # LOG_LEVEL_WARN
        )
        entry = decode_log_entry(data)

        assert entry.log_source == LOG_SOURCE_MANAGER
        assert entry.content == "Manager message"
        assert entry.source_label == "MGR"
        assert entry.level_label == "WARN"

    def test_format_line_includes_level(self):
        entry = LogEntry(
            log_source=LOG_SOURCE_APP,
            content="test message",
            timestamp=datetime(2024, 1, 15, 10, 30, 45, 123000, tzinfo=timezone.utc),
            sequence=7,
            level=LOG_LEVEL_INFO,
        )
        line = entry.format_line()
        assert line == "[2024-01-15 10:30:45.123] [INFO] [APP] [seq:7] test message"

    def test_format_line_warn_level(self):
        entry = LogEntry(
            log_source=LOG_SOURCE_MANAGER,
            content="warning msg",
            timestamp=datetime(2024, 6, 1, 12, 0, 0, 0, tzinfo=timezone.utc),
            sequence=99,
            level=LOG_LEVEL_WARN,
        )
        line = entry.format_line()
        assert line == "[2024-06-01 12:00:00.000] [WARN] [MGR] [seq:99] warning msg"


class TestStreamLogs:
    @mock.patch("snowflake.cli._plugins.streamlit.log_streaming.websocket.WebSocket")
    def test_yields_log_entries(self, mock_ws_class):
        from snowflake.cli._plugins.streamlit.proto import logs_service_pb2 as pb2

        entry1 = pb2.LogEntry(
            log_source=1, content="line1", sequence=1, level=2
        )
        entry1.timestamp.seconds = 1700000000
        entry2 = pb2.LogEntry(
            log_source=2, content="line2", sequence=2, level=3
        )
        entry2.timestamp.seconds = 1700000001

        import websocket as ws_lib

        mock_ws = mock.Mock()
        mock_ws_class.return_value = mock_ws
        mock_ws.recv_data.side_effect = [
            (ws_lib.ABNF.OPCODE_BINARY, entry1.SerializeToString()),
            (ws_lib.ABNF.OPCODE_BINARY, entry2.SerializeToString()),
            (ws_lib.ABNF.OPCODE_CLOSE, b""),
        ]

        entries = list(stream_logs("wss://test/logs", "token123", 100))

        assert len(entries) == 2
        assert entries[0].content == "line1"
        assert entries[0].source_label == "APP"
        assert entries[1].content == "line2"
        assert entries[1].source_label == "MGR"

        mock_ws.connect.assert_called_once_with(
            "wss://test/logs",
            header=['Authorization: Snowflake Token="token123"'],
            timeout=10,
        )

    @mock.patch("snowflake.cli._plugins.streamlit.log_streaming.websocket.WebSocket")
    def test_handles_connection_closed(self, mock_ws_class):
        import websocket as ws_lib

        mock_ws = mock.Mock()
        mock_ws_class.return_value = mock_ws
        mock_ws.recv_data.side_effect = ws_lib.WebSocketConnectionClosedException()

        entries = list(stream_logs("wss://test/logs", "token123", 100))
        assert entries == []

    @mock.patch("snowflake.cli._plugins.streamlit.log_streaming.websocket.WebSocket")
    def test_handles_timeout(self, mock_ws_class):
        import websocket as ws_lib

        mock_ws = mock.Mock()
        mock_ws_class.return_value = mock_ws
        mock_ws.recv_data.side_effect = ws_lib.WebSocketTimeoutException()

        entries = list(stream_logs("wss://test/logs", "token123", 100))
        assert entries == []

    @mock.patch("snowflake.cli._plugins.streamlit.log_streaming.websocket.WebSocket")
    def test_graceful_close_on_normal_exit(self, mock_ws_class):
        import websocket as ws_lib

        mock_ws = mock.Mock()
        mock_ws_class.return_value = mock_ws
        mock_ws.recv_data.side_effect = [
            (ws_lib.ABNF.OPCODE_CLOSE, b""),
        ]

        list(stream_logs("wss://test/logs", "token123", 100))

        mock_ws.close.assert_called_once_with(status=ws_lib.STATUS_NORMAL)

    @mock.patch("snowflake.cli._plugins.streamlit.log_streaming.websocket.WebSocket")
    def test_skips_malformed_protobuf(self, mock_ws_class):
        import websocket as ws_lib

        mock_ws = mock.Mock()
        mock_ws_class.return_value = mock_ws
        mock_ws.recv_data.side_effect = [
            (ws_lib.ABNF.OPCODE_BINARY, b"\xff\xff\xff"),  # invalid protobuf
            (ws_lib.ABNF.OPCODE_CLOSE, b""),
        ]

        entries = list(stream_logs("wss://test/logs", "token123", 100))
        assert entries == []
