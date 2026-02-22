# Copyright (c) 2026 Snowflake Inc.
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
Protobuf codec for the Streamlit developer log streaming protocol.

Uses generated protobuf classes from logs_service.proto and provides
a Python-friendly dataclass wrapper for log entries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from snowflake.cli._plugins.streamlit.proto.generated.developer.v1 import (
    logs_service_pb2 as pb2,
)

# Re-export enum values for convenience
LOG_SOURCE_APP = pb2.LOG_SOURCE_APP
LOG_SOURCE_MANAGER = pb2.LOG_SOURCE_MANAGER
LOG_SOURCE_UNSPECIFIED = pb2.LOG_SOURCE_UNSPECIFIED

LOG_LEVEL_DEBUG = pb2.LOG_LEVEL_DEBUG
LOG_LEVEL_INFO = pb2.LOG_LEVEL_INFO
LOG_LEVEL_WARN = pb2.LOG_LEVEL_WARN
LOG_LEVEL_ERROR = pb2.LOG_LEVEL_ERROR
LOG_LEVEL_UNSPECIFIED = pb2.LOG_LEVEL_UNSPECIFIED

LOG_SOURCE_LABELS = {
    LOG_SOURCE_APP: "APP",
    LOG_SOURCE_MANAGER: "MGR",
    LOG_SOURCE_UNSPECIFIED: "UNKNOWN",
}

LOG_LEVEL_LABELS = {
    LOG_LEVEL_UNSPECIFIED: "UNKNOWN",
    LOG_LEVEL_DEBUG: "DEBUG",
    LOG_LEVEL_INFO: "INFO",
    LOG_LEVEL_WARN: "WARN",
    LOG_LEVEL_ERROR: "ERROR",
}


@dataclass
class LogEntry:
    log_source: int
    content: str
    timestamp: datetime
    sequence: int
    level: int

    @property
    def source_label(self) -> str:
        return LOG_SOURCE_LABELS.get(self.log_source, "UNKNOWN")

    @property
    def level_label(self) -> str:
        return LOG_LEVEL_LABELS.get(self.level, "UNKNOWN")

    def format_line(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"[{ts}] [{self.level_label}] [{self.source_label}] [seq:{self.sequence}] {self.content}"

    def to_dict(self) -> dict[str, str | int]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level_label,
            "source": self.source_label,
            "sequence": self.sequence,
            "content": self.content,
        }


def encode_stream_logs_request(tail_lines: int) -> bytes:
    """Encode a StreamLogsRequest protobuf message to binary."""
    request = pb2.StreamLogsRequest(tail_lines=tail_lines)
    return request.SerializeToString()


def decode_log_entry(data: bytes) -> LogEntry:
    """Decode a binary protobuf LogEntry message into a Python dataclass."""
    entry = pb2.LogEntry()
    entry.ParseFromString(data)

    if entry.HasField("timestamp"):
        ts = entry.timestamp.ToDatetime(tzinfo=timezone.utc)
    else:
        ts = datetime.fromtimestamp(0, tz=timezone.utc)

    return LogEntry(
        log_source=entry.log_source,
        content=entry.content,
        timestamp=ts,
        sequence=entry.sequence,
        level=entry.level,
    )
