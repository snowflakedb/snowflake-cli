"""Tests ensuring resolver logging never exposes sensitive values."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import pytest
from snowflake.cli.api.config_ng.core import SourceType, ValueSource
from snowflake.cli.api.config_ng.resolver import ConfigurationResolver


class _BrokenSource(ValueSource):
    def __init__(
        self,
        *,
        source_type: SourceType,
        exception: Exception,
        source_name: ValueSource.SourceName = "cli_config_toml",
    ):
        self._source_type = source_type
        self._exception = exception
        self._source_name = source_name

    @property
    def source_name(self) -> ValueSource.SourceName:
        return self._source_name

    @property
    def source_type(self) -> SourceType:
        return self._source_type

    def discover(self, key: Optional[str] = None) -> Dict[str, Any]:
        raise self._exception

    def supports_key(self, key: str) -> bool:
        return True


def test_file_source_errors_are_sanitized(caplog: pytest.LogCaptureFixture):
    secret = "SuperSecret123!"
    resolver = ConfigurationResolver(
        sources=[
            _BrokenSource(
                source_type=SourceType.FILE,
                exception=ValueError(f"Raw secret: {secret}"),
            )
        ]
    )

    with caplog.at_level(logging.WARNING):
        resolved = resolver.resolve()

    assert resolved == {}
    assert secret not in caplog.text
    assert "ValueError" in caplog.text
    assert "details_masked" in caplog.text


def test_overlay_source_logs_include_only_structural_metadata(
    caplog: pytest.LogCaptureFixture,
):
    class StructuredError(Exception):
        def __init__(self):
            super().__init__("leaked!")  # pragma: no cover
            self.section = "connections.default"
            self.option = "password"

    resolver = ConfigurationResolver(
        sources=[
            _BrokenSource(
                source_type=SourceType.OVERLAY,
                exception=StructuredError(),
                source_name="cli_env",
            )
        ]
    )

    with caplog.at_level(logging.WARNING):
        resolved = resolver.resolve()

    assert resolved == {}
    assert "connections.default" in caplog.text
    assert "password" in caplog.text
    assert "leaked!" not in caplog.text
