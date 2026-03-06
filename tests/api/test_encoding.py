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

import logging

import pytest
from snowflake.cli.api.encoding import (
    detect_encoding_environment,
    get_file_io_encoding,
    get_subprocess_encoding,
)


class TestGetFileIoEncoding:
    def test_returns_none_when_not_configured(self, monkeypatch):
        monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
        assert get_file_io_encoding() is None

    @pytest.mark.parametrize(
        "env_val,expected",
        [
            ("utf-8", "utf-8"),
            ("cp1252", "cp1252"),
            ("cp932", "cp932"),
        ],
    )
    def test_reads_from_env_var(self, env_val, expected, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", env_val)
        assert get_file_io_encoding() == expected

    def test_env_var_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", "utf-8")
        # Even if config would return something else, env var wins
        assert get_file_io_encoding() == "utf-8"


class TestGetSubprocessEncoding:
    def test_returns_none_when_not_configured(self, monkeypatch):
        monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", raising=False)
        assert get_subprocess_encoding() is None

    def test_reads_from_env_var(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", "utf-8")
        assert get_subprocess_encoding() == "utf-8"


class TestDetectEncodingEnvironment:
    def test_returns_encoding_info(self):
        result = detect_encoding_environment()
        assert "filesystem" in result
        assert "default" in result
        assert "locale" in result

    def test_includes_configured_encoding(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", "utf-8")
        result = detect_encoding_environment()
        assert result.get("configured") == "utf-8"

    def test_no_configured_key_when_not_set(self, monkeypatch):
        monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)
        result = detect_encoding_environment()
        assert "configured" not in result

    def test_logs_warning_on_mismatch(self, monkeypatch, caplog):
        monkeypatch.setattr("sys.getfilesystemencoding", lambda: "utf-8")
        monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
        monkeypatch.setattr("locale.getpreferredencoding", lambda: "cp1252")
        monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)

        with caplog.at_level(logging.WARNING):
            detect_encoding_environment()
        assert "Encoding mismatch detected" in caplog.text

    def test_no_warning_when_consistent(self, monkeypatch, caplog):
        monkeypatch.setattr("sys.getfilesystemencoding", lambda: "utf-8")
        monkeypatch.setattr("sys.getdefaultencoding", lambda: "utf-8")
        monkeypatch.setattr("locale.getpreferredencoding", lambda: "UTF-8")
        monkeypatch.delenv("SNOWFLAKE_CLI_ENCODING_FILE_IO", raising=False)

        with caplog.at_level(logging.WARNING):
            detect_encoding_environment()
        assert "Encoding mismatch" not in caplog.text
