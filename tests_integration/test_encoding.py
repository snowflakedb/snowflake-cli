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

import contextlib
import locale
import os
import tempfile
import warnings
from pathlib import Path

import pytest

from snowflake.cli._plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    execute_script_in_sandbox,
)
from tests_common import IS_WINDOWS

UNICODE_SQL_CONTENT = "select '日本語テスト' as RESULT;\n"
UNICODE_EXPECTED_VALUE = "日本語テスト"

GERMAN_SQL_CONTENT = "select 'Ä Ö Ü ß Straße' as RESULT;\n"
GERMAN_EXPECTED_VALUE = "Ä Ö Ü ß Straße"

CHINESE_SQL_CONTENT = "select '中文测试数据' as RESULT;\n"
CHINESE_EXPECTED_VALUE = "中文测试数据"

KOREAN_SQL_CONTENT = "select '한국어 테스트' as RESULT;\n"
KOREAN_EXPECTED_VALUE = "한국어 테스트"

MULTI_SCRIPT_CONTENT = "select '日本語' as A;\nselect 'Ä Ö Ü' as B;\nselect '中文' as C;\n"


def _write_file_with_encoding(path: Path, content: str, encoding: str):
    path.write_bytes(content.encode(encoding))


class TestEncodingScenarios:
    @pytest.mark.integration
    def test_sql_file_cp1252_german_content(self, runner, tmp_path):
        sql_file = tmp_path / "cp1252_query.sql"
        _write_file_with_encoding(sql_file, GERMAN_SQL_CONTENT, "cp1252")

        result = runner.invoke_with_connection_json(
            ["sql", "-f", str(sql_file)],
            env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "cp1252"},
        )

        assert result.exit_code == 0, result.output
        assert result.json == [{"RESULT": GERMAN_EXPECTED_VALUE}]

    @pytest.mark.integration
    def test_sql_file_cp932_japanese_content(self, runner, tmp_path):
        sql_file = tmp_path / "cp932_query.sql"
        _write_file_with_encoding(sql_file, UNICODE_SQL_CONTENT, "cp932")

        result = runner.invoke_with_connection_json(
            ["sql", "-f", str(sql_file)],
            env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "cp932"},
        )

        assert result.exit_code == 0, result.output
        assert result.json == [{"RESULT": UNICODE_EXPECTED_VALUE}]

    @pytest.mark.integration
    def test_sql_file_cp936_chinese_content(self, runner, tmp_path):
        sql_file = tmp_path / "cp936_query.sql"
        _write_file_with_encoding(sql_file, CHINESE_SQL_CONTENT, "cp936")

        result = runner.invoke_with_connection_json(
            ["sql", "-f", str(sql_file)],
            env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "cp936"},
        )

        assert result.exit_code == 0, result.output
        assert result.json == [{"RESULT": CHINESE_EXPECTED_VALUE}]

    @pytest.mark.integration
    @pytest.mark.skipif(not IS_WINDOWS, reason="Windows-only test")
    def test_utf8_file_without_encoding_does_not_work_on_windows(
        self, runner, tmp_path
    ):
        sql_file = tmp_path / "default_encoding.sql"
        _write_file_with_encoding(sql_file, UNICODE_SQL_CONTENT, "utf-8")

        result = runner.invoke_with_connection_json(
            ["sql", "-f", str(sql_file)],
        )

        assert result.exit_code != 0

    @pytest.mark.integration
    def test_multi_query_file_with_encoding(self, runner, tmp_path):
        sql_file = tmp_path / "multi.sql"
        _write_file_with_encoding(sql_file, MULTI_SCRIPT_CONTENT, "utf-8")

        result = runner.invoke_with_connection_json(
            ["sql", "-f", str(sql_file)],
            env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "utf-8"},
        )

        assert result.exit_code == 0, result.output
        assert result.json == [
            [{"A": "日本語"}],
            [{"B": "Ä Ö Ü"}],
            [{"C": "中文"}],
        ]


@contextlib.contextmanager
def _temporary_locale(category, locale_name):
    original = locale.setlocale(category)
    try:
        locale.setlocale(category, locale_name)
        yield
    except locale.Error:
        pytest.skip(f"Locale {locale_name!r} not available on this system")
    finally:
        locale.setlocale(category, original)


class TestLocalesScenarios:
    @pytest.mark.integration
    @pytest.mark.skipif(IS_WINDOWS, reason="Linux/macOS locale test")
    def test_c_locale_with_encoding_override(self, runner, tmp_path):
        sql_file = tmp_path / "c_locale.sql"
        _write_file_with_encoding(sql_file, UNICODE_SQL_CONTENT, "utf-8")

        with _temporary_locale(locale.LC_CTYPE, "C"):
            result = runner.invoke_with_connection_json(
                ["sql", "-f", str(sql_file)],
                env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "utf-8"},
            )

        assert result.exit_code == 0, result.output
        assert result.json == [{"RESULT": UNICODE_EXPECTED_VALUE}]

    @pytest.mark.integration
    @pytest.mark.skipif(IS_WINDOWS, reason="Linux/macOS locale test")
    def test_posix_locale_with_encoding_override(self, runner, tmp_path):
        sql_file = tmp_path / "posix_locale.sql"
        _write_file_with_encoding(sql_file, CHINESE_SQL_CONTENT, "utf-8")

        with _temporary_locale(locale.LC_CTYPE, "POSIX"):
            result = runner.invoke_with_connection_json(
                ["sql", "-f", str(sql_file)],
                env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "utf-8"},
            )

        assert result.exit_code == 0, result.output
        assert result.json == [{"RESULT": CHINESE_EXPECTED_VALUE}]

    @pytest.mark.integration
    @pytest.mark.skipif(IS_WINDOWS, reason="Linux/macOS locale test")
    def test_mixed_unicode_in_single_query_under_utf8_locale(self, runner):
        mixed = "select '日本語 Ä Ö Ü 中文 한국어' as RESULT"

        with _temporary_locale(locale.LC_CTYPE, "en_US.UTF-8"):
            result = runner.invoke_with_connection_json(
                ["sql", "-q", mixed],
                env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "utf-8"},
            )

        assert result.exit_code == 0, result.output
        assert result.json == [{"RESULT": "日本語 Ä Ö Ü 中文 한국어"}]

    @pytest.mark.integration
    @pytest.mark.skipif(IS_WINDOWS, reason="Linux/macOS locale test")
    def test_sql_source_command_with_utf8_locale(self, runner, tmp_path):
        include_file = tmp_path / "include.sql"
        _write_file_with_encoding(include_file, "select '日本語テスト' as RESULT;\n", "utf-8")

        with _temporary_locale(locale.LC_CTYPE, "en_US.UTF-8"):
            result = runner.invoke_with_connection_json(
                [
                    "sql",
                    "-q",
                    f"!source {include_file.as_posix()};",
                ],
                env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "utf-8"},
            )

        assert result.exit_code == 0, result.output
        assert UNICODE_EXPECTED_VALUE in str(result.json)


class TestWarnings:
    @pytest.mark.integration
    @pytest.mark.skipif(IS_WINDOWS, reason="Unix-only test")
    def test_no_encoding_warning_on_utf8_unix(self, runner):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            result = runner.invoke_with_connection(
                ["sql", "-q", "select 1 as OK"],
            )

            assert result.exit_code == 0, result.output
            encoding_warnings = [
                w for w in caught if "encoding" in str(w.message).lower()
            ]
            assert (
                len(encoding_warnings) == 0
            ), f"Unexpected encoding warnings on UTF-8 Unix: {encoding_warnings}"

    @pytest.mark.integration
    def test_no_warning_when_both_encodings_configured(self, runner):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            result = runner.invoke_with_connection(
                ["sql", "-q", "select 1 as OK"],
                env={
                    "SNOWFLAKE_CLI_ENCODING_FILE_IO": "utf-8",
                    "SNOWFLAKE_CLI_ENCODING_SUBPROCESS": "utf-8",
                },
            )

            assert result.exit_code == 0, result.output
            encoding_warnings = [
                w for w in caught if "encoding" in str(w.message).lower()
            ]
            assert (
                len(encoding_warnings) == 0
            ), f"Unexpected encoding warnings: {encoding_warnings}"

    @pytest.mark.integration
    @pytest.mark.skipif(
        not IS_WINDOWS, reason="Only Windows requires suppressing warnings"
    )
    def test_warnings_suppressed(self, runner):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            result = runner.invoke_with_connection(
                ["sql", "-q", "select 1 as OK"],
                env={
                    "SNOWFLAKE_CLI_ENCODING_SHOW_WARNINGS": "false",
                },
            )

            assert result.exit_code == 0, result.output
            encoding_warnings = [
                w for w in caught if "encoding" in str(w.message).lower()
            ]
            assert (
                len(encoding_warnings) == 0
            ), f"Warnings should be suppressed: {encoding_warnings}"


class TestStageOperationsWithEncoding:
    @pytest.mark.integration
    def test_stage_execute_utf8_sql_file(
        self, runner, snowflake_session, test_database, tmp_path
    ):
        stage_name = "test_encoding_stage"
        runner.invoke_with_connection_json(["stage", "create", stage_name])

        sql_file = tmp_path / "encoded.sql"
        _write_file_with_encoding(sql_file, UNICODE_SQL_CONTENT, "utf-8")

        result = runner.invoke_with_connection_json(
            ["stage", "copy", str(sql_file), f"@{stage_name}"],
            env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "utf-8"},
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["stage", "execute", f"@{stage_name}/encoded.sql"],
            env={"SNOWFLAKE_CLI_ENCODING_FILE_IO": "utf-8"},
        )
        assert result.exit_code == 0, result.output

        runner.invoke_with_connection_json(["stage", "drop", stage_name])


class TestSubprocessOutputDecoding:
    @pytest.mark.integration
    def test_snowpark_package_create_uses_subprocess_encoding(self, runner):
        init_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            try:
                result = runner.invoke_with_connection(
                    [
                        "snowpark",
                        "package",
                        "create",
                        "dummy-pkg-for-tests-with-deps",
                        "--ignore-anaconda",
                    ],
                    env={"SNOWFLAKE_CLI_ENCODING_SUBPROCESS": "utf-8"},
                )
                assert result.exit_code == 0, result.output
                assert Path("dummy_pkg_for_tests_with_deps.zip").exists()
            finally:
                os.chdir(init_dir)

    @pytest.mark.integration
    def test_snowpark_package_create_with_cp1252_subprocess_encoding(self, runner):
        init_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            try:
                result = runner.invoke_with_connection(
                    [
                        "snowpark",
                        "package",
                        "create",
                        "dummy-pkg-for-tests-with-deps",
                        "--ignore-anaconda",
                    ],
                    env={"SNOWFLAKE_CLI_ENCODING_SUBPROCESS": "cp1252"},
                )
                assert result.exit_code == 0, result.output
                assert Path("dummy_pkg_for_tests_with_deps.zip").exists()
            finally:
                os.chdir(init_dir)

    @pytest.mark.integration
    def test_spcs_image_registry_login_uses_subprocess_encoding(self, runner):
        result = runner.invoke_with_connection(
            ["spcs", "image-registry", "login"],
            env={"SNOWFLAKE_CLI_ENCODING_SUBPROCESS": "utf-8"},
        )
        assert "Login Succeeded" in result.output or result.exit_code != 0

    @pytest.mark.integration
    def test_sandbox_execute_script_uses_subprocess_encoding(self, monkeypatch):

        monkeypatch.setenv("SNOWFLAKE_CLI_ENCODING_SUBPROCESS", "utf-8")

        result = execute_script_in_sandbox(
            script_source="print('日本語テスト café Straße')",
            env_type=ExecutionEnvironmentType.CURRENT,
        )

        assert result.returncode == 0, result.stderr
        assert "日本語テスト café Straße" in result.stdout

    @pytest.mark.integration
    def test_sandbox_execute_script_with_pythonutf8_mode(self, monkeypatch):
        monkeypatch.setenv("PYTHONUTF8", "1")

        result = execute_script_in_sandbox(
            script_source="print('日本語テスト café Straße')",
            env_type=ExecutionEnvironmentType.CURRENT,
        )

        assert result.returncode == 0, result.stderr
        assert "日本語テスト café Straße" in result.stdout
