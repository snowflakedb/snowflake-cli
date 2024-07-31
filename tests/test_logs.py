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

import logging
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import pytest
import tomlkit
from snowflake.cli.api.config import config_init
from snowflake.cli.api.exceptions import InvalidLogsConfiguration
from snowflake.cli.app import loggers

from tests.conftest import clean_logging_handlers
from tests.testing_utils.files_and_dirs import assert_file_permissions_are_strict
from tests_common import IS_WINDOWS


@pytest.fixture
def setup_config_and_logs(snowflake_home):
    @contextmanager
    def _setup_config_and_logs(
        *,
        save_logs: Optional[bool] = None,
        level: Optional[str] = None,
        verbose: bool = False,
        debug: bool = False,
        use_custom_logs_path=False,
    ):
        logs_path = snowflake_home / "logs"
        if use_custom_logs_path:
            logs_path = snowflake_home / "custom" / "logs"

        config_path = snowflake_home / "config.toml"
        log_config_data: dict[str, str | bool] = {}
        config_data = dict(connections={}, cli=dict(logs=log_config_data))
        if use_custom_logs_path:
            log_config_data["path"] = str(logs_path)
        if save_logs is not None:
            log_config_data["save_logs"] = save_logs
        if level:
            log_config_data["level"] = level
        tomlkit.dump(config_data, config_path.open("w"))

        config_path.chmod(0o700)

        # Make sure we start without any leftovers
        clean_logging_handlers()
        shutil.rmtree(logs_path, ignore_errors=True)

        # Setup loggers
        config_init(config_path)
        loggers.create_loggers(verbose=verbose, debug=debug)
        assert len(_list_handlers()) == (2 if save_logs else 1)

        yield logs_path

        # After the test, logging handlers still have open file handles
        # Close everything so we can delete the log file
        clean_logging_handlers()
        shutil.rmtree(logs_path, ignore_errors=True)

    return _setup_config_and_logs


def print_log_messages():
    logger = logging.getLogger("snowflake.cli")
    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.critical("critical message")
    _flush_logs()


def _flush_logs() -> None:
    for handler in logging.getLogger("snowflake.cli").handlers:
        handler.flush()


def _list_handlers():
    return logging.getLogger("snowflake.cli").handlers


def get_logs_file(logs_path: Path) -> Path:
    return next(logs_path.iterdir())


def assert_log_level(log_messages: str, expected_level: str) -> None:
    all_levels = ["debug", "info", "warning", "error", "critical"]
    assert expected_level in all_levels

    expected_in_logs = False
    for level in all_levels:
        if level == expected_level:
            expected_in_logs = True
        assert expected_in_logs == (level.upper() in log_messages)


def assert_file_log_level(logs_path: Path, expected_level: str) -> None:
    assert_log_level(get_logs_file(logs_path).read_text(), expected_level)


def assert_log_is_empty(logs_path: Path) -> None:
    assert get_logs_file(logs_path).read_text() == ""


def test_logs_section_appears_in_fresh_config_file(temp_dir):
    config_file = Path(temp_dir) / "sub" / "config.toml"
    assert config_file.exists() is False
    config_init(config_file)
    assert config_file.exists() is True
    assert '[cli.logs]\nsave_logs = true\npath = "' in config_file.read_text()
    assert f'{os.sep}logs"\nlevel = "info"' in config_file.read_text()


def test_logs_saved_by_default(setup_config_and_logs):
    with setup_config_and_logs(save_logs=True) as logs_path:
        print_log_messages()
        assert_file_log_level(logs_path, expected_level="info")


def test_default_logs_location_is_created_automatically(setup_config_and_logs):
    with setup_config_and_logs(save_logs=True) as logs_path:
        print_log_messages()
        assert logs_path.exists()


def test_logs_can_be_turned_off_by_config(setup_config_and_logs):
    with setup_config_and_logs(save_logs=False) as logs_path:
        print_log_messages()
        assert not logs_path.exists()


def test_logs_path_is_configurable(setup_config_and_logs):
    with setup_config_and_logs(save_logs=True, use_custom_logs_path=True) as logs_path:
        print_log_messages()
        assert_file_log_level(logs_path, expected_level="info")


def test_logs_default_level_is_info(setup_config_and_logs):
    with setup_config_and_logs(save_logs=True) as logs_path:
        print_log_messages()
        assert_file_log_level(logs_path, expected_level="info")


def test_log_level_is_configurable(setup_config_and_logs):
    for level in ["debug", "info", "warning", "error", "critical"]:
        with setup_config_and_logs(save_logs=True, level=level) as logs_path:
            print_log_messages()
            assert_file_log_level(logs_path, expected_level=level)


def test_log_level_is_overriden_by_debug_flag(capsys, setup_config_and_logs):
    with setup_config_and_logs(
        save_logs=True, level="warning", debug=True
    ) as logs_path:
        print_log_messages()
        assert_file_log_level(logs_path, expected_level="debug")
        captured = capsys.readouterr()
        assert_log_level(captured.out + captured.err, expected_level="debug")
        assert_file_log_level(logs_path, expected_level="debug")


def test_log_level_is_not_overriden_by_verbose_flag(capsys, setup_config_and_logs):
    with setup_config_and_logs(
        save_logs=True, verbose=True, level="warning"
    ) as logs_path:
        print_log_messages()
        captured = capsys.readouterr()
        assert_log_level(captured.out + captured.err, expected_level="info")
        assert_file_log_level(logs_path, expected_level="warning")


def test_stdout_log_level_remains_error(capsys, setup_config_and_logs):
    with setup_config_and_logs(save_logs=True, level="debug"):
        print_log_messages()
        captured = capsys.readouterr()
        assert_log_level(captured.out + captured.err, expected_level="error")


def test_incorrect_log_level_in_config(setup_config_and_logs):
    try:
        with setup_config_and_logs(save_logs=True, level="funny_level"):
            assert False, "Bug: below error should be thrown"
    except InvalidLogsConfiguration as e:
        assert (
            e.message == "Invalid 'level' value set in [logs] section: funny_level."
            " 'level' should be one of: DEBUG / INFO / WARNING / ERROR / CRITICAL"
        )


@pytest.mark.skipif(
    IS_WINDOWS, reason="Permissions for new files aren't strict in Windows"
)
def test_log_files_permissions(setup_config_and_logs):
    with setup_config_and_logs(save_logs=True) as logs_path:
        print_log_messages()
        assert_file_permissions_are_strict(get_logs_file(logs_path))


def test_disabled_logs_with_debug_flag(setup_config_and_logs):
    with setup_config_and_logs(save_logs=False, debug=True):
        print_log_messages()
    # Should not raise exception
