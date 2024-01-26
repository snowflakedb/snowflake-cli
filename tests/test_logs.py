import logging
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import pytest
from snowflake.cli.app import loggers
from snowflake.cli.api.config import config_init
from snowflake.cli.api.exceptions import InvalidLogsConfiguration

from tests.conftest import clean_logging_handlers


@pytest.fixture
def setup_config_and_logs(temp_dir):
    @contextmanager
    def _setup_config_and_logs(
        *,
        save_logs: Optional[bool] = None,
        level: Optional[str] = None,
        verbose: bool = False,
        debug: bool = False,
        do_not_create_directory: bool = False,
    ):
        logs_path = Path(temp_dir) / "logs"
        config_path = Path(temp_dir) / "config.toml"
        if not do_not_create_directory:
            logs_path.mkdir()
        config_path.write_text(
            "\n".join(
                x
                for x in [
                    "[connections]",
                    "",
                    "[cli.logs]",
                    f'path = "{logs_path}"',
                    f"save_logs = {str(save_logs).lower()}" if save_logs else None,
                    f'level = "{level}"' if level else None,
                ]
                if x is not None
            )
        )
        config_path.chmod(0o700)

        clean_logging_handlers()
        config_init(config_path)
        loggers.create_loggers(verbose=verbose, debug=debug)
        assert len(_list_handlers()) == (2 if save_logs else 1)
        yield logs_path
        shutil.rmtree(logs_path)

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


def _get_logs_file(logs_path: Path) -> Path:
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
    assert_log_level(_get_logs_file(logs_path).read_text(), expected_level)


def assert_log_dir_is_empty(logs_path: Path) -> None:
    assert len(list(logs_path.iterdir())) == 0


def test_logs_section_appears_in_fresh_config_file(temp_dir):
    config_file = Path(temp_dir) / "sub" / "config.toml"
    assert config_file.exists() is False
    config_init(config_file)
    assert config_file.exists() is True
    assert '[cli.logs]\nsave_logs = false\npath = "' in config_file.read_text()
    assert '/logs"\nlevel = "info"' in config_file.read_text()


def test_logs_not_saved_by_default(setup_config_and_logs):
    with setup_config_and_logs() as logs_path:
        print_log_messages()
        assert_log_dir_is_empty(logs_path)


def test_logs_can_be_turned_off_by_config(setup_config_and_logs):
    with setup_config_and_logs(save_logs=False) as logs_path:
        print_log_messages()
        assert_log_dir_is_empty(logs_path)


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
    with setup_config_and_logs(save_logs=True, level="debug") as logs_path:
        print_log_messages()
        captured = capsys.readouterr()
        assert_log_level(captured.out + captured.err, expected_level="error")


def test_log_directory_does_not_exist(setup_config_and_logs):
    try:
        with setup_config_and_logs(save_logs=True, do_not_create_directory=True):
            assert False, "Bug: below error should be thrown"
    except InvalidLogsConfiguration as e:
        assert e.message.startswith("Directory '")
        assert e.message.endswith("' does not exist")


def test_incorrect_log_level_in_config(setup_config_and_logs):
    try:
        with setup_config_and_logs(save_logs=True, level="funny_level"):
            assert False, "Bug: below error should be thrown"
    except InvalidLogsConfiguration as e:
        assert (
            e.message == "Invalid 'level' value set in [logs] section: funny_level."
            " 'level' should be one of: DEBUG / INFO / WARNING / ERROR / CRITICAL"
        )
