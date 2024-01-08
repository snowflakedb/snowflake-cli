import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from snowcli.cli import loggers
from snowcli.config import config_init
from snowcli.exception import InvalidLogsConfiguration
from snowflake.connector.config_manager import CONFIG_MANAGER

from tests.conftest import clean_logging_handlers


def setup_logging(
    tmpdir: Path,
    *,
    save_logs: Optional[bool] = None,
    level: Optional[str] = None,
    verbose: bool = False,
    debug: bool = False,
    do_not_create_directory: bool = False,
):
    logs_path = _get_logs_dir(tmpdir)
    config_path = tmpdir / "config.toml"

    if not do_not_create_directory:
        logs_path.mkdir()
    config_path.write_text(
        "\n".join(
            x
            for x in [
                "[connections]",
                "",
                "[logs]",
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
    print(CONFIG_MANAGER.file_path)
    print(config_path.read_text())
    loggers.create_loggers(verbose=verbose, debug=debug)
    print(_list_handlers())
    assert len(_list_handlers()) == (2 if save_logs else 1)


def print_log_messages():
    logger = logging.getLogger("snowcli")
    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.critical("critical message")
    _flush_logs()


def _flush_logs():
    for handler in logging.getLogger("snowcli").handlers:
        handler.flush()


def _list_handlers():
    return logging.getLogger("snowcli").handlers


def _get_logs_dir(tmpdir: Path) -> Path:
    return tmpdir / "logs"


def _get_logs_file(tmpdir: Path) -> Path:
    return next(_get_logs_dir(tmpdir).iterdir())


def assert_log_level(log_messages: str, expected_level: str) -> None:
    all_levels = ["debug", "info", "warning", "error", "critical"]
    assert expected_level in all_levels

    expected_in_logs = False
    for level in all_levels:
        if level == expected_level:
            expected_in_logs = True
        assert expected_in_logs == (level.upper() in log_messages)


def assert_file_log_level(tmpdir, expected_level: str) -> None:
    assert_log_level(_get_logs_file(tmpdir).read_text(), expected_level)


def assert_log_dir_is_empty(tmpdir: Path) -> None:
    assert len(list(_get_logs_dir(tmpdir).iterdir())) == 0


def test_logs_not_saved_by_default():
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        setup_logging(tmpdir)
        print_log_messages()

        assert_log_dir_is_empty(tmpdir)


def test_logs_can_be_turned_off_by_config():
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        setup_logging(tmpdir, save_logs=False)
        print_log_messages()

        assert_log_dir_is_empty(tmpdir)


def test_logs_default_level_is_info():
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        setup_logging(tmpdir, save_logs=True)
        print_log_messages()
        assert_file_log_level(tmpdir, expected_level="info")


def test_log_level_is_configurable():
    for level in ["debug", "info", "warning", "error", "critical"]:
        with TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            setup_logging(tmpdir, save_logs=True, level=level)
            print_log_messages()
            assert_file_log_level(tmpdir, expected_level=level)


def test_log_level_is_overriden_by_debug_flag(capsys):
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        setup_logging(tmpdir, save_logs=True, level="warning", debug=True)
        print_log_messages()
        assert_file_log_level(tmpdir, expected_level="debug")
        captured = capsys.readouterr()
        assert_log_level(captured.out + captured.err, expected_level="debug")
        assert_file_log_level(tmpdir, expected_level="debug")


def test_log_level_is_not_overriden_by_verbose_flag(capsys):
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        setup_logging(tmpdir, save_logs=True, verbose=True, level="warning")
        print_log_messages()
        captured = capsys.readouterr()
        assert_log_level(captured.out + captured.err, expected_level="info")
        assert_file_log_level(tmpdir, expected_level="warning")


def test_stdout_log_level_remains_error(capsys):
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        setup_logging(tmpdir, save_logs=True, level="debug")
        print_log_messages()
        captured = capsys.readouterr()
        assert_log_level(captured.out + captured.err, expected_level="error")


def test_log_directory_does_not_exist():
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        try:
            setup_logging(tmpdir, save_logs=True, do_not_create_directory=True)
            assert False, "Bug: below error should be thrown"
        except InvalidLogsConfiguration as e:
            assert e.message == f"Directory '{_get_logs_dir(tmpdir)}' does not exist"


def test_incorrect_log_level_in_config():
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        try:
            setup_logging(tmpdir, save_logs=True, level="funny_level")
            assert False, "Bug: below error should be thrown"
        except InvalidLogsConfiguration as e:
            assert (
                e.message == "Invalid 'level' value set in [logs] section: funny_level."
                " 'level' should be one of: DEBUG / INFO / WARNING / ERROR / CRITICAL"
            )
