"""These tests verify that the CLI runs work as expected."""
from __future__ import annotations

from pathlib import Path
from unittest import mock

from testing_utils.result_assertions import (
    assert_that_result_is_exception_without_debug_mode,
    assert_that_result_is_exception_with_debug_mode,
)


def test_help_option(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0


def test_streamlit_help(runner):
    result = runner.invoke(["streamlit", "--help"])
    assert result.exit_code == 0


@mock.patch("snowcli.cli.warehouse.config")
def test_custom_config_path(mock_config, runner):
    config_file = Path(__file__).parent / "test.toml"
    runner.invoke(["--config-file", str(config_file), "warehouse", "status"])
    mock_config.snowflake_connection.show_warehouses.assert_called_once_with(
        database="db_for_test", schema="test_public", role="test_role", warehouse="xs"
    )


@mock.patch("snowcli.cli.sql.config.is_auth")
def test_regular_exception_handling_for_top_level_command(mock_is_auth, runner):
    mock_is_auth.side_effect = ValueError("Test exception message")
    result = runner.invoke(["sql", "-q", "select 1"])

    assert_that_result_is_exception_without_debug_mode(
        result, "ValueError", "Test exception message"
    )


@mock.patch("snowcli.cli.sql.config.is_auth")
def test_debug_exception_handling_for_top_level_command(mock_is_auth, runner):
    mock_is_auth.side_effect = ValueError("Test exception message")
    result = runner.invoke(["--debug", "sql", "-q", "select 1"])

    assert_that_result_is_exception_with_debug_mode(
        result, ValueError("Test exception message")
    )
