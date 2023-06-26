"""These tests verify that the CLI runs work as expected."""
from __future__ import annotations

from pathlib import Path
from unittest import mock


def test_help_option(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0


def test_streamlit_help(runner):
    result = runner.invoke(["streamlit", "--help"], catch_exceptions=False)
    assert result.exit_code == 0, result.output


@mock.patch("snowcli.config.SnowflakeConnector")
def test_custom_config_path(mock_conn, runner):
    config_file = Path(__file__).parent / "test.toml"
    runner.invoke(["--config-file", str(config_file), "warehouse", "status"])
    mock_conn.assert_called_once_with(
        connection_parameters={
            "database": "db_for_test",
            "schema": "test_public",
            "role": "test_role",
            "warehouse": "xs",
        },
        overrides={},
    )
