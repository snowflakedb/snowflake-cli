"""These tests verify that the CLI runs work as expected."""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest import mock

from snowcli.__about__ import VERSION
from snowcli.config import cli_config


def test_help_option(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0


def test_streamlit_help(runner):
    result = runner.invoke(["streamlit", "--help"], catch_exceptions=False)
    assert result.exit_code == 0, result.output


@mock.patch("snowcli.snow_connector.SnowflakeConnector")
@mock.patch.dict(os.environ, {}, clear=True)
def test_custom_config_path(mock_conn, runner):
    config_file = Path(__file__).parent / "test.toml"
    mock_conn.return_value.ctx.execute_string.return_value = [None, mock.MagicMock()]
    result = runner.invoke(
        ["--config-file", str(config_file), "warehouse", "status"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(
        connection_parameters={
            "database": "db_for_test",
            "schema": "test_public",
            "role": "test_role",
            "warehouse": "xs",
            "password": "dummy_password",
        },
        overrides={},
    )


def test_info_callback(runner):
    result = runner.invoke(["--info", "--format", "json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [
        {"key": "version", "value": VERSION},
        {"key": "default_config_file_path", "value": str(cli_config.file_path)},
    ]
