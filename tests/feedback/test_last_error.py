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

import json
from pathlib import Path
from unittest import mock

import click
import pytest
from snowflake.cli._plugins.feedback.last_error import read_last_cli_error
from snowflake.cli.api.commands.execution_metadata import ExecutionMetadata
from snowflake.cli.api.commands.snow_typer import SnowTyper


def _patch_home(tmp_path: Path):
    fake_manager = mock.Mock()
    fake_manager.file_path = tmp_path / "config.toml"
    return mock.patch(
        "snowflake.cli._plugins.feedback.last_error.get_config_manager",
        return_value=fake_manager,
    )


def _record(tmp_path: Path, command_name: str, exc: BaseException) -> ExecutionMetadata:
    @click.group()
    def snow():
        pass

    @snow.command(command_name)
    def _cmd():
        pass

    parent = click.Context(snow, info_name="snow")
    child = click.Context(_cmd, parent=parent, info_name=command_name)
    execution = ExecutionMetadata()
    with _patch_home(tmp_path):
        with child.scope():
            SnowTyper.exception_handler(exc, execution)
    return execution


def _read(tmp_path: Path) -> dict:
    return json.loads((tmp_path / "last_cli_error.json").read_text(encoding="utf-8"))


def test_exception_handler_writes_id_type_command_and_timestamp(tmp_path):
    secret = "password=hunter2"
    execution = _record(tmp_path, "sql", RuntimeError(secret))
    payload = _read(tmp_path)
    raw = (tmp_path / "last_cli_error.json").read_text(encoding="utf-8")

    assert set(payload) == {"id", "error_type", "command", "timestamp"}
    assert payload["id"] == execution.execution_id
    assert payload["error_type"] == "RuntimeError"
    assert payload["command"] == "sql"
    assert payload["timestamp"].endswith("Z")
    assert secret not in raw
    assert "message" not in payload


def test_exception_handler_does_not_write_without_an_error(tmp_path):
    with _patch_home(tmp_path):
        assert read_last_cli_error() is None
    assert not (tmp_path / "last_cli_error.json").exists()


@pytest.mark.parametrize(
    "exc",
    [
        KeyboardInterrupt(),
        click.exceptions.Exit(2),
        click.exceptions.Abort(),
        SystemExit(1),
    ],
)
def test_exception_handler_skips_non_errors(tmp_path, exc):
    _record(tmp_path, "sql", exc)
    assert not (tmp_path / "last_cli_error.json").exists()


def test_feedback_command_does_not_replace_the_previous_error(tmp_path):
    _record(tmp_path, "sql", RuntimeError("compilation failed"))
    original = (tmp_path / "last_cli_error.json").read_text(encoding="utf-8")
    _record(tmp_path, "feedback", RuntimeError("send failed: token=gho_secret"))
    assert (tmp_path / "last_cli_error.json").read_text(encoding="utf-8") == original
    assert "gho_secret" not in original


def test_read_returns_only_id_and_type(tmp_path):
    secret = "do not send this sql"
    (tmp_path / "last_cli_error.json").write_text(
        json.dumps(
            {
                "id": "abc",
                "error_type": "CliError",
                "command": "sql",
                "timestamp": "2026-10-22T00:00:00Z",
                "message": secret,
            }
        ),
        encoding="utf-8",
    )
    with _patch_home(tmp_path):
        assert read_last_cli_error() == {"id": "abc", "error_type": "CliError"}


def test_read_ignores_corrupt_file(tmp_path):
    (tmp_path / "last_cli_error.json").write_text("{", encoding="utf-8")
    with _patch_home(tmp_path):
        assert read_last_cli_error() is None
