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

import pytest
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._app.telemetry import CLITelemetryClient
from snowflake.cli._plugins.upgrade import manager
from snowflake.cli._plugins.upgrade.layout import MANAGED_HOME_ENV, ManagedLayout
from snowflake.cli._plugins.upgrade.manager import (
    STATUS_ALREADY_CURRENT,
    STATUS_NEW_MAJOR,
    STATUS_REFUSED,
    STATUS_REVERTED,
    STATUS_UPGRADED,
    UnimplementedVersionSource,
)
from snowflake.cli._plugins.upgrade.telemetry import (
    SPOOL_LIMIT,
    TRIGGER_MANUAL,
    append_spool_record,
    drop_spool_events,
    read_spool_events,
    read_spool_records,
    spool_path,
)
from snowflake.cli.api.constants import TELEMETRY_PENDING_LIMIT
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS


class _FixedVersionSource:
    def __init__(self, version: str) -> None:
        self._version = version

    def latest_version(self) -> str:
        return self._version


class _MaterializingSource:
    def __init__(self, version: str, payload: Path) -> None:
        self._version = version
        self._payload = payload

    def latest_version(self) -> str:
        return self._version

    def materialize(self, version: str, layout: ManagedLayout) -> None:
        layout.install_binary(version, self._payload)


@pytest.fixture(autouse=True)
def _reset_version_source():
    manager.set_version_source(UnimplementedVersionSource())
    yield
    manager.set_version_source(UnimplementedVersionSource())


@pytest.fixture
def managed_home(tmp_path, monkeypatch):
    root = tmp_path / "snowflake-cli"
    monkeypatch.setenv(MANAGED_HOME_ENV, str(root))
    return root


@pytest.fixture
def no_telemetry_dial(monkeypatch):
    monkeypatch.setattr(
        "snowflake.cli._app.telemetry._telemetry_may_open_connection",
        lambda: False,
    )


def _write_fake_binary(path: Path, version: str) -> Path:
    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
    if IS_WINDOWS:
        SecurePath(path).write_text(f"@echo off\necho {version}\n")
    else:
        SecurePath(path).write_text(f"#!/bin/sh\necho {version}\n")
        path.chmod(0o755)
    return path


def _seed_current(managed_home: Path, version: str) -> ManagedLayout:
    layout = ManagedLayout(managed_home)
    source = managed_home.parent / f"src-{version}"
    _write_fake_binary(source, version)
    layout.install_binary(version, source)
    layout.retarget(version)
    return layout


def _enable_managed(monkeypatch, version: str = "3.12.0") -> None:
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", version)


def _upgrade_messages(managed_home: Path) -> list[dict]:
    records = read_spool_records(ManagedLayout(managed_home))
    return [record["message"] for record in records]


def _assert_no_machine_id(payload: dict) -> None:
    dumped = json.dumps(payload)
    assert "machine_id" not in dumped
    assert "machine-id" not in dumped


def test_refuse_without_connection_leaves_spool(
    runner, managed_home, no_telemetry_dial
):
    result = runner.invoke(["upgrade"])
    assert result.exit_code == 0, result.output
    messages = _upgrade_messages(managed_home)
    assert len(messages) == 1
    message = messages[0]
    assert message["upgrade_trigger"] == TRIGGER_MANUAL
    assert message["upgrade_status"] == STATUS_REFUSED
    assert isinstance(message["upgrade_duration_ms"], int)
    assert "command_ci_environment" in message
    _assert_no_machine_id(message)
    assert spool_path(ManagedLayout(managed_home)).is_file()


def test_already_current_and_new_major_are_recorded(
    runner, monkeypatch, managed_home, no_telemetry_dial
):
    _enable_managed(monkeypatch, "3.12.0")
    manager.set_version_source(_FixedVersionSource("3.12.0"))
    current = runner.invoke(["upgrade"])
    assert current.exit_code == 0, current.output
    current_msg = _upgrade_messages(managed_home)[-1]
    assert current_msg["upgrade_status"] == STATUS_ALREADY_CURRENT
    assert current_msg["upgrade_from"] == "3.12.0"
    assert current_msg["upgrade_to"] == "3.12.0"

    manager.set_version_source(_FixedVersionSource("4.0.0"))
    major = runner.invoke(["upgrade"])
    assert major.exit_code == 0, major.output
    statuses = [msg["upgrade_status"] for msg in _upgrade_messages(managed_home)]
    assert STATUS_ALREADY_CURRENT in statuses
    assert STATUS_NEW_MAJOR in statuses
    major_msg = _upgrade_messages(managed_home)[-1]
    assert major_msg["upgrade_status"] == STATUS_NEW_MAJOR
    assert major_msg["upgrade_from"] == "3.12.0"
    assert major_msg["upgrade_to"] == "4.0.0"
    _assert_no_machine_id(major_msg)


def test_upgraded_and_reverted_are_recorded(
    runner, monkeypatch, managed_home, tmp_path, no_telemetry_dial
):
    _enable_managed(monkeypatch, "3.12.0")
    _seed_current(managed_home, "3.12.0")
    payload = tmp_path / "src-3.13.1"
    _write_fake_binary(payload, "3.13.1")
    manager.set_version_source(_MaterializingSource("3.13.1", payload))

    upgraded = runner.invoke(["upgrade"])
    assert upgraded.exit_code == 0, upgraded.output
    upgraded_msg = _upgrade_messages(managed_home)[-1]
    assert upgraded_msg["upgrade_trigger"] == TRIGGER_MANUAL
    assert upgraded_msg["upgrade_status"] == STATUS_UPGRADED
    assert upgraded_msg["upgrade_from"] == "3.12.0"
    assert upgraded_msg["upgrade_to"] == "3.13.1"

    monkeypatch.setattr(__about__, "VERSION", "3.13.1")
    reverted = runner.invoke(["upgrade", "--revert"])
    assert reverted.exit_code == 0, reverted.output
    reverted_msg = _upgrade_messages(managed_home)[-1]
    assert reverted_msg["upgrade_status"] == STATUS_REVERTED
    assert reverted_msg["upgrade_from"] == "3.13.1"
    assert reverted_msg["upgrade_to"] == "3.12.0"
    _assert_no_machine_id(reverted_msg)


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.connection.commands.ObjectManager")
def test_later_connected_command_drains_spool(
    _object_manager, mock_conn, runner, monkeypatch, managed_home, no_telemetry_dial
):
    result = runner.invoke(["upgrade"])
    assert result.exit_code == 0, result.output
    assert read_spool_records(ManagedLayout(managed_home))
    mock_conn.return_value._telemetry.try_add_log_to_batch.reset_mock()  # noqa: SLF001

    monkeypatch.setattr(
        "snowflake.cli._app.telemetry._telemetry_may_open_connection",
        lambda: True,
    )
    connected = runner.invoke(["connection", "test"], catch_exceptions=False)
    assert connected.exit_code == 0, connected.output
    assert read_spool_records(ManagedLayout(managed_home)) == []

    logged = [
        call.args[0].to_dict()["message"]
        for call in mock_conn.return_value._telemetry.try_add_log_to_batch.call_args_list  # noqa: SLF001
    ]
    upgrade_events = [
        msg for msg in logged if msg.get("upgrade_trigger") == TRIGGER_MANUAL
    ]
    assert len(upgrade_events) == 1
    assert upgrade_events[0]["upgrade_status"] == STATUS_REFUSED
    assert "command_ci_environment" in upgrade_events[0]
    _assert_no_machine_id(upgrade_events[0])


def test_spool_drops_oldest_when_over_limit(managed_home):
    layout = ManagedLayout(managed_home)
    for i in range(SPOOL_LIMIT + 5):
        append_spool_record(
            {"message": {"n": i}, "timestamp": i},
            layout=layout,
        )
    records = read_spool_records(layout)
    assert len(records) == SPOOL_LIMIT
    assert records[0]["message"]["n"] == 5
    assert records[-1]["message"]["n"] == SPOOL_LIMIT + 4
    if not IS_WINDOWS:
        mode = spool_path(layout).stat().st_mode & 0o777
        assert mode == 0o600


def test_spool_limit_is_shared_pending_limit():
    assert SPOOL_LIMIT == TELEMETRY_PENDING_LIMIT


def test_drop_spool_events_keeps_records_appended_after_read(managed_home):
    layout = ManagedLayout(managed_home)
    append_spool_record({"message": {"n": 1}, "timestamp": 1}, layout=layout)
    append_spool_record({"message": {"n": 2}, "timestamp": 2}, layout=layout)
    events = read_spool_events(layout)
    append_spool_record({"message": {"n": 3}, "timestamp": 3}, layout=layout)
    drop_spool_events(events, layout=layout)
    records = read_spool_records(layout)
    assert [record["message"]["n"] for record in records] == [3]


def test_flush_keeps_spool_when_send_batch_fails(managed_home):
    layout = ManagedLayout(managed_home)
    append_spool_record(
        {"message": {"upgrade_trigger": TRIGGER_MANUAL}, "timestamp": 1},
        layout=layout,
    )
    client = CLITelemetryClient()
    channel = mock.Mock()
    channel.send_batch.side_effect = RuntimeError("upload failed")
    with mock.patch.object(
        CLITelemetryClient, "_telemetry", new_callable=mock.PropertyMock
    ) as telemetry_prop:
        telemetry_prop.return_value = channel
        with pytest.raises(RuntimeError, match="upload failed"):
            client.flush()
    assert read_spool_records(layout)
    channel.try_add_log_to_batch.assert_called()


def test_flush_keeps_spool_when_try_add_log_to_batch_fails(managed_home):
    layout = ManagedLayout(managed_home)
    append_spool_record(
        {"message": {"upgrade_trigger": TRIGGER_MANUAL}, "timestamp": 1},
        layout=layout,
    )
    client = CLITelemetryClient()
    channel = mock.Mock()
    channel.try_add_log_to_batch.side_effect = RuntimeError("batch fail")
    with mock.patch.object(
        CLITelemetryClient, "_telemetry", new_callable=mock.PropertyMock
    ) as telemetry_prop:
        telemetry_prop.return_value = channel
        client.flush()
    assert read_spool_records(layout)
    channel.send_batch.assert_called_once()
