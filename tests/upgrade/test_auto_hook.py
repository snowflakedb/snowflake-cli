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

import sys
from pathlib import Path

import pytest
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._app.main_typer import SnowCliMainTyper
from snowflake.cli._plugins.upgrade import manager, spawn
from snowflake.cli._plugins.upgrade.controls import AUTO_UPGRADE_DONE_ENV
from snowflake.cli._plugins.upgrade.layout import MANAGED_HOME_ENV, ManagedLayout
from snowflake.cli._plugins.upgrade.repo import HttpRepo
from snowflake.cli._plugins.upgrade.rollout import IMMEDIATE, PublishedRelease
from snowflake.cli.api.config import (
    AUTO_UPGRADE_KEY,
    CLI_SECTION,
    get_env_variable_name,
)
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS
from tests_common.feature_flag_utils import with_feature_flags

AUTO_UPGRADE_ENV = get_env_variable_name(CLI_SECTION, key=AUTO_UPGRADE_KEY)


class _ReleaseSource:
    def __init__(self, version: str, payload: Path):
        self._version = version
        self._payload = payload
        self.release_calls = 0
        self.materialize_calls = 0

    def latest_release(self):
        self.release_calls += 1
        return PublishedRelease(version=self._version, rollout=IMMEDIATE)

    def latest_version(self) -> str:
        return self._version

    def materialize(self, version: str, layout: ManagedLayout) -> None:
        self.materialize_calls += 1
        layout.install_binary(version, self._payload)


@pytest.fixture(autouse=True)
def _reset_version_source():
    manager.set_version_source(None)
    yield
    manager.set_version_source(None)


@pytest.fixture
def managed_home(tmp_path, monkeypatch):
    root = tmp_path / "snowflake-cli"
    monkeypatch.setenv(MANAGED_HOME_ENV, str(root))
    monkeypatch.delenv(AUTO_UPGRADE_DONE_ENV, raising=False)
    monkeypatch.delenv(AUTO_UPGRADE_ENV, raising=False)
    return root


def _forbid_upgrade_http(monkeypatch) -> dict:
    """Count repo calls. The hook fail-opens, so a raised error would be hidden."""
    calls = {"n": 0}

    def _boom(*_args, **_kwargs):
        calls["n"] += 1
        raise AssertionError("auto-upgrade contacted the repo")

    monkeypatch.setattr(HttpRepo, "latest_release", _boom)
    monkeypatch.setattr(HttpRepo, "latest_version", _boom)
    monkeypatch.setattr(HttpRepo, "materialize", _boom)
    return calls


def _forbid_spawn(monkeypatch) -> dict:
    calls = {"n": 0}

    def _boom(*_args, **_kwargs):
        calls["n"] += 1
        raise AssertionError("spawned")

    monkeypatch.setattr(spawn, "run_new_binary", _boom)
    return calls


def _app() -> tuple[SnowCliMainTyper, list[str]]:
    """Two commands so Typer stays a group.

    A single registered command is invoked directly, and ``["sql"]`` would be
    an extra argument instead of the subcommand name.
    """
    app = SnowCliMainTyper()
    ran: list[str] = []

    @app.command("sql")
    def sql() -> None:
        ran.append("sql")

    @app.command("object")
    def object_() -> None:
        ran.append("object")

    return app, ran


def _enable(monkeypatch):
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "1")
    return with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True})


def _binary(path: Path) -> Path:
    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
    if IS_WINDOWS:
        SecurePath(path).write_text("@echo off\n")
    else:
        SecurePath(path).write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o755)
    return path


def test_pypi_stamp_never_downloads(monkeypatch, managed_home):
    """Direct-install gate holds even when the product opt-in is on.

    The upgrade engine stays unloaded: a PyPI install must not import it.
    """
    sys.modules.pop("snowflake.cli._plugins.upgrade.auto", None)
    http = _forbid_upgrade_http(monkeypatch)
    spawned = _forbid_spawn(monkeypatch)
    monkeypatch.setattr(__about__, "INSTALLATION_SOURCE", CLIInstallationSource.PYPI)
    app, ran = _app()
    with _enable(monkeypatch):
        with pytest.raises(SystemExit) as caught:
            app(["sql"])
    assert caught.value.code in (0, None)
    assert ran == ["sql"]
    assert http["n"] == 0
    assert spawned["n"] == 0
    assert "snowflake.cli._plugins.upgrade.auto" not in sys.modules


def test_feature_flag_off_is_a_no_op(monkeypatch, managed_home):
    http = _forbid_upgrade_http(monkeypatch)
    spawned = _forbid_spawn(monkeypatch)
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "1")
    app, ran = _app()
    with pytest.raises(SystemExit) as caught:
        app(["sql"])
    assert caught.value.code in (0, None)
    assert ran == ["sql"]
    assert http["n"] == 0
    assert spawned["n"] == 0


@pytest.mark.parametrize(
    "argv",
    [
        ["--help"],
        ["-h"],
        ["--version"],
        ["--info"],
        ["--docs"],
        ["--structure"],
        ["upgrade"],
        ["sql", "--help"],
    ],
)
def test_meta_flags_never_download(monkeypatch, managed_home, argv):
    http = _forbid_upgrade_http(monkeypatch)
    spawned = _forbid_spawn(monkeypatch)
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    app, ran = _app()
    with _enable(monkeypatch):
        try:
            app(argv)
        except SystemExit:
            pass
    assert ran == []
    assert http["n"] == 0
    assert spawned["n"] == 0


def test_opted_in_managed_upgrades_then_spawns(monkeypatch, managed_home, tmp_path):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    source = _ReleaseSource("3.13.0", _binary(tmp_path / "next"))
    manager.set_version_source(source)
    spawned: dict = {}

    def fake_spawn(version, argv):
        spawned["version"] = version
        spawned["argv"] = list(argv)
        raise SystemExit(7)

    monkeypatch.setattr(spawn, "run_new_binary", fake_spawn)
    app, ran = _app()
    user_argv = ["sql", "--format", "json", "-q", "select 1"]
    with _enable(monkeypatch):
        with pytest.raises(SystemExit) as caught:
            app(user_argv)
    assert caught.value.code == 7
    assert source.release_calls == 1
    assert source.materialize_calls == 1
    assert spawned == {"version": "3.13.0", "argv": user_argv}
    assert ran == []


def test_agent_args_help_spawns_with_original_argv(monkeypatch, managed_home, tmp_path):
    """Agent ``--help`` is not a Snow meta skip, and the child sees that tail."""
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    source = _ReleaseSource("3.13.0", _binary(tmp_path / "next"))
    manager.set_version_source(source)
    spawned: dict = {}

    def fake_spawn(version, argv):
        spawned["version"] = version
        spawned["argv"] = list(argv)
        raise SystemExit(7)

    monkeypatch.setattr(spawn, "run_new_binary", fake_spawn)
    app, ran = _app()
    user_argv = ["ai", "claude", "--agent-args", "--help"]
    with _enable(monkeypatch):
        with pytest.raises(SystemExit) as caught:
            app(user_argv)
    assert caught.value.code == 7
    assert source.release_calls == 1
    assert spawned == {"version": "3.13.0", "argv": user_argv}
    assert ran == []


def test_spawn_error_fail_open_runs_original_command(
    monkeypatch, managed_home, tmp_path
):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    source = _ReleaseSource("3.13.0", _binary(tmp_path / "next"))
    manager.set_version_source(source)

    def fake_spawn(version, argv):
        raise FileNotFoundError(version)

    monkeypatch.setattr(spawn, "run_new_binary", fake_spawn)
    app, ran = _app()
    with _enable(monkeypatch):
        with pytest.raises(SystemExit) as caught:
            app(["sql"])
    assert caught.value.code in (0, None)
    assert ran == ["sql"]
    assert source.release_calls == 1


def test_main_entry_spawns_before_click(monkeypatch, managed_home, tmp_path):
    """``SnowCliMainTyper.main`` uses the same hook as ``__call__``."""
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    source = _ReleaseSource("3.13.0", _binary(tmp_path / "next"))
    manager.set_version_source(source)
    spawned: dict = {}

    def fake_spawn(version, argv):
        spawned["version"] = version
        spawned["argv"] = list(argv)
        raise SystemExit(7)

    monkeypatch.setattr(spawn, "run_new_binary", fake_spawn)
    app, ran = _app()
    user_argv = ["sql", "--format", "json"]
    with _enable(monkeypatch):
        with pytest.raises(SystemExit) as caught:
            app.main(args=user_argv)
    assert caught.value.code == 7
    assert spawned == {"version": "3.13.0", "argv": user_argv}
    assert ran == []
