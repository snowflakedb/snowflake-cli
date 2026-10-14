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

import inspect
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._plugins.upgrade import auto, manager
from snowflake.cli._plugins.upgrade.auto import (
    CHECK_TIMEOUT_SECONDS,
    DOWNLOAD_TIMEOUT_SECONDS,
    FailedOpen,
    Skip,
    Upgraded,
    maybe_auto_upgrade,
)
from snowflake.cli._plugins.upgrade.controls import AUTO_UPGRADE_DONE_ENV
from snowflake.cli._plugins.upgrade.layout import (
    MANAGED_HOME_ENV,
    ManagedLayout,
    read_reverted_from,
    write_reverted_from,
)
from snowflake.cli._plugins.upgrade.lock import try_upgrade_lock
from snowflake.cli._plugins.upgrade.manager import UnimplementedVersionSource
from snowflake.cli._plugins.upgrade.repo import (
    DOWNLOAD_TIMEOUT_SECONDS as MANUAL_DOWNLOAD_TIMEOUT_SECONDS,
)
from snowflake.cli._plugins.upgrade.repo import (
    POINTER_TIMEOUT_SECONDS,
    HttpRepo,
)
from snowflake.cli._plugins.upgrade.rollout import (
    HOLD,
    IMMEDIATE,
    PublishedRelease,
    Rollout,
    RolloutPolicy,
)
from snowflake.cli._plugins.upgrade.telemetry import TRIGGER_AUTO, read_spool_records
from snowflake.cli._plugins.upgrade.trust import INVALID_SIGNATURE_MESSAGE
from snowflake.cli.api.config import (
    AUTO_UPGRADE_ALLOW_MAJOR_KEY,
    AUTO_UPGRADE_KEY,
    CLI_SECTION,
    config_init,
    get_env_variable_name,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS
from tests_common.feature_flag_utils import with_feature_flags

AUTO_UPGRADE_ENV = get_env_variable_name(CLI_SECTION, key=AUTO_UPGRADE_KEY)
ALLOW_MAJOR_ENV = get_env_variable_name(CLI_SECTION, key=AUTO_UPGRADE_ALLOW_MAJOR_KEY)


class _ReleaseSource:
    def __init__(self, version: str, rollout=IMMEDIATE, payload: Path | None = None):
        self._version = version
        self._rollout = rollout
        self._payload = payload
        self.release_calls = 0
        self.materialize_calls = 0

    def latest_release(self):
        self.release_calls += 1
        return PublishedRelease(version=self._version, rollout=self._rollout)

    def latest_version(self) -> str:
        return self._version

    def materialize(self, version: str, layout: ManagedLayout) -> None:
        self.materialize_calls += 1
        if self._payload is None:
            raise AssertionError("materialize should not run")
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
def managed(monkeypatch, managed_home):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    monkeypatch.delenv(AUTO_UPGRADE_DONE_ENV, raising=False)
    monkeypatch.delenv(AUTO_UPGRADE_ENV, raising=False)
    monkeypatch.delenv(ALLOW_MAJOR_ENV, raising=False)
    return managed_home


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


def _seed(root: Path, version: str, binary: Path) -> ManagedLayout:
    layout = ManagedLayout(root)
    layout.install_binary(version, binary)
    layout.retarget(version)
    return layout


def _messages(root: Path) -> list[dict]:
    return [record["message"] for record in read_spool_records(ManagedLayout(root))]


def _assert_no_machine_id(root: Path, payload: dict) -> None:
    machine_id_path = root / ".machine-id"
    dumped = str(payload)
    assert "machine_id" not in dumped
    assert "machine-id" not in dumped
    if machine_id_path.is_file():
        machine_id = machine_id_path.read_text(encoding="utf-8").strip()
        assert machine_id not in ManagedLayout(root).root.joinpath(
            ".telemetry-spool.jsonl"
        ).read_text(encoding="utf-8")


def test_not_direct_install_skips_without_spool_or_download(
    monkeypatch, managed_home, capsys
):
    monkeypatch.setattr(__about__, "INSTALLATION_SOURCE", CLIInstallationSource.PYPI)
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "not_direct_install"
    assert source.release_calls == 0
    assert source.materialize_calls == 0
    assert _messages(managed_home) == []
    assert capsys.readouterr().out == ""


def test_flag_off(monkeypatch, managed, capsys):
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "1")
    action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "flag_off"
    assert source.release_calls == 0
    message = _messages(managed)[-1]
    assert message["upgrade_trigger"] == TRIGGER_AUTO
    assert message["upgrade_skip_reason"] == "flag_off"
    assert message["upgrade_status"] == "skipped"
    assert capsys.readouterr().out == ""


def test_config_off(monkeypatch, managed):
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "config_off"
    assert source.release_calls == 0
    assert _messages(managed)[-1]["upgrade_skip_reason"] == "config_off"


@pytest.mark.parametrize("raw", ["yes", "on", "YES"])
def test_env_yes_and_on_count_as_opt_in(monkeypatch, managed, raw):
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    monkeypatch.setenv(AUTO_UPGRADE_ENV, raw)
    action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "flag_off"
    assert source.release_calls == 0


def test_env_unrecognized_is_off_not_fail_open(monkeypatch, managed):
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "maybe")
    action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "env_off"
    assert source.release_calls == 0


def test_env_off_overrides_config_on(monkeypatch, managed, config_file):
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    monkeypatch.setenv(AUTO_UPGRADE_ENV, "0")
    with config_file(f"[cli]\n{AUTO_UPGRADE_KEY} = true\n") as cfg:
        config_init(cfg)
        with with_feature_flags({FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE: True}):
            action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "env_off"
    assert source.release_calls == 0


def test_cli_flag(monkeypatch, managed):
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["--no-auto-upgrade", "sql"])
    assert isinstance(action, Skip)
    assert action.reason == "cli_flag"
    assert source.release_calls == 0


def test_already_done(monkeypatch, managed):
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    monkeypatch.setenv(AUTO_UPGRADE_DONE_ENV, "1")
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "already_done"
    assert source.release_calls == 0


@pytest.mark.parametrize(
    "argv",
    [
        ["--help"],
        ["-h"],
        ["--version"],
        ["--version=3.13.0"],
        ["app", "publish", "--version", "v1"],
        ["app", "publish", "--version=v1"],
        ["--info"],
        ["--docs"],
        ["--structure"],
        ["upgrade"],
        ["upgrade", "--dry-run"],
        ["sql", "--help"],
    ],
)
def test_meta_command(monkeypatch, managed, argv):
    source = _ReleaseSource("3.13.0")
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(argv)
    assert isinstance(action, Skip)
    assert action.reason == "meta_command"
    assert source.release_calls == 0
    assert source.materialize_calls == 0


def test_removed_docs_pages_flag_still_upgrades(monkeypatch, managed, tmp_path):
    source = _ReleaseSource("3.13.0", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["--docs-pages"])
    assert isinstance(action, Upgraded)
    assert source.materialize_calls == 1


def test_version_prefix_is_not_meta(monkeypatch, managed, tmp_path):
    source = _ReleaseSource("3.13.0", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["app", "publish", "--version-id", "v1"])
    assert isinstance(action, Upgraded)
    assert source.materialize_calls == 1


@pytest.mark.parametrize("step", ["gc", "clear"])
def test_cleanup_after_retarget_does_not_fail_the_upgrade(
    monkeypatch, managed, tmp_path, step
):
    binary = _binary(tmp_path / "current")
    layout = _seed(managed, "3.12.0", binary)
    write_reverted_from(layout, "3.11.0")
    source = _ReleaseSource("3.13.0", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    if step == "gc":

        def boom_gc(self):
            raise CliError("gc failed")

        monkeypatch.setattr(ManagedLayout, "gc", boom_gc)
    else:

        def boom_clear(_layout):
            raise CliError("clear failed")

        monkeypatch.setattr(manager, "clear_reverted_from", boom_clear)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Upgraded)
    assert action.decision.payload["status"] == "upgraded"
    assert layout.current_version() == "3.13.0"
    assert _messages(managed)[-1]["upgrade_status"] == "upgraded"
    if step == "gc":
        assert read_reverted_from(layout) is None
    else:
        assert read_reverted_from(layout) == "3.11.0"


def test_lock_busy(monkeypatch, managed):
    source = _ReleaseSource("3.13.0", payload=Path("unused"))
    manager.set_version_source(source)
    layout = ManagedLayout(managed)
    with try_upgrade_lock(layout) as acquired:
        assert acquired is True
        with _enable(monkeypatch):
            action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, FailedOpen)
    assert action.reason == "lock_busy"
    assert source.release_calls == 0
    assert _messages(managed)[-1]["upgrade_skip_reason"] == "lock_busy"


def test_hold_does_not_download(monkeypatch, managed, tmp_path):
    source = _ReleaseSource("3.13.0", rollout=HOLD, payload=tmp_path / "bin")
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "hold"
    assert source.release_calls == 1
    assert source.materialize_calls == 0
    message = _messages(managed)[-1]
    assert message["upgrade_skip_reason"] == "hold"
    assert isinstance(message["upgrade_rollout_score"], float)
    assert 0 <= message["upgrade_rollout_score"] < 1
    _assert_no_machine_id(managed, message)


def test_not_in_slice_does_not_download(monkeypatch, managed, tmp_path):
    rollout = Rollout(
        policy=RolloutPolicy.STAGED,
        released_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    source = _ReleaseSource("3.13.0", rollout=rollout, payload=tmp_path / "bin")
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "not_in_slice"
    assert source.materialize_calls == 0


def test_reverted_version_equal_or_older_does_not_download(
    monkeypatch, managed, tmp_path
):
    binary = _binary(tmp_path / "current")
    layout = _seed(managed, "3.12.0", binary)
    write_reverted_from(layout, "3.13.0")
    source = _ReleaseSource("3.13.0", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        equal = maybe_auto_upgrade(["sql"])
    assert isinstance(equal, Skip)
    assert equal.reason == "reverted_version"
    assert source.materialize_calls == 0
    assert read_reverted_from(layout) == "3.13.0"

    source_older = _ReleaseSource("3.12.1", payload=_binary(tmp_path / "older"))
    manager.set_version_source(source_older)
    with _enable(monkeypatch):
        older = maybe_auto_upgrade(["sql"])
    assert isinstance(older, Skip)
    assert older.reason == "reverted_version"
    assert source_older.materialize_calls == 0


def test_pointer_newer_than_reverted_stamp_upgrades_and_clears_it(
    monkeypatch, managed, tmp_path, capsys
):
    binary = _binary(tmp_path / "current")
    layout = _seed(managed, "3.12.0", binary)
    write_reverted_from(layout, "3.13.0")
    source = _ReleaseSource("3.13.1", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Upgraded)
    assert action.decision.payload["status"] == "upgraded"
    assert action.decision.payload["to"] == "3.13.1"
    assert source.materialize_calls == 1
    assert read_reverted_from(layout) is None
    assert layout.current_version() == "3.13.1"
    assert capsys.readouterr().out == ""
    message = _messages(managed)[-1]
    assert message["upgrade_trigger"] == TRIGGER_AUTO
    assert message["upgrade_status"] == "upgraded"
    assert "upgrade_skip_reason" not in message
    _assert_no_machine_id(managed, message)


def test_new_major_without_opt_in_does_not_download(monkeypatch, managed, tmp_path):
    source = _ReleaseSource("4.0.0", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "new_major"
    assert source.materialize_calls == 0
    message = _messages(managed)[-1]
    assert message["upgrade_status"] == "new_major"
    assert message["upgrade_skip_reason"] == "new_major"
    assert message["upgrade_from"] == "3.12.0"
    assert message["upgrade_to"] == "4.0.0"


def test_new_major_with_opt_in_upgrades(monkeypatch, managed, tmp_path):
    monkeypatch.setenv(ALLOW_MAJOR_ENV, "1")
    source = _ReleaseSource("4.0.0", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Upgraded)
    assert action.decision.payload["to"] == "4.0.0"
    assert source.materialize_calls == 1


def test_already_current(monkeypatch, managed, tmp_path):
    source = _ReleaseSource("3.12.0", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Skip)
    assert action.reason == "already_current"
    assert source.materialize_calls == 0
    assert _messages(managed)[-1]["upgrade_status"] == "already_current"


def test_check_timeout_by_patched_sleep_does_not_download(
    monkeypatch, managed, tmp_path
):
    clock = {"now": 0.0}

    def monotonic():
        return clock["now"]

    def sleep(seconds):
        clock["now"] += seconds

    monkeypatch.setattr(time, "monotonic", monotonic)
    monkeypatch.setattr(time, "sleep", sleep)
    source = _ReleaseSource("3.13.0", payload=_binary(tmp_path / "next"))

    def latest_release(self=source):
        source.release_calls += 1
        time.sleep(CHECK_TIMEOUT_SECONDS + 1)
        return PublishedRelease(version="3.13.0", rollout=IMMEDIATE)

    source.latest_release = latest_release  # type: ignore[method-assign]
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, FailedOpen)
    assert action.reason == "timeout"
    assert source.materialize_calls == 0
    assert _messages(managed)[-1]["upgrade_skip_reason"] == "timeout"


def test_download_timeout_by_patched_sleep_does_not_retarget(
    monkeypatch, managed, tmp_path
):
    clock = {"now": 0.0}

    def monotonic():
        return clock["now"]

    def sleep(seconds):
        clock["now"] += seconds

    monkeypatch.setattr(time, "monotonic", monotonic)
    monkeypatch.setattr(time, "sleep", sleep)
    binary = _binary(tmp_path / "current")
    layout = _seed(managed, "3.12.0", binary)
    payload = _binary(tmp_path / "next")
    source = _ReleaseSource("3.13.0", payload=payload)

    def materialize(version, layout):
        time.sleep(DOWNLOAD_TIMEOUT_SECONDS + 1)
        source.materialize_calls += 1
        layout.install_binary(version, payload)

    source.materialize = materialize  # type: ignore[method-assign]
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, FailedOpen)
    assert action.reason == "timeout"
    assert source.materialize_calls == 1
    assert layout.current_version() == "3.12.0"


def test_http_timeout_fail_open(monkeypatch, managed):
    source = _ReleaseSource("3.13.0")

    def latest_release():
        source.release_calls += 1
        raise requests.Timeout("check exceeded")

    source.latest_release = latest_release  # type: ignore[method-assign]
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, FailedOpen)
    assert action.reason == "timeout"
    assert source.materialize_calls == 0


def test_network_fail_open(monkeypatch, managed):
    source = _ReleaseSource("3.13.0")

    def latest_release():
        raise requests.ConnectionError("proxy")

    source.latest_release = latest_release  # type: ignore[method-assign]
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, FailedOpen)
    assert action.reason == "network"
    assert source.materialize_calls == 0


@pytest.mark.parametrize(
    "message",
    [
        INVALID_SIGNATURE_MESSAGE,
        "Checksum mismatch for snow.tar.gz: expected aa, got bb. Refusing to install.",
    ],
)
def test_integrity_fail_open_does_not_retarget(monkeypatch, managed, tmp_path, message):
    binary = _binary(tmp_path / "current")
    layout = _seed(managed, "3.12.0", binary)
    source = _ReleaseSource("3.13.0", payload=_binary(tmp_path / "next"))
    if "Checksum" in message:

        def materialize(version, layout):
            source.materialize_calls += 1
            raise CliError(message)

        source.materialize = materialize  # type: ignore[method-assign]
    else:

        def latest_release():
            source.release_calls += 1
            raise CliError(message)

        source.latest_release = latest_release  # type: ignore[method-assign]
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, FailedOpen)
    assert action.reason == "integrity"
    assert layout.current_version() == "3.12.0"


def test_unexpected_error_fail_open(monkeypatch, managed):
    source = _ReleaseSource("3.13.0")

    def latest_release():
        raise RuntimeError("boom")

    source.latest_release = latest_release  # type: ignore[method-assign]
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, FailedOpen)
    assert action.reason == "error"
    assert source.materialize_calls == 0


@pytest.mark.parametrize(
    "argv",
    [
        ["--format", "json", "sql", "-q", "select 1"],
        [],
    ],
)
def test_json_and_repl_still_upgrade(monkeypatch, managed, tmp_path, argv):
    source = _ReleaseSource("3.13.0", payload=_binary(tmp_path / f"next-{len(argv)}"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(argv)
    assert isinstance(action, Upgraded)
    assert source.materialize_calls == 1


def test_reuses_evaluate_managed_upgrade(monkeypatch, managed, tmp_path):
    assert "plan_upgrade" not in inspect.getsource(auto)
    seen = {}
    real = auto.evaluate_managed_upgrade

    def spy(*, dry_run: bool, allow_major: bool = False):
        seen["dry_run"] = dry_run
        seen["allow_major"] = allow_major
        return real(dry_run=dry_run, allow_major=allow_major)

    monkeypatch.setattr(auto, "evaluate_managed_upgrade", spy)
    source = _ReleaseSource("3.13.0", payload=_binary(tmp_path / "next"))
    manager.set_version_source(source)
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, Upgraded)
    assert seen == {"dry_run": False, "allow_major": False}


def test_auto_tightens_http_timeouts_and_restores_them(monkeypatch, managed):
    repo = HttpRepo(base_url="https://sfc-repo.snowflakecomputing.com/snowflake-cli")
    seen = {}

    def latest_release(self):
        seen["pointer"] = self.pointer_timeout
        seen["download"] = self.download_timeout
        seen["deadline"] = self.check_deadline
        raise requests.Timeout("check")

    monkeypatch.setattr(HttpRepo, "latest_release", latest_release)
    manager.set_version_source(repo)
    started = time.monotonic()
    with _enable(monkeypatch):
        action = maybe_auto_upgrade(["sql"])
    assert isinstance(action, FailedOpen)
    assert action.reason == "timeout"
    assert seen["pointer"] == CHECK_TIMEOUT_SECONDS
    assert seen["download"] == DOWNLOAD_TIMEOUT_SECONDS
    assert started <= seen["deadline"] <= started + CHECK_TIMEOUT_SECONDS + 5
    assert repo.pointer_timeout == POINTER_TIMEOUT_SECONDS
    assert repo.download_timeout == MANUAL_DOWNLOAD_TIMEOUT_SECONDS
    assert repo.check_deadline is None
