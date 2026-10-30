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

import hashlib
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from snowflake.cli._plugins.upgrade import lock as lock_mod
from snowflake.cli._plugins.upgrade import machine as machine_mod
from snowflake.cli._plugins.upgrade.layout import (
    MACHINE_ID_NAME,
    MANAGED_HOME_ENV,
    UPGRADE_LOCK_NAME,
    ManagedLayout,
)
from snowflake.cli._plugins.upgrade.lock import try_upgrade_lock
from snowflake.cli._plugins.upgrade.machine import (
    get_or_create_machine_id,
    rollout_score,
)
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS


@pytest.fixture
def managed_home(tmp_path, monkeypatch):
    root = tmp_path / "snowflake-cli"
    monkeypatch.setenv(MANAGED_HOME_ENV, str(root))
    return root


def _write_fake_binary(path: Path, marker: str) -> Path:
    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
    SecurePath(path).write_text(f"#!/bin/sh\necho {marker}\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def test_machine_id_stable_across_calls(managed_home):
    layout = ManagedLayout()
    first = get_or_create_machine_id(layout)
    second = get_or_create_machine_id(layout)
    assert first == second
    stored = layout.machine_id_path.read_text(encoding="utf-8").strip()
    assert stored == first
    uuid.UUID(first)


@pytest.mark.skipif(IS_WINDOWS, reason="POSIX mode bits")
def test_machine_id_mode_0600(managed_home):
    layout = ManagedLayout()
    get_or_create_machine_id(layout)
    mode = layout.machine_id_path.stat().st_mode & 0o777
    assert mode == 0o600


def test_rollout_score_in_range_and_stable(managed_home):
    layout = ManagedLayout()
    machine_id = get_or_create_machine_id(layout)
    score = rollout_score(machine_id)
    assert 0.0 <= score < 1.0
    assert rollout_score(machine_id) == score
    assert rollout_score(uuid.UUID(machine_id)) == score


def test_rollout_score_matches_spec_formula():
    machine_id = uuid.UUID("00000000-0000-4000-8000-000000000000")
    n = int.from_bytes(hashlib.sha256(machine_id.bytes).digest()[:8], "big") >> 11
    expected = n / float(1 << 53)
    assert 0.0 <= expected < 1.0
    assert rollout_score(machine_id) == expected
    assert rollout_score(str(machine_id)) == expected


def test_corrupt_machine_id_is_replaced(managed_home, monkeypatch):
    monkeypatch.setattr(machine_mod, "_MACHINE_ID_WAIT_SECONDS", 0.05)
    monkeypatch.setattr(machine_mod, "_MACHINE_ID_POLL_SECONDS", 0.01)
    layout = ManagedLayout()
    SecurePath(layout.root).mkdir(parents=True, exist_ok=True)
    SecurePath(layout.machine_id_path).write_text("not-a-uuid\n", encoding="utf-8")
    machine_id = get_or_create_machine_id(layout)
    uuid.UUID(machine_id)
    assert layout.machine_id_path.read_text(encoding="utf-8").strip() == machine_id


def test_empty_machine_id_replaced_after_wait(managed_home, monkeypatch):
    monkeypatch.setattr(machine_mod, "_MACHINE_ID_WAIT_SECONDS", 0.05)
    monkeypatch.setattr(machine_mod, "_MACHINE_ID_POLL_SECONDS", 0.01)
    layout = ManagedLayout()
    SecurePath(layout.root).mkdir(parents=True, exist_ok=True)
    layout.machine_id_path.touch()
    machine_id = get_or_create_machine_id(layout)
    uuid.UUID(machine_id)
    assert layout.machine_id_path.read_text(encoding="utf-8").strip() == machine_id


def test_first_create_loser_adopts_winner_id(managed_home):
    layout = ManagedLayout()
    SecurePath(layout.root).mkdir(parents=True, exist_ok=True)
    winner_id = "11111111-1111-4111-8111-111111111111"
    layout.machine_id_path.touch()

    def write_winner() -> None:
        time.sleep(0.05)
        layout.machine_id_path.write_text(f"{winner_id}\n", encoding="utf-8")

    writer = threading.Thread(target=write_winner)
    writer.start()
    got = get_or_create_machine_id(layout)
    writer.join()
    assert got == winner_id
    assert layout.machine_id_path.read_text(encoding="utf-8").strip() == winner_id


def test_concurrent_first_create_agrees_on_one_id(managed_home):
    layout = ManagedLayout()
    workers = 8
    with ThreadPoolExecutor(max_workers=workers) as pool:
        ids = list(pool.map(lambda _: get_or_create_machine_id(layout), range(workers)))
    assert len(set(ids)) == 1
    stored = layout.machine_id_path.read_text(encoding="utf-8").strip()
    assert stored == ids[0]
    uuid.UUID(ids[0])


@pytest.mark.skipif(IS_WINDOWS, reason="POSIX mode bits")
def test_unix_lock_file_mode_0600(managed_home):
    layout = ManagedLayout()
    with try_upgrade_lock(layout) as acquired:
        assert acquired is True
        mode = layout.upgrade_lock_path.stat().st_mode & 0o777
        assert mode == 0o600


@pytest.mark.skipif(IS_WINDOWS, reason="Unix fcntl flock")
def test_second_lock_fails_immediately(managed_home):
    layout = ManagedLayout()
    with try_upgrade_lock(layout) as first:
        assert first is True
        with try_upgrade_lock(layout) as second:
            assert second is False
    with try_upgrade_lock(layout) as after:
        assert after is True


@pytest.mark.skipif(IS_WINDOWS, reason="Unix fcntl flock")
def test_lock_released_on_exception(managed_home):
    layout = ManagedLayout()
    with pytest.raises(RuntimeError, match="boom"):
        with try_upgrade_lock(layout) as acquired:
            assert acquired is True
            raise RuntimeError("boom")
    with try_upgrade_lock(layout) as acquired:
        assert acquired is True


def test_windows_mutex_busy(managed_home, monkeypatch):
    monkeypatch.setattr(lock_mod, "_is_windows", lambda: True)
    monkeypatch.setattr(lock_mod, "_try_windows_mutex", lambda path: (None, False))
    layout = ManagedLayout()
    with try_upgrade_lock(layout) as acquired:
        assert acquired is False


def test_windows_mutex_released_on_exception(managed_home, monkeypatch):
    released = []
    handle = 42
    monkeypatch.setattr(lock_mod, "_is_windows", lambda: True)
    monkeypatch.setattr(lock_mod, "_try_windows_mutex", lambda path: (handle, True))
    monkeypatch.setattr(lock_mod, "_release_windows_mutex", released.append)
    layout = ManagedLayout()
    with pytest.raises(RuntimeError, match="boom"):
        with try_upgrade_lock(layout) as acquired:
            assert acquired is True
            raise RuntimeError("boom")
    assert released == [handle]


def test_windows_mutex_name_is_local_and_path_scoped(tmp_path):
    first = lock_mod._mutex_name(tmp_path / "a" / UPGRADE_LOCK_NAME)  # noqa: SLF001
    second = lock_mod._mutex_name(tmp_path / "b" / UPGRADE_LOCK_NAME)  # noqa: SLF001
    assert first.startswith("Local\\SnowflakeCliUpgradeLock-")
    assert second.startswith("Local\\SnowflakeCliUpgradeLock-")
    assert first != second


def test_gc_keeps_machine_id_and_lock_and_only_current_previous_versions(managed_home):
    layout = ManagedLayout()
    machine_id = get_or_create_machine_id(layout)
    with try_upgrade_lock(layout) as acquired:
        assert acquired is True

    fake_a = _write_fake_binary(managed_home / "src-a", "3.12.0")
    fake_b = _write_fake_binary(managed_home / "src-b", "3.13.0")
    fake_c = _write_fake_binary(managed_home / "src-c", "3.14.0")
    layout.install_binary("3.12.0", fake_a)
    layout.retarget("3.12.0")
    layout.install_binary("3.13.0", fake_b)
    layout.retarget("3.13.0")
    layout.install_binary("3.14.0", fake_c)
    layout.retarget("3.14.0")

    removed = layout.gc()
    assert layout.version_dir("3.12.0") in removed
    assert not layout.version_dir("3.12.0").exists()
    assert layout.version_dir("3.13.0").is_dir()
    assert layout.version_dir("3.14.0").is_dir()
    assert layout.machine_id_path.is_file()
    assert layout.machine_id_path.read_text(encoding="utf-8").strip() == machine_id
    assert MACHINE_ID_NAME not in {p.name for p in removed}
    assert UPGRADE_LOCK_NAME not in {p.name for p in removed}
    assert layout.upgrade_lock_path.exists()
