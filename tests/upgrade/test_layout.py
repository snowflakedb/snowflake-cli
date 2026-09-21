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

import os
from pathlib import Path

import pytest
from snowflake.cli._plugins.upgrade import layout as layout_mod
from snowflake.cli._plugins.upgrade.layout import (
    MANAGED_HOME_ENV,
    ManagedLayout,
    posix_shim_contents,
    resolve_install_root,
    windows_cmd_contents,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS


def _write_fake_binary(path: Path, marker: str) -> Path:
    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
    SecurePath(path).write_text(f"#!/bin/sh\necho {marker}\n")
    path.chmod(0o755)
    return path


def _assert_shim_points_at(layout: ManagedLayout, installed: Path) -> None:
    leftover = list(layout.bin_dir.glob(".*.tmp"))
    assert leftover == []
    if IS_WINDOWS:
        assert not layout.unix_shim.is_symlink()
        assert layout.windows_cmd_shim.read_text() == windows_cmd_contents(installed)
        assert layout.unix_shim.read_text() == posix_shim_contents(installed)
        return
    assert layout.unix_shim.is_symlink()
    assert Path(os.readlink(layout.unix_shim)) == installed
    assert layout.unix_shim.resolve() == installed.resolve()


@pytest.fixture
def managed_home(tmp_path, monkeypatch):
    root = tmp_path / "snowflake-cli"
    monkeypatch.setenv(MANAGED_HOME_ENV, str(root))
    return root


def test_resolve_install_root_honours_override(tmp_path, monkeypatch):
    override = tmp_path / "custom-root"
    monkeypatch.setenv(MANAGED_HOME_ENV, str(override))
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    assert resolve_install_root() == override


def test_resolve_install_root_unix_xdg(tmp_path, monkeypatch):
    monkeypatch.delenv(MANAGED_HOME_ENV, raising=False)
    monkeypatch.setattr(layout_mod.platform, "system", lambda: "Linux")
    xdg = tmp_path / "xdg-data"
    monkeypatch.setenv("XDG_DATA_HOME", str(xdg))
    assert resolve_install_root() == xdg / "snowflake-cli"


def test_resolve_install_root_unix_default(tmp_path, monkeypatch):
    monkeypatch.delenv(MANAGED_HOME_ENV, raising=False)
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.setattr(layout_mod.platform, "system", lambda: "Linux")
    monkeypatch.setattr(layout_mod.Path, "home", lambda *args, **kwargs: tmp_path)
    assert resolve_install_root() == tmp_path / ".local" / "share" / "snowflake-cli"


def test_resolve_install_root_windows_localappdata(tmp_path, monkeypatch):
    monkeypatch.delenv(MANAGED_HOME_ENV, raising=False)
    monkeypatch.setattr(layout_mod.platform, "system", lambda: "Windows")
    local = tmp_path / "AppData" / "Local"
    monkeypatch.setenv("LOCALAPPDATA", str(local))
    assert resolve_install_root() == local / "snowflake-cli"


def test_install_retarget_revert_gc(managed_home):
    layout = ManagedLayout()
    assert layout.root == managed_home

    fake_a = _write_fake_binary(managed_home / "src-a", "3.12.0")
    fake_b = _write_fake_binary(managed_home / "src-b", "3.13.0")
    fake_c = _write_fake_binary(managed_home / "src-c", "3.14.0")

    installed_a = layout.install_binary("3.12.0", fake_a)
    layout.retarget("3.12.0")
    _assert_shim_points_at(layout, installed_a)
    assert layout.current_version() == "3.12.0"
    assert layout.previous_version() is None
    assert layout.gc() == []

    installed_b = layout.install_binary("3.13.0", fake_b)
    layout.retarget("3.13.0")
    _assert_shim_points_at(layout, installed_b)
    assert layout.current_version() == "3.13.0"
    assert layout.previous_version() == "3.12.0"
    # Same-session: previous version dir is untouched.
    assert installed_a.read_text() == "#!/bin/sh\necho 3.12.0\n"

    layout.install_binary("3.14.0", fake_c)
    layout.retarget("3.14.0")
    removed = layout.gc()
    assert layout.version_dir("3.12.0") in removed
    assert not layout.version_dir("3.12.0").exists()
    assert layout.version_dir("3.13.0").is_dir()
    assert layout.version_dir("3.14.0").is_dir()
    assert layout.bin_dir.is_dir()

    layout.revert()
    assert layout.current_version() == "3.13.0"
    assert layout.previous_version() == "3.14.0"
    _assert_shim_points_at(layout, installed_b)


@pytest.mark.skipif(IS_WINDOWS, reason="Unix shim is an atomic symlink replace")
def test_unix_symlink_replace_is_atomic_and_cleans_temp(managed_home):
    layout = ManagedLayout()
    first = layout.install_binary(
        "1.0.0", _write_fake_binary(managed_home / "src-1", "one")
    )
    layout.retarget("1.0.0")
    second = layout.install_binary(
        "2.0.0", _write_fake_binary(managed_home / "src-2", "two")
    )
    layout.retarget("2.0.0")

    assert layout.unix_shim.is_symlink()
    assert Path(os.readlink(layout.unix_shim)) == second
    leftover = list(layout.bin_dir.glob(".snow.tmp*")) + list(
        layout.bin_dir.glob(".*.tmp")
    )
    assert leftover == []
    assert first.exists()


def test_revert_without_previous_errors(managed_home):
    layout = ManagedLayout()
    layout.install_binary("3.12.0", _write_fake_binary(managed_home / "src", "v"))
    layout.retarget("3.12.0")
    with pytest.raises(CliError, match="No previous snowflake-managed version"):
        layout.revert()


def test_refuses_overwrite_of_running_binary(managed_home):
    layout = ManagedLayout()
    source = _write_fake_binary(managed_home / "src", "v")
    layout.install_binary("3.12.0", source)
    layout.retarget("3.12.0")
    with pytest.raises(CliError, match="Refusing to overwrite"):
        layout.install_binary("3.12.0", source)


def test_refuses_overwrite_of_previous_version(managed_home):
    layout = ManagedLayout()
    first = _write_fake_binary(managed_home / "src-a", "3.12.0")
    second = _write_fake_binary(managed_home / "src-b", "3.13.0")
    layout.install_binary("3.12.0", first)
    layout.retarget("3.12.0")
    layout.install_binary("3.13.0", second)
    layout.retarget("3.13.0")
    assert layout.previous_version() == "3.12.0"

    with pytest.raises(CliError, match="already exists"):
        layout.install_binary("3.12.0", first)
    assert layout.binary_path("3.12.0").read_text() == first.read_text()


def test_gc_without_current_leaves_unretargeted_install(managed_home):
    layout = ManagedLayout()
    installed = layout.install_binary(
        "3.12.0", _write_fake_binary(managed_home / "src", "v")
    )
    assert layout.current_version() is None
    assert layout.gc() == []
    assert installed.is_file()


def test_current_shim_target_follows_live_shim_not_stale_pointer(managed_home):
    layout = ManagedLayout()
    old = layout.install_binary(
        "3.12.0", _write_fake_binary(managed_home / "src-old", "old")
    )
    layout.retarget("3.12.0")
    layout.install_binary("3.13.0", _write_fake_binary(managed_home / "src-new", "new"))
    SecurePath(layout.current_pointer).write_text("3.13.0\n")

    assert layout.current_version() == "3.13.0"
    assert layout.current_shim_target() == old
    with pytest.raises(CliError, match="Refusing to overwrite"):
        layout.install_binary(
            "3.12.0", _write_fake_binary(managed_home / "src-retry", "x")
        )
    assert old.read_text() == "#!/bin/sh\necho old\n"


def test_gc_keeps_live_shim_target_when_pointer_is_ahead(managed_home):
    layout = ManagedLayout()
    old = layout.install_binary(
        "3.12.0", _write_fake_binary(managed_home / "src-old", "old")
    )
    layout.retarget("3.12.0")
    layout.install_binary("3.13.0", _write_fake_binary(managed_home / "src-new", "new"))
    SecurePath(layout.current_pointer).write_text("3.13.0\n")

    assert layout.gc() == []
    assert old.is_file()
    assert layout.binary_path("3.13.0").is_file()


def test_windows_shim_contents_on_linux(managed_home, monkeypatch):
    monkeypatch.setattr(layout_mod.platform, "system", lambda: "Windows")
    layout = ManagedLayout()
    source = _write_fake_binary(managed_home / "src.exe", "win")
    installed = layout.install_binary("3.12.0", source)
    assert installed.name == "snow.exe"
    layout.retarget("3.12.0")

    cmd = layout.windows_cmd_shim.read_text()
    posix = layout.unix_shim.read_text()
    assert cmd == windows_cmd_contents(installed)
    assert posix == posix_shim_contents(installed)
    assert "@echo off" in cmd
    assert "snow.exe" in cmd
    assert "%*" in cmd
    assert posix.startswith("#!/bin/sh\n")
    assert "exec " in posix
    assert not layout.unix_shim.is_symlink()


def test_invalid_version_is_rejected(managed_home):
    layout = ManagedLayout()
    source = _write_fake_binary(managed_home / "src", "v")
    with pytest.raises(CliError, match="Invalid snowflake-managed version"):
        layout.install_binary("../evil", source)
    with pytest.raises(CliError, match="Invalid snowflake-managed version"):
        layout.install_binary("bin", source)
