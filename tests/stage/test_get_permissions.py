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

import shutil
import subprocess
from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.secure_utils import windows_get_not_whitelisted_users_with_access
from snowflake.cli.api.stage_path import StagePath

from tests.testing_utils.files_and_dirs import assert_file_permissions_are_strict
from tests_common import IS_WINDOWS

STAGE_MANAGER = "snowflake.cli._plugins.stage.manager.StageManager"


def _widen(path: Path) -> None:
    if IS_WINDOWS:
        perms = "(OI)(CI)F" if path.is_dir() else "F"
        result = subprocess.run(
            ["icacls", str(path), "/GRANT", f"Everyone:{perms}"],
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    else:
        path.chmod(0o777 if path.is_dir() else 0o666)


def _assert_still_wide(path: Path) -> None:
    if IS_WINDOWS:
        assert windows_get_not_whitelisted_users_with_access(path)
    else:
        expected = 0o777 if path.is_dir() else 0o666
        assert path.stat().st_mode & 0o777 == expected


def _write_wide_file(path: Path, text: str) -> None:
    path.write_text(text)
    _widen(path)


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_get_restricts_new_files_and_skips_neighbors(mock_execute, tmp_path):
    dest = tmp_path / "downloads"
    dest.mkdir()
    neighbor = dest / "already_here.txt"
    _write_wide_file(neighbor, "keep")
    neighbor_dir = dest / "keep_dir"
    neighbor_dir.mkdir()
    _widen(neighbor_dir)

    def write_download(*args, **kwargs):
        _write_wide_file(dest / "staged.sql", "SELECT 1")
        _write_wide_file(dest / "other.sql", "SELECT 2")
        (neighbor_dir / "touched").write_text("x")
        return mock.Mock()

    mock_execute.side_effect = write_download
    StageManager().get("@mystage/staged.sql", dest)

    assert_file_permissions_are_strict(dest / "staged.sql")
    assert_file_permissions_are_strict(dest / "other.sql")
    _assert_still_wide(neighbor)
    _assert_still_wide(neighbor_dir)


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_get_restricts_replaced_file(mock_execute, tmp_path):
    dest = tmp_path / "downloads"
    dest.mkdir()
    existing = dest / "staged.sql"
    _write_wide_file(existing, "old")

    def overwrite(*args, **kwargs):
        existing.write_text("new content")
        return mock.Mock()

    mock_execute.side_effect = overwrite
    StageManager().get("@mystage/staged.sql", dest)

    assert_file_permissions_are_strict(existing)


@mock.patch(f"{STAGE_MANAGER}.iter_stage")
@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_get_recursive_restricts_nested_file(mock_execute, mock_iter, tmp_path):
    dest = tmp_path / "downloads"
    dest.mkdir()
    mock_iter.return_value = [StagePath.from_stage_str("@mystage/rendered/obj.sql")]

    def write_download(*args, **kwargs):
        _write_wide_file(dest / "rendered" / "obj.sql", "SELECT 1")
        return mock.Mock()

    mock_execute.side_effect = write_download
    StageManager().get_recursive("@mystage", dest)

    assert_file_permissions_are_strict(dest / "rendered" / "obj.sql")


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_get_restricts_partial_downloads_when_get_fails(mock_execute, tmp_path):
    dest = tmp_path / "downloads"
    dest.mkdir()

    def write_then_fail(*args, **kwargs):
        _write_wide_file(dest / "landed.sql", "SELECT 1")
        raise RuntimeError("GET failed")

    mock_execute.side_effect = write_then_fail
    with pytest.raises(RuntimeError, match="GET failed"):
        StageManager().get("@mystage/staged.sql", dest)

    assert_file_permissions_are_strict(dest / "landed.sql")


@mock.patch(f"{STAGE_MANAGER}.execute_query")
def test_get_restricts_file_moved_from_wide_temp(mock_execute, tmp_path):
    dest = tmp_path / "downloads"
    dest.mkdir()
    wide_temp = tmp_path / "wide_temp"
    wide_temp.mkdir()
    _widen(wide_temp)

    def move_from_temp(*args, **kwargs):
        staged = wide_temp / "staged.sql"
        staged.write_text("SELECT 1")
        shutil.move(str(staged), dest / "staged.sql")
        return mock.Mock()

    mock_execute.side_effect = move_from_temp
    StageManager().get("@mystage/staged.sql", dest)

    assert_file_permissions_are_strict(dest / "staged.sql")
