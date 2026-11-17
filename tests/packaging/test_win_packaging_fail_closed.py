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

"""Windows packaging must fail closed after hatch/exe/zip errors.

Build-dev-binaries Win #551: hatch cargo 404 still produced SUCCESS and a
~352 byte zip. The Jenkins job treated that as green. Source-lock the bat
so hatch, move, --help, --version, rmdir, undersized zip, and zip members
all `exit /b 1`. Member listing accepts both slash directions because
Windows tar -tf may print dist/snow/snow.exe or dist\\snow\\snow.exe.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WINDOWS_SCRIPT = REPO_ROOT / "scripts" / "packaging" / "win" / "build_snowflake_cli.bat"
MIN_ZIP_BYTES = 1048576


def _bat() -> str:
    return WINDOWS_SCRIPT.read_text()


def _lines() -> list[str]:
    return [line.rstrip("\r") for line in _bat().splitlines()]


def _index_containing(needle: str) -> int:
    for i, line in enumerate(_lines()):
        if needle in line:
            return i
    raise AssertionError(f"missing {needle!r} in {WINDOWS_SCRIPT}")


def _block(start: int, n: int = 5) -> str:
    return "\n".join(_lines()[start : start + n]).lower()


def test_min_zip_is_one_mib():
    assert f"MIN_ZIP_BYTES={MIN_ZIP_BYTES}" in _bat()
    assert "LSS %MIN_ZIP_BYTES%" in _bat()
    assert "ZIP_OK=" in _bat()
    idx = _index_containing("LSS %MIN_ZIP_BYTES%")
    block = "\n".join(_lines()[idx : idx + 4]).lower()
    assert "exit /b 1" in block


def test_zip_member_checks_fail_closed():
    text = _bat()
    assert 'set "ZIP_LIST=%ZIP_PATH%.list"' in text
    tar_idx = _index_containing("tar -tf")
    tar_block = _block(tar_idx, n=6)
    assert "errorlevel" in tar_block
    assert "exit /b 1" in tar_block
    snow_idx = _index_containing('findstr /I /C:"dist/snow/snow.exe"')
    snow_line = _lines()[snow_idx]
    assert '/C:"dist/snow/snow.exe"' in snow_line
    assert r'/C:"dist\snow\snow.exe"' in snow_line
    snow_block = _block(snow_idx, n=6)
    assert "errorlevel" in snow_block
    assert "exit /b 1" in snow_block
    assert 'type "%zip_list%"' in snow_block
    managed_idx = _index_containing('findstr /I /C:"dist/snowflake-managed/snow.exe"')
    managed_line = _lines()[managed_idx]
    assert '/C:"dist/snowflake-managed/snow.exe"' in managed_line
    assert r'/C:"dist\snowflake-managed\snow.exe"' in managed_line
    managed_block = _block(managed_idx, n=6)
    assert "errorlevel" in managed_block
    assert "exit /b 1" in managed_block
    assert 'type "%zip_list%"' in managed_block


def test_hatch_isolated_binary_fail_closed():
    idx = _index_containing("hatch -e packaging run build-isolated-binary")
    block = _block(idx)
    assert "errorlevel" in block
    assert "exit /b 1" in block


def test_missing_exe_after_hatch_fail_closed():
    text = _bat()
    assert r'if not exist "dist\binary\snow-%CLI_VERSION%.exe"' in text
    idx = _index_containing(r'if not exist "dist\binary\snow-%CLI_VERSION%.exe"')
    block = "\n".join(_lines()[idx : idx + 4]).lower()
    assert "exit /b 1" in block


def test_move_and_help_fail_closed():
    text = _bat()
    assert r'move "dist\binary\snow-%CLI_VERSION%.exe"' in text
    move_idx = _index_containing(r'move "dist\binary\snow-%CLI_VERSION%.exe"')
    move_block = _block(move_idx)
    assert "errorlevel" in move_block
    assert "exit /b 1" in move_block
    help_idx = _index_containing('snow.exe" --help')
    help_block = _block(help_idx)
    assert "errorlevel" in help_block
    assert "exit /b 1" in help_block
    version_idx = _index_containing('snow.exe" --version')
    version_block = _block(version_idx)
    assert "errorlevel" in version_block
    assert "exit /b 1" in version_block


def test_zip_assert_used_on_both_paths():
    text = _bat()
    assert "call :assert_zip_ok snowflake-cli-%CLI_VERSION_WIN%.zip" in text
    assert text.count("call :assert_zip_ok") >= 2
    stripped = [line.strip() for line in _lines()]
    assert "call :build_and_stage snow" in stripped
    assert "call :build_and_stage snowflake-managed" in stripped
    for label in (
        "call :build_and_stage snow",
        "call :build_and_stage snowflake-managed",
        "call :assert_zip_ok snowflake-cli-%CLI_VERSION_WIN%.zip",
    ):
        idx = next(i for i, line in enumerate(stripped) if line == label)
        block = _block(idx)
        assert "errorlevel" in block
        assert "exit /b 1" in block


def test_rmdir_dist_fail_closed():
    text = _bat()
    assert "could not remove dist" in text
    idx = _index_containing("could not remove dist")
    block = "\n".join(_lines()[max(0, idx - 3) : idx + 2]).lower()
    assert "exit /b 1" in block


def test_empty_hatch_version_fail_closed():
    text = _bat()
    assert 'if "%CLI_VERSION_WIN%"==""' in text
    assert 'if "%CLI_VERSION%"==""' in text
    for needle in ('if "%CLI_VERSION_WIN%"==""', 'if "%CLI_VERSION%"==""'):
        idx = _index_containing(needle)
        block = "\n".join(_lines()[idx : idx + 4]).lower()
        assert "exit /b 1" in block


def test_managed_sets_installation_source_before_build():
    stripped = [line.strip() for line in _lines()]
    source_idx = stripped.index(
        "set SNOWFLAKE_CLI_INSTALLATION_SOURCE=SNOWFLAKE_MANAGED"
    )
    call_idx = stripped.index("call :build_and_stage snowflake-managed")
    assert source_idx < call_idx
