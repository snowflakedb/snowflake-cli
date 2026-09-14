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
import stat
from dataclasses import dataclass, field
from pathlib import Path

from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import FileTooLargeError
from snowflake.cli.api.secure_path import SecurePath

RC_FILENAMES = (".zprofile", ".zshrc", ".profile", ".bash_profile", ".bashrc")
HISTORICAL_COMMENT = "# added by Snowflake SnowflakeCLI installer v1.0"
MACOS_APP_PATH = "SnowflakeCLI.app/Contents/MacOS"
BEGIN_MARK = "# snowflake-cli PATH begin"
END_MARK = "# snowflake-cli PATH end"
# Mirrors the backup convention already used by the macOS installer's
# postinstall script (`cp -p $profile "$profile-snowflake.bak"`), under a
# distinct suffix so the two backups never collide.
BACKUP_SUFFIX = "-snowflake-cleanup.bak"
TEMP_SUFFIX = ".snowflake-cleanup.tmp"


@dataclass(frozen=True)
class FileCleanup:
    path: Path
    removed_pairs: int
    unpaired_comment_lines: tuple[int, ...]


@dataclass
class CleanupResult:
    files: list[FileCleanup] = field(default_factory=list)
    missing_files: list[Path] = field(default_factory=list)
    skipped_files: list[Path] = field(default_factory=list)
    skipped_symlinks: list[Path] = field(default_factory=list)


def _line_text(line: str) -> str:
    return line.rstrip("\r\n")


def _protected_lines(lines: list[str]) -> set[int]:
    protected: set[int] = set()
    inside_marked_block = False
    for index, line in enumerate(lines):
        text = _line_text(line)
        if text == BEGIN_MARK:
            inside_marked_block = True
        if inside_marked_block:
            protected.add(index)
        if text == END_MARK and inside_marked_block:
            inside_marked_block = False
    return protected


def _is_historical_path_line(line: str) -> bool:
    prefix = "export PATH="
    if not line.startswith(prefix) or "brew shellenv" in line:
        return False
    destination, separator, _ = line.removeprefix(prefix).partition(":$PATH")
    return bool(separator and MACOS_APP_PATH in destination)


def _clean_file_contents(contents: str) -> tuple[str, int, tuple[int, ...]]:
    lines = contents.splitlines(keepends=True)
    protected = _protected_lines(lines)
    removed: set[int] = set()
    unpaired: list[int] = []
    pair_count = 0

    for index, line in enumerate(lines):
        if _line_text(line) != HISTORICAL_COMMENT:
            continue

        following_index = index + 1
        while (
            following_index < len(lines)
            and not _line_text(lines[following_index]).strip()
        ):
            following_index += 1

        if following_index < len(lines):
            following_line = _line_text(lines[following_index])
            is_historical_path = _is_historical_path_line(following_line)
            crosses_marked_block = any(
                line_index in protected
                for line_index in range(index, following_index + 1)
            )
        else:
            is_historical_path = False
            crosses_marked_block = False

        if is_historical_path and not crosses_marked_block:
            # Comment, any blank lines used to find the PATH line, the PATH
            # line itself, and at most one trailing blank that the installer
            # typically leaves after the pair.
            removed.add(index)
            removed.update(range(index + 1, following_index + 1))
            trailing_index = following_index + 1
            if (
                trailing_index < len(lines)
                and trailing_index not in protected
                and not _line_text(lines[trailing_index]).strip()
            ):
                removed.add(trailing_index)
            pair_count += 1
        elif not is_historical_path:
            unpaired.append(index + 1)

    cleaned = "".join(line for index, line in enumerate(lines) if index not in removed)
    return cleaned, pair_count, tuple(unpaired)


def _backup_original_file(secure_path: SecurePath) -> None:
    """Copy the original file to a sibling ``*-snowflake-cleanup.bak`` file.

    Uses SecurePath.copy() so the backup goes through the same size and
    permission safety net as the rest of this module. Any stale backup from a
    previous run is replaced so the command stays idempotent.
    """
    backup_path = secure_path.path.with_name(secure_path.path.name + BACKUP_SUFFIX)
    SecurePath(backup_path).unlink(missing_ok=True)
    secure_path.copy(backup_path)


def _write_cleaned_contents_atomically(secure_path: SecurePath, cleaned: str) -> None:
    """Write ``cleaned`` without ever leaving a half-written rc file behind.

    The new contents are written to a temporary file in the same directory as
    the target (so the final move is on the same filesystem), the original
    file's permissions are copied onto it, and ``os.replace`` swaps it onto the
    target path atomically (atomic on POSIX).

    If anything from the write through the replace fails, the temporary file
    is removed before the exception propagates, so a failed run never leaves
    a stray ``*.snowflake-cleanup.tmp`` file behind.
    """
    target_path = secure_path.path
    original_mode = stat.S_IMODE(target_path.stat().st_mode)

    tmp_path = target_path.with_name(target_path.name + TEMP_SUFFIX)
    tmp_secure_path = SecurePath(tmp_path)
    tmp_secure_path.unlink(missing_ok=True)
    try:
        tmp_secure_path.write_text(cleaned)
        os.chmod(tmp_path, original_mode)
        os.replace(tmp_path, target_path)
    except BaseException:
        tmp_secure_path.unlink(missing_ok=True)
        raise


def _apply_cleanup_with_backup(secure_path: SecurePath, cleaned: str) -> None:
    """Back up ``secure_path`` and atomically overwrite it with ``cleaned``.

    If the atomic write fails after the backup was created, the just-created
    backup is removed before the exception propagates: since the original
    file was never actually modified, a failed run should leave no backup
    artifact behind either.
    """
    backup_path = secure_path.path.with_name(secure_path.path.name + BACKUP_SUFFIX)
    _backup_original_file(secure_path)
    try:
        _write_cleaned_contents_atomically(secure_path, cleaned)
    except BaseException:
        SecurePath(backup_path).unlink(missing_ok=True)
        raise


def clean_installer_path_files(home: Path, apply: bool) -> CleanupResult:
    result = CleanupResult()
    for filename in RC_FILENAMES:
        path = home / filename
        secure_path = SecurePath(path)
        if not secure_path.exists():
            result.missing_files.append(path)
            continue

        # os.replace() (used to atomically apply the cleaned contents) does not
        # follow a symlink: it replaces the symlink's own directory entry, not
        # the file it points to. Treat symlinked rc files as unsupported and
        # skip them rather than silently converting them into regular files
        # (and, via stat() following the link, possibly inheriting the
        # target's permission bits).
        if path.is_symlink():
            result.skipped_symlinks.append(path)
            continue

        try:
            contents = secure_path.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
            cleaned, pair_count, unpaired = _clean_file_contents(contents)
            if apply and pair_count:
                _apply_cleanup_with_backup(secure_path, cleaned)
        except (OSError, UnicodeError, FileTooLargeError):
            result.skipped_files.append(path)
            continue

        result.files.append(
            FileCleanup(
                path=path,
                removed_pairs=pair_count,
                unpaired_comment_lines=unpaired,
            )
        )
    return result
