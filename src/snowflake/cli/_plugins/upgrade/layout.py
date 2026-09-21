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

import logging
import os
import platform
import re
from pathlib import Path
from typing import Optional

from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

MANAGED_HOME_ENV = "SNOWFLAKE_CLI_MANAGED_HOME"
APP_DIRNAME = "snowflake-cli"
BIN_DIRNAME = "bin"
UNIX_BINARY_NAME = "snow"
WINDOWS_BINARY_NAME = "snow.exe"
UNIX_SHIM_NAME = "snow"
WINDOWS_CMD_SHIM_NAME = "snow.cmd"
CURRENT_POINTER_NAME = ".current"
PREVIOUS_POINTER_NAME = ".previous"
RESERVED_ROOT_NAMES = frozenset(
    {BIN_DIRNAME, CURRENT_POINTER_NAME, PREVIOUS_POINTER_NAME}
)
_VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_QUOTED_PATH_RE = re.compile(r'"([^"]+)"')


def _is_windows() -> bool:
    return platform.system() == "Windows"


def resolve_install_root() -> Path:
    """Return the snowflake-managed install root.

    Override with ``SNOWFLAKE_CLI_MANAGED_HOME`` (tests and support). Otherwise
    Unix uses ``$XDG_DATA_HOME/snowflake-cli`` or ``~/.local/share/snowflake-cli``,
    and Windows uses ``%LOCALAPPDATA%\\snowflake-cli``.
    """
    override = os.environ.get(MANAGED_HOME_ENV)
    if override:
        return Path(override).expanduser()

    if _is_windows():
        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            return Path(local_appdata) / APP_DIRNAME
        return Path.home() / "AppData" / "Local" / APP_DIRNAME

    xdg_data_home = os.environ.get("XDG_DATA_HOME")
    if xdg_data_home:
        return Path(xdg_data_home) / APP_DIRNAME
    return Path.home() / ".local" / "share" / APP_DIRNAME


def validate_version(version: str) -> str:
    if not version or not _VERSION_RE.match(version) or version in RESERVED_ROOT_NAMES:
        raise CliError(
            f"Invalid snowflake-managed version {version!r}. "
            "Use a version directory name such as 3.12.0."
        )
    return version


def windows_cmd_contents(binary: Path) -> str:
    return f'@echo off\n"{binary}" %*\n'


def posix_shim_contents(binary: Path) -> str:
    return f'#!/bin/sh\nexec "{binary.as_posix()}" "$@"\n'


class ManagedLayout:
    """Versioned snowflake-managed install tree and PATH shims.

    Unix::

        <root>/<version>/snow
        <root>/bin/snow                 # symlink, atomically replaced

    Windows::

        <root>\\<version>\\snow.exe
        <root>\\bin\\snow.cmd
        <root>\\bin\\snow               # Git Bash
    """

    def __init__(self, root: Optional[Path] = None) -> None:
        self.root = Path(root) if root is not None else resolve_install_root()

    @property
    def bin_dir(self) -> Path:
        return self.root / BIN_DIRNAME

    @property
    def unix_shim(self) -> Path:
        return self.bin_dir / UNIX_SHIM_NAME

    @property
    def windows_cmd_shim(self) -> Path:
        return self.bin_dir / WINDOWS_CMD_SHIM_NAME

    @property
    def current_pointer(self) -> Path:
        return self.root / CURRENT_POINTER_NAME

    @property
    def previous_pointer(self) -> Path:
        return self.root / PREVIOUS_POINTER_NAME

    def version_dir(self, version: str) -> Path:
        return self.root / validate_version(version)

    def binary_name(self) -> str:
        return WINDOWS_BINARY_NAME if _is_windows() else UNIX_BINARY_NAME

    def binary_path(self, version: str) -> Path:
        return self.version_dir(version) / self.binary_name()

    def current_version(self) -> Optional[str]:
        return self._read_pointer(self.current_pointer)

    def previous_version(self) -> Optional[str]:
        return self._read_pointer(self.previous_pointer)

    def current_shim_target(self) -> Optional[Path]:
        """Binary the live shim launches, falling back to the ``.current`` pointer.

        ``retarget`` writes pointer files before replacing ``bin/snow``, so after a
        crash ``.current`` can name a version the shim does not yet launch. Callers
        that need the running file must use this, not ``current_version()`` alone.
        """
        live = self._live_shim_binary()
        if live is not None:
            return live
        version = self.current_version()
        if version is None:
            return None
        return self.binary_path(version)

    def install_binary(self, version: str, source: Path) -> Path:
        """Copy ``source`` into a new version directory.

        Never overwrites an existing version binary — including the live shim
        target, ``.current``, and ``.previous`` (needed for revert).
        """
        dest = self.binary_path(version)
        if dest.exists() or dest.is_symlink():
            if self._is_protected_binary(dest):
                raise CliError(
                    f"Refusing to overwrite the running snowflake-managed binary at {dest}. "
                    "Install into a new version directory, then retarget the shim."
                )
            raise CliError(
                f"Snowflake-managed version {version} already exists at {dest}. "
                "Install into a new version directory, then retarget the shim."
            )

        SecurePath(dest.parent).mkdir(parents=True, exist_ok=True)
        SecurePath(source).copy(dest)
        if not _is_windows():
            dest.chmod(0o755)
        log.info("Installed snowflake-managed binary for %s at %s", version, dest)
        return dest

    def retarget(self, version: str) -> Path:
        """Point shims at ``version`` and record the previous target for revert."""
        binary = self.binary_path(version)
        if not binary.is_file():
            raise CliError(
                f"No snowflake-managed binary at {binary}. "
                "Install that version before retargeting the shim."
            )

        previous = self.current_version()
        if previous is not None and previous != version:
            self._write_pointer(self.previous_pointer, previous)
        self._write_pointer(self.current_pointer, version)
        self._write_shims(binary)
        log.info("Retargeted snowflake-managed shim to %s", binary)
        return binary

    def revert(self) -> Path:
        """Point shims at the recorded previous version directory."""
        previous = self.previous_version()
        if previous is None:
            raise CliError(
                "No previous snowflake-managed version is recorded. "
                "Revert is only available after a successful upgrade."
            )
        current = self.current_version()
        target = self.retarget(previous)
        if current is not None and current != previous:
            self._write_pointer(self.previous_pointer, current)
        return target

    def gc(self) -> list[Path]:
        """Delete version directories other than current, previous, and the live shim.

        No-op until ``.current`` is set so an install that has not been retargeted
        (crash recovery, or install → gc → retarget) is not wiped.
        """
        removed: list[Path] = []
        if self.current_version() is None:
            log.info("Skipping snowflake-managed GC; no current version is recorded.")
            return removed
        if not self.root.is_dir():
            return removed

        keep = {self.current_version(), self.previous_version()} - {None}
        live = self._live_shim_binary()
        if live is not None:
            live_version = self._version_name_for_binary(live)
            if live_version is not None:
                keep.add(live_version)

        for child in self.root.iterdir():
            if child.name in RESERVED_ROOT_NAMES or not child.is_dir():
                continue
            if child.name in keep:
                continue
            log.info("Removing unused snowflake-managed version dir %s", child)
            SecurePath(child).rmdir(recursive=True)
            removed.append(child)
        return removed

    def _live_shim_binary(self) -> Optional[Path]:
        """Return the binary the PATH shim currently launches, if any."""
        if self.unix_shim.is_symlink():
            return Path(os.readlink(self.unix_shim))
        for shim in (self.windows_cmd_shim, self.unix_shim):
            if not shim.is_file():
                continue
            match = _QUOTED_PATH_RE.search(
                SecurePath(shim).read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
            )
            if match:
                return Path(match.group(1))
        return None

    def _protected_binaries(self) -> list[Path]:
        paths: list[Path] = []
        live = self._live_shim_binary()
        if live is not None:
            paths.append(live)
        current = self.current_version()
        if current:
            paths.append(self.binary_path(current))
        return paths

    def _is_protected_binary(self, dest: Path) -> bool:
        try:
            dest_resolved = dest.resolve()
        except OSError:
            return False
        for protected in self._protected_binaries():
            try:
                if (protected.exists() or protected.is_symlink()) and (
                    dest_resolved == protected.resolve()
                ):
                    return True
            except OSError:
                continue
        return False

    def _version_name_for_binary(self, binary: Path) -> Optional[str]:
        try:
            parent = Path(binary).parent
            if parent.parent.resolve() != self.root.resolve():
                return None
        except OSError:
            return None
        name = parent.name
        if not name or name in RESERVED_ROOT_NAMES:
            return None
        return name

    def _read_pointer(self, path: Path) -> Optional[str]:
        if not path.is_file():
            return None
        value = SecurePath(path).read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB)
        version = value.strip()
        return version or None

    def _write_pointer(self, path: Path, version: str) -> None:
        SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
        SecurePath(path).write_text(f"{validate_version(version)}\n")

    def _write_shims(self, binary: Path) -> None:
        SecurePath(self.bin_dir).mkdir(parents=True, exist_ok=True)
        if _is_windows():
            _atomic_write_text(self.windows_cmd_shim, windows_cmd_contents(binary))
            posix_shim = self.unix_shim
            _atomic_write_text(posix_shim, posix_shim_contents(binary))
            posix_shim.chmod(0o755)
            return
        _atomic_symlink_replace(self.unix_shim, binary)


def _atomic_symlink_replace(link_path: Path, target: Path) -> None:
    """Replace ``link_path`` with a symlink to ``target`` via temp + ``os.replace``."""
    SecurePath(link_path.parent).mkdir(parents=True, exist_ok=True)
    tmp = link_path.parent / f".{link_path.name}.{os.getpid()}.tmp"
    try:
        if tmp.exists() or tmp.is_symlink():
            tmp.unlink()
        tmp.symlink_to(target)
        os.replace(tmp, link_path)
    except BaseException:
        if tmp.exists() or tmp.is_symlink():
            tmp.unlink()
        raise


def _atomic_write_text(path: Path, contents: str) -> None:
    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    tmp_secure = SecurePath(tmp)
    tmp_secure.unlink(missing_ok=True)
    try:
        tmp_secure.write_text(contents)
        os.replace(tmp, path)
    except BaseException:
        tmp_secure.unlink(missing_ok=True)
        raise
