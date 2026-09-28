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

import ctypes
import errno
import hashlib
import logging
import os
import platform
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional, Tuple

from snowflake.cli._plugins.upgrade.layout import ManagedLayout
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

_LOCK_MODE = 0o600
_ERROR_ALREADY_EXISTS = 183
_MUTEX_PREFIX = "Local\\SnowflakeCliUpgradeLock-"


def _is_windows() -> bool:
    return platform.system() == "Windows"


@contextmanager
def try_upgrade_lock(layout: Optional[ManagedLayout] = None) -> Iterator[bool]:
    """Non-blocking exclusive lock on ``<managed-home>/.upgrade.lock``.

    Yields ``True`` if this process holds the lock, ``False`` if another snow
    is already upgrading (caller fail-opens). Released on context exit,
    including when the body raises.

    Unix uses ``fcntl.flock``. Windows uses a per-managed-home named mutex
    (``CreateMutexW``); flock-style byte locks are awkward across ``msvcrt``
    and we do not take a ``portalocker`` dependency.
    """
    root = layout if layout is not None else ManagedLayout()
    path = root.upgrade_lock_path
    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)

    if _is_windows():
        handle, acquired = _try_windows_mutex(path)
        try:
            yield acquired
        finally:
            if handle is not None:
                _release_windows_mutex(handle)
        return

    fd = os.open(str(path), os.O_CREAT | os.O_RDWR, _LOCK_MODE)
    acquired = False
    try:
        os.chmod(path, _LOCK_MODE)
        acquired = _try_unix_flock(fd)
        yield acquired
    finally:
        if acquired:
            _unlock_unix(fd)
        os.close(fd)


def _try_unix_flock(fd: int) -> bool:
    import fcntl

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    except OSError as exc:
        if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
            return False
        raise


def _unlock_unix(fd: int) -> None:
    import fcntl

    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        log.debug("Failed to unlock upgrade lock fd %s", fd, exc_info=True)


def _mutex_name(lock_path: Path) -> str:
    digest = hashlib.sha256(os.fsencode(str(lock_path.resolve()))).hexdigest()
    return f"{_MUTEX_PREFIX}{digest}"


def _try_windows_mutex(lock_path: Path) -> Tuple[Optional[int], bool]:
    """CreateMutexW; documented Windows stand-in for flock.

    Returns ``(handle, acquired)``. On ``ERROR_ALREADY_EXISTS`` the handle is
    closed here and ``acquired`` is False.
    """
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel32.CreateMutexW.argtypes = [
        ctypes.c_void_p,
        wintypes.BOOL,
        ctypes.c_wchar_p,
    ]
    kernel32.CreateMutexW.restype = ctypes.c_void_p

    name = _mutex_name(lock_path)
    handle = kernel32.CreateMutexW(None, True, name)
    if not handle:
        log.debug("CreateMutexW failed for upgrade lock", exc_info=False)
        return None, False
    if ctypes.get_last_error() == _ERROR_ALREADY_EXISTS:  # type: ignore[attr-defined]
        kernel32.CloseHandle(handle)
        return None, False
    SecurePath(lock_path).touch(permissions_mask=_LOCK_MODE, exist_ok=True)
    return handle, True


def _release_windows_mutex(handle: int) -> None:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel32.ReleaseMutex(handle)
    kernel32.CloseHandle(handle)
