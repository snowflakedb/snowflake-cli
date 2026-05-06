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

import logging
import stat
from pathlib import Path
from typing import List, Optional

from snowflake.connector.compat import IS_WINDOWS

log = logging.getLogger(__name__)

# Well-known Windows SIDs whose display names are localized per system language
# (e.g. "SYSTEM" / "Administrators" in English, "Système" / "Administrateurs"
# in French, "системный" / "Администраторы" in Russian). Resolving them at
# runtime keeps the whitelist correct on non-English Windows installations.
# https://learn.microsoft.com/en-us/windows-server/identity/ad-ds/manage/understand-security-identifiers
_LOCAL_SYSTEM_SID = "S-1-5-18"
_BUILTIN_ADMINISTRATORS_SID = "S-1-5-32-544"

# English fallbacks used if the Windows API call fails (or when running under
# a stubbed API in tests). These match the pre-i18n behavior.
_LOCAL_SYSTEM_FALLBACK_NAME = "SYSTEM"
_BUILTIN_ADMINISTRATORS_FALLBACK_NAME = "Administrators"


def _lookup_windows_sid_account_name(sid_str: str) -> Optional[str]:
    """Resolve a well-known SID string to its localized account name via the
    Windows API, or ``None`` if resolution fails.

    Uses ``ctypes`` rather than ``pywin32`` so we don't add a new runtime
    dependency. Only called on Windows.
    """
    try:
        import ctypes
        from ctypes import wintypes
    except ImportError:  # pragma: no cover - ctypes is part of stdlib
        return None

    try:
        advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)  # type: ignore[attr-defined]
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    except (AttributeError, OSError):
        # WinDLL is missing on non-Windows, and loading can fail under unusual
        # runtimes. Fall back to the caller-supplied default in both cases.
        return None

    convert_string_sid_to_sid = advapi32.ConvertStringSidToSidW
    convert_string_sid_to_sid.argtypes = [
        wintypes.LPCWSTR,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    convert_string_sid_to_sid.restype = wintypes.BOOL

    lookup_account_sid = advapi32.LookupAccountSidW
    lookup_account_sid.argtypes = [
        wintypes.LPCWSTR,
        ctypes.c_void_p,
        wintypes.LPWSTR,
        wintypes.LPDWORD,
        wintypes.LPWSTR,
        wintypes.LPDWORD,
        ctypes.POINTER(wintypes.DWORD),
    ]
    lookup_account_sid.restype = wintypes.BOOL

    local_free = kernel32.LocalFree
    local_free.argtypes = [wintypes.HLOCAL]

    sid = ctypes.c_void_p()
    if not convert_string_sid_to_sid(sid_str, ctypes.byref(sid)):
        return None

    try:
        name_size = wintypes.DWORD(256)
        domain_size = wintypes.DWORD(256)
        name = ctypes.create_unicode_buffer(name_size.value)
        domain = ctypes.create_unicode_buffer(domain_size.value)
        sid_name_use = wintypes.DWORD()
        if not lookup_account_sid(
            None,
            sid,
            name,
            ctypes.byref(name_size),
            domain,
            ctypes.byref(domain_size),
            ctypes.byref(sid_name_use),
        ):
            return None
        return name.value or None
    finally:
        local_free(sid)


def _get_windows_whitelisted_users():
    # whitelisted users list obtained in consultation with prodsec: CASEC-9627
    import os

    system_name = (
        _lookup_windows_sid_account_name(_LOCAL_SYSTEM_SID)
        or _LOCAL_SYSTEM_FALLBACK_NAME
    )
    admins_name = (
        _lookup_windows_sid_account_name(_BUILTIN_ADMINISTRATORS_SID)
        or _BUILTIN_ADMINISTRATORS_FALLBACK_NAME
    )

    whitelisted = [
        system_name,
        admins_name,
        "Network",
        "Domain Admins",
        "Domain Users",
        os.getlogin(),
    ]

    # Keep the English names too, so administrators who have mixed-language
    # inventories still pass the whitelist if a host returns English names
    # under a non-English locale.
    if system_name != _LOCAL_SYSTEM_FALLBACK_NAME:
        whitelisted.append(_LOCAL_SYSTEM_FALLBACK_NAME)
    if admins_name != _BUILTIN_ADMINISTRATORS_FALLBACK_NAME:
        whitelisted.append(_BUILTIN_ADMINISTRATORS_FALLBACK_NAME)

    return whitelisted


def _run_icacls(file_path: Path) -> str:
    import subprocess

    return subprocess.check_output(["icacls", str(file_path)], text=True)


def _windows_permissions_are_denied(permission_codes: str) -> bool:
    # according to https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/icacls
    return "(DENY)" in permission_codes or "(N)" in permission_codes


def windows_get_not_whitelisted_users_with_access(file_path: Path) -> List[str]:
    import re

    # according to https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/icacls
    icacls_output_regex = (
        rf"({re.escape(str(file_path))})?.*\\(?P<user>.*):(?P<permissions>[(A-Z),]+)"
    )
    whitelisted_users = _get_windows_whitelisted_users()

    users_with_access = []
    for permission in re.finditer(icacls_output_regex, _run_icacls(file_path)):
        if (permission.group("user") not in whitelisted_users) and (
            not _windows_permissions_are_denied(permission.group("permissions"))
        ):
            users_with_access.append(permission.group("user"))
    return list(set(users_with_access))


def _windows_file_permissions_are_strict(file_path: Path) -> bool:
    return windows_get_not_whitelisted_users_with_access(file_path) == []


def _unix_file_permissions_are_strict(file_path: Path) -> bool:
    accessible_by_others = (
        # https://docs.python.org/3/library/stat.html
        stat.S_IRGRP  # readable by group
        | stat.S_IROTH  # readable by others
        | stat.S_IWGRP  # writeable by group
        | stat.S_IWOTH  # writeable by others
        | stat.S_IXGRP  # executable by group
        | stat.S_IXOTH  # executable by others
    )
    return (file_path.stat().st_mode & accessible_by_others) == 0


def file_permissions_are_strict(file_path: Path) -> bool:
    if IS_WINDOWS:
        return _windows_file_permissions_are_strict(file_path)
    return _unix_file_permissions_are_strict(file_path)


def chmod(path: Path, permissions_mask: int) -> None:
    log.info("Update permissions of file %s to %s", path, oct(permissions_mask))
    path.chmod(permissions_mask)


def _unix_restrict_file_permissions(path: Path) -> None:
    owner_permissions = (
        # https://docs.python.org/3/library/stat.html
        stat.S_IRUSR  # readable by owner
        | stat.S_IWUSR  # writeable by owner
        | stat.S_IXUSR  # executable by owner
    )
    chmod(path, path.stat().st_mode & owner_permissions)


def _windows_restrict_file_permissions(path: Path) -> None:
    import subprocess

    for user in windows_get_not_whitelisted_users_with_access(path):
        log.info("Removing permissions of user %s from file %s", user, path)
        subprocess.run(["icacls", str(path), "/remove:g", f"{user}"])


def restrict_file_permissions(file_path: Path) -> None:
    if IS_WINDOWS:
        _windows_restrict_file_permissions(file_path)
    else:
        _unix_restrict_file_permissions(file_path)
