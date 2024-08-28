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
from typing import List

from snowflake.connector.compat import IS_WINDOWS

log = logging.getLogger(__name__)


def _get_windows_whitelisted_users():
    # whitelisted users list obtained in consultation with prodsec: CASEC-9627
    import os

    return [
        "SYSTEM",
        "Administrators",
        "Network",
        "Domain Admins",
        "Domain Users",
        os.getlogin(),
    ]


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
        subprocess.run(["icacls", str(path), "/DENY", f"{user}:F"])


def restrict_file_permissions(file_path: Path) -> None:
    if IS_WINDOWS:
        _windows_restrict_file_permissions(file_path)
    else:
        _unix_restrict_file_permissions(file_path)
