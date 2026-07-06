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
import os
import stat
import warnings
from pathlib import Path
from typing import List

from snowflake.cli.api.constants import IS_WINDOWS
from snowflake.cli.api.utils.types import try_cast_to_bool

log = logging.getLogger(__name__)

# Mirrors the Python connector (config_manager.py) bitmasks
_READABLE_BY_OTHERS = stat.S_IRGRP | stat.S_IROTH  # 0o044
_WRITABLE_BY_OTHERS = stat.S_IWGRP | stat.S_IWOTH  # 0o022

# Public env var and the SPCS-injected variant that opt into relaxed permission
# enforcement (readable-by-others config files are allowed, downgraded to a
# warning instead of a hard error). Mirrors the connector, minus the deprecated
# SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE which we intentionally do
# not honour here.
_SKIP_WARNING_ENV_VAR = "SF_SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION"
_SPCS_INJECTED_SKIP_ENV_VAR = "SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION"


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


def file_is_writable_by_others(file_path: Path) -> bool:
    if IS_WINDOWS:
        return False
    return bool(file_path.stat().st_mode & _WRITABLE_BY_OTHERS)


def file_is_readable_by_others(file_path: Path) -> bool:
    if IS_WINDOWS:
        return False
    return bool(file_path.stat().st_mode & _READABLE_BY_OTHERS)


def should_skip_permission_warning() -> bool:
    """
    Whether the user has opted into relaxed permission enforcement via one of the
    connector's skip env vars.

    When this returns True, a config file that is *readable* by group/others is
    allowed (downgraded from a hard ``ConfigFileTooWidePermissionsError`` to a
    warning). It never relaxes the *writable*-by-others check, which always
    raises. The public var takes precedence over the SPCS-injected one; an
    unparsable value is treated as False (does not skip).
    """
    for env_var in (_SKIP_WARNING_ENV_VAR, _SPCS_INJECTED_SKIP_ENV_VAR):
        raw_value = os.environ.get(env_var)
        if raw_value is None:
            continue
        try:
            return try_cast_to_bool(raw_value)
        except ValueError:
            log.debug(
                "Could not parse %s value %r as boolean, defaulting to False",
                env_var,
                raw_value,
            )
            return False
    return False


def issue_unix_permissions_warning(config_path: Path) -> None:
    warnings.warn(
        f"Bad owner or permissions on {config_path}.\n"
        f' * To change owner, run `chown $USER "{config_path}"`.\n'
        f' * To restrict permissions, run `chmod 0600 "{config_path}"`.\n'
        f" * In future versions of Snowflake CLI strict configuration file permissions "
        f"will be mandatory. To test if your files have correct permissions set "
        f"SNOWFLAKE_CLI_FEATURES_ENFORCE_STRICT_CONFIG_PERMISSIONS=1 and try again.",
        stacklevel=4,
    )


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
