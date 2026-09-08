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

import functools
import logging
import os
import re
import stat
import warnings
from pathlib import Path
from typing import List

from snowflake.cli.api.constants import IS_WINDOWS
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal
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

# Absolute paths prevent %PATH% hijacking — an attacker who can prepend a
# world-writable directory to %PATH% would otherwise control which binary runs.
# %SystemRoot% is a rarely-modified, stable system variable, making it a much
# narrower attack surface than %PATH%.
_SYSTEM32 = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32"
_WHOAMI = str(_SYSTEM32 / "whoami.exe")
_ICACLS = str(_SYSTEM32 / "icacls.exe")


@functools.lru_cache(maxsize=None)
def _get_windows_username() -> str:
    import subprocess

    try:
        result = subprocess.run(
            [_WHOAMI], capture_output=True, text=True, check=True, timeout=10
        )
        return result.stdout.strip()
    except subprocess.TimeoutExpired as e:
        raise CliError(
            f"whoami.exe timed out while determining the current Windows username. "
            f"Config file permission checks require whoami.exe to be available."
        ) from e
    except FileNotFoundError as e:
        raise CliError(
            f"Snowflake CLI could not determine the current Windows username "
            f"({_WHOAMI} not found). "
            f"Config file permission checks require whoami.exe to be available."
        ) from e
    except subprocess.CalledProcessError as e:
        raise CliError(
            f"whoami.exe exited with a non-zero status ({e.returncode}) while "
            f"determining the current Windows username. "
            f"Config file permission checks require whoami.exe to be available."
        ) from e


def _strip_windows_domain_prefix(name: str) -> str:
    # icacls output regex captures only the short name (the part after the last
    # backslash), so whitelist entries must use the same form for comparison to work.
    return name.rsplit("\\", 1)[-1]


def _get_windows_whitelisted_users():
    # Only principals whose access is unavoidable belong here: SYSTEM and the
    # Administrators group can bypass the ACL anyway (take-ownership,
    # SeBackupPrivilege), so reporting them would be noise nobody can act on.
    #
    # This list originally came from a consultation with prodsec (CASEC-9627) and also
    # contained "Domain Users" and "Network"; both were dropped for SNOW-3649686.
    # "Domain Users" is the primary group of every account in the domain — the
    # AD-scoped equivalent of Everyone — and "Network" (NT AUTHORITY\NETWORK,
    # S-1-5-2) is any principal authenticated over the network. Neither has
    # ACL-bypassing authority, so a read ACE for either is a real and revocable
    # exposure of the cleartext credentials in the config file.
    return [
        "SYSTEM",
        "Administrators",
        "Administrator",
        "Domain Admins",
        _strip_windows_domain_prefix(_get_windows_username()),
    ]


def _icacls(path: Path, *args: str, failure_msg: str | None = None) -> str:
    import subprocess

    try:
        result = subprocess.run(
            [_ICACLS, str(path), *args],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired as e:
        raise CliError(
            f"icacls.exe timed out on {path}. "
            f"If the path is on a network share, consider moving your "
            f"Snowflake config to a local directory."
        ) from e
    except FileNotFoundError as e:
        raise CliError(
            f"{_ICACLS} not found. "
            f"Config file permission checks require icacls.exe to be available."
        ) from e
    if result.returncode != 0:
        detail = sanitize_for_terminal(result.stderr.strip())
        msg = (
            f"{failure_msg}: {detail}"
            if failure_msg
            else f"icacls.exe failed on {path}: {detail}"
        )
        raise CliError(msg)
    return result.stdout


def _windows_permissions_are_denied(permission_codes: str) -> bool:
    # according to https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/icacls
    return "(DENY)" in permission_codes or "(N)" in permission_codes


def windows_get_not_whitelisted_users_with_access(file_path: Path) -> List[str]:
    # according to https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/icacls
    # ACE lines take two forms:
    #   "DOMAIN\user:(perms)"  — domain/built-in accounts (have a backslash prefix)
    #   "user:(perms)"         — well-known SIDs (Everyone, CREATOR OWNER, etc.)
    # (?:.*\\)? makes the domain-prefix optional so both forms are matched.
    # (?P<user>\S[^\\:\r\n]*?) excludes backslashes from the user capture so the
    # domain prefix is always consumed by the preceding group, never bled into the name.
    # [^\r\n]+ captures the full permissions token to end-of-line, including
    # lowercase "special access" entries that [(A-Z),]+ would truncate.
    # Unparsable entries (e.g. "(special access:)") are conservatively treated
    # as grants — the user is flagged as having access.
    icacls_output_regex = rf"({re.escape(str(file_path))})?(?:.*\\)?\s*(?P<user>\S[^\\:\r\n]*?)\s*:(?P<permissions>[^\r\n]+)"
    whitelisted_users = _get_windows_whitelisted_users()
    whitelisted_casefold = {w.casefold() for w in whitelisted_users}

    users_with_access = []
    for permission in re.finditer(icacls_output_regex, _icacls(file_path)):
        if (permission.group("user").casefold() not in whitelisted_casefold) and (
            not _windows_permissions_are_denied(permission.group("permissions"))
        ):
            users_with_access.append(permission.group("user"))
    # sorted() for determinism — list(set(...)) would vary with the hash seed.
    return sorted(set(users_with_access))


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
    Returns True when SF_SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION or the
    SPCS-injected equivalent is set to a truthy value. The public var takes
    precedence; an unparsable value is treated as False.
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
    username = _get_windows_username()
    log.info("Setting strict permissions on %s for user %s", path, username)
    # (OI)(CI) are directory-propagation flags; on a plain file they produce
    # an inherit-only ACE (applies to children, not the file itself), leaving
    # the file inaccessible after /inheritance:r strips all inherited ACEs.
    perms = "(OI)(CI)(F)" if path.is_dir() else "(F)"
    _icacls(
        path,
        "/inheritance:r",
        "/grant:r",
        f"{username}:{perms}",
        failure_msg=f"Failed to set strict permissions on {path}",
    )

    for user in windows_get_not_whitelisted_users_with_access(path):
        log.info("Removing permissions of user %s from file %s", user, path)
        _icacls(
            path,
            "/remove:g",
            user,
            failure_msg=f"Failed to remove ACE for {user} from {path}",
        )


def restrict_file_permissions(file_path: Path) -> None:
    if IS_WINDOWS:
        _windows_restrict_file_permissions(file_path)
    else:
        _unix_restrict_file_permissions(file_path)


def get_windows_permission_warning(file: Path) -> str:
    unauthorized_users = [
        sanitize_for_terminal(u)
        for u in windows_get_not_whitelisted_users_with_access(file)
    ]
    username = sanitize_for_terminal(_get_windows_username())
    perms = "(OI)(CI)(F)" if file.is_dir() else "(F)"
    remove_args = " ".join(f'/remove:g "{u}"' for u in unauthorized_users)
    fix_cmd = (
        f'icacls "{file}" /inheritance:r /grant:r "{username}:{perms}" {remove_args}'
    )
    return (
        f"Unauthorized users have access to configuration file {file}.\n"
        f" * Users with unauthorized access: {', '.join(unauthorized_users)}\n"
        f" * To restrict permissions, run:\n"
        f"   {fix_cmd}"
    )
