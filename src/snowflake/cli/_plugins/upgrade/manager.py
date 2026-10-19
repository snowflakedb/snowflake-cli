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
import platform
from dataclasses import dataclass
from typing import Optional, Protocol

from packaging.version import InvalidVersion, Version
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._plugins.upgrade.layout import (
    ManagedLayout,
    clear_reverted_from,
    write_reverted_from,
)
from snowflake.cli._plugins.upgrade.repo import HttpRepo
from snowflake.cli.api.config import (
    is_air_gapped,
    restrict_owned_default_config_files,
    take_upgrade_restricted_paths,
)
from snowflake.cli.api.exceptions import CliError

log = logging.getLogger(__name__)

STATUS_REFUSED = "refused"
STATUS_ALREADY_CURRENT = "already_current"
STATUS_NEW_MAJOR = "new_major"
STATUS_DRY_RUN = "dry_run"
STATUS_UPGRADED = "upgraded"
STATUS_REVERTED = "reverted"

INSTALL_SH_COMMAND = (
    "curl -LsS https://sfc-repo.snowflakecomputing.com/snowflake-cli/install.sh | sh"
)
INSTALL_PS1_COMMAND = (
    "irm https://sfc-repo.snowflakecomputing.com/snowflake-cli/install.ps1 | iex"
)

DOWNLOAD_NOT_AVAILABLE = (
    "Installing a newer snowflake-managed version is not available in this preview."
)


class VersionSource(Protocol):
    """Latest published snowflake-managed version. HttpRepo also materializes tarballs."""

    def latest_version(self) -> str:
        ...


class UnimplementedVersionSource:
    def latest_version(self) -> str:
        raise CliError(
            "Fetching the latest snowflake-managed version is not available in this preview."
        )


_version_source: Optional[VersionSource] = None


def get_version_source() -> VersionSource:
    global _version_source
    if _version_source is None:
        _version_source = HttpRepo()
    return _version_source


def set_version_source(source: Optional[VersionSource]) -> None:
    global _version_source
    _version_source = source


def install_command(*, windows: Optional[bool] = None) -> str:
    is_windows = platform.system() == "Windows" if windows is None else windows
    return INSTALL_PS1_COMMAND if is_windows else INSTALL_SH_COMMAND


def parse_cli_version(value: str) -> Version:
    try:
        return Version(value)
    except InvalidVersion as exc:
        raise CliError(f"Invalid Snowflake CLI version {value!r}.") from exc


@dataclass(frozen=True)
class UpgradeDecision:
    payload: dict
    message: str


def _channel() -> str:
    return __about__.INSTALLATION_SOURCE.value


def _current_version() -> str:
    return __about__.VERSION


def refuse_result() -> UpgradeDecision:
    channel = _channel()
    command = install_command()
    message = (
        "snow upgrade can only be used with a direct install of Snowflake CLI.\n"
        f"This install is {channel}.\n"
        "\n"
        "Install using a direct install:\n"
        f"  {command}"
    )
    return UpgradeDecision(
        payload={
            "channel": channel,
            "status": STATUS_REFUSED,
            "install_command": command,
        },
        message=message,
    )


def evaluate_managed_upgrade(
    *, dry_run: bool, allow_major: bool = False
) -> UpgradeDecision:
    """Gate, version-compare, and (unless dry-run) download + retarget.

    ``allow_major`` is for auto-upgrade only. Manual ``snow upgrade`` keeps
    the default (same-major).
    """
    channel = _channel()
    current = _current_version()
    latest = get_version_source().latest_version()
    current_v = parse_cli_version(current)
    latest_v = parse_cli_version(latest)

    if latest_v.major > current_v.major and not allow_major:
        return UpgradeDecision(
            payload={
                "channel": channel,
                "status": STATUS_NEW_MAJOR,
                "from": current,
                "to": latest,
            },
            message=(
                f"A new major version is available ({current} → {latest}). "
                "snow upgrade installs same-major updates only."
            ),
        )

    if latest_v <= current_v:
        return UpgradeDecision(
            payload={
                "channel": channel,
                "status": STATUS_ALREADY_CURRENT,
                "from": current,
                "to": latest,
            },
            message=f"Already current ({current}).",
        )

    if dry_run:
        return UpgradeDecision(
            payload={
                "channel": channel,
                "status": STATUS_DRY_RUN,
                "from": current,
                "to": latest,
            },
            message=f"Would upgrade {current} → {latest}.",
        )

    return apply_upgrade(current=current, latest=latest)


def _append_restriction_note(message: str, paths: list) -> str:
    if not paths:
        return message
    rendered = ", ".join(str(path) for path in paths)
    return f"{message} Restricted permissions on {rendered}."


def append_upgrade_restriction_note(message: str) -> str:
    """Add files ``config_init`` tightened so ``snow upgrade`` could start."""
    return _append_restriction_note(message, take_upgrade_restricted_paths())


def apply_upgrade(*, current: str, latest: str) -> UpgradeDecision:
    source = get_version_source()
    materialize = getattr(source, "materialize", None)
    if not callable(materialize):
        raise CliError(DOWNLOAD_NOT_AVAILABLE)

    layout = ManagedLayout()
    materialize(latest, layout)
    # config_init already tightened user-owned defaults so this command could
    # start. Restrict again for auto-upgrade, which retargets before that check.
    restricted = restrict_owned_default_config_files()
    shim_target = layout.retarget(latest)
    # The shim already points at ``latest``. GC and stamp clear are cleanup;
    # a failure here must not report the upgrade as failed.
    _best_effort_after_retarget(layout)
    message = f"Upgraded {current} → {latest}."
    message = _append_restriction_note(message, restricted)
    message = append_upgrade_restriction_note(message)
    return UpgradeDecision(
        payload={
            "channel": _channel(),
            "status": STATUS_UPGRADED,
            "from": current,
            "to": latest,
            "shim_target": str(shim_target),
        },
        message=message,
    )


def _best_effort_after_retarget(layout: ManagedLayout) -> None:
    """Run post-retarget cleanup without failing an upgrade that already landed."""
    try:
        layout.gc()
    except Exception:
        log.debug("GC after retarget failed; shim already moved", exc_info=True)
    try:
        clear_reverted_from(layout)
    except Exception:
        log.debug(
            "Clearing reverted-from after retarget failed; shim already moved",
            exc_info=True,
        )


def apply_revert(*, dry_run: bool) -> UpgradeDecision:
    """Retarget the shim at the previous snowflake-managed version.

    Does not fetch sfc-repo and does not touch ``~/.snowflake`` config.
    """
    channel = _channel()
    layout = ManagedLayout()
    previous = layout.previous_version()
    if previous is None:
        raise CliError(
            "No previous snowflake-managed version is recorded. "
            "Revert is only available after a successful upgrade."
        )
    current = layout.current_version() or _current_version()
    if dry_run:
        return UpgradeDecision(
            payload={
                "channel": channel,
                "status": STATUS_DRY_RUN,
                "from": current,
                "to": previous,
            },
            message=f"Would revert {current} → {previous}.",
        )

    shim_target = layout.revert()
    write_reverted_from(layout, current)
    return UpgradeDecision(
        payload={
            "channel": channel,
            "status": STATUS_REVERTED,
            "from": current,
            "to": previous,
            "shim_target": str(shim_target),
        },
        message=f"Reverted {current} → {previous}.",
    )


def plan_upgrade(*, dry_run: bool, revert: bool = False) -> UpgradeDecision:
    if is_air_gapped():
        raise CliError(
            "Upgrades are disabled because cli.air_gapped is set. "
            "Unset cli.air_gapped or SNOWFLAKE_CLI_AIR_GAPPED to upgrade from a networked host."
        )
    if __about__.INSTALLATION_SOURCE != CLIInstallationSource.SNOWFLAKE_MANAGED:
        return refuse_result()
    if revert:
        return apply_revert(dry_run=dry_run)
    return evaluate_managed_upgrade(dry_run=dry_run)
