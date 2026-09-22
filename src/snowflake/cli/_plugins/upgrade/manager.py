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

import platform
from dataclasses import dataclass
from typing import Optional, Protocol

from packaging.version import InvalidVersion, Version
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._plugins.upgrade.layout import ManagedLayout
from snowflake.cli._plugins.upgrade.repo import HttpRepo
from snowflake.cli.api.exceptions import CliError

STATUS_REFUSED = "refused"
STATUS_ALREADY_CURRENT = "already_current"
STATUS_NEW_MAJOR = "new_major"
STATUS_DRY_RUN = "dry_run"
STATUS_UPGRADED = "upgraded"

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
        "snow upgrade can only be used with the snowflake-managed distribution of Snowflake CLI.\n"
        f"This install is {channel}.\n"
        "\n"
        "Install the snowflake-managed distribution:\n"
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


def evaluate_managed_upgrade(*, dry_run: bool) -> UpgradeDecision:
    """Gate, version-compare, and (unless dry-run) download + retarget."""
    channel = _channel()
    current = _current_version()
    latest = get_version_source().latest_version()
    current_v = parse_cli_version(current)
    latest_v = parse_cli_version(latest)

    if latest_v.major > current_v.major:
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


def apply_upgrade(*, current: str, latest: str) -> UpgradeDecision:
    source = get_version_source()
    materialize = getattr(source, "materialize", None)
    if not callable(materialize):
        raise CliError(DOWNLOAD_NOT_AVAILABLE)

    layout = ManagedLayout()
    materialize(latest, layout)
    shim_target = layout.retarget(latest)
    return UpgradeDecision(
        payload={
            "channel": _channel(),
            "status": STATUS_UPGRADED,
            "from": current,
            "to": latest,
            "shim_target": str(shim_target),
        },
        message=f"Upgraded {current} → {latest}.",
    )


def plan_upgrade(*, dry_run: bool) -> UpgradeDecision:
    if __about__.INSTALLATION_SOURCE != CLIInstallationSource.SNOWFLAKE_MANAGED:
        return refuse_result()
    return evaluate_managed_upgrade(dry_run=dry_run)
