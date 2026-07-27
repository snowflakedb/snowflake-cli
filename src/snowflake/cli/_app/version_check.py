import json
import logging
import time
from dataclasses import dataclass
from warnings import warn

import requests
from packaging.version import Version
from snowflake.cli.__about__ import VERSION
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.config import (
    CLI_SECTION,
    IGNORE_NEW_VERSION_WARNING_KEY,
    get_config_bool_value,
    get_config_manager,
)
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

REPOSITORY_URL_PIP = "https://pypi.org/pypi/snowflake-cli/json"
REPOSITORY_URL_BREW = "https://formulae.brew.sh/api/formula/snowflake-cli.json"

# Set by an explicit version-reporting command (e.g. `snow helpers check-version`)
# so the passive upgrade banner does not duplicate the command's own output.
_suppress_new_version_banner = False

# How often to refresh the version cache (seconds)
VERSION_CACHE_REFRESH_INTERVAL = 60 * 60  # 1 hour
# How often to show the new version message (seconds)
NEW_VERSION_MESSAGE_INTERVAL = 60 * 60 * 24 * 7  # 1 week


def should_ignore_new_version_warning() -> bool:
    return get_config_bool_value(
        CLI_SECTION, key=IGNORE_NEW_VERSION_WARNING_KEY, default=False
    )


def was_warning_shown_recently(last_time_shown: float | int | None) -> bool:
    """
    Returns True if the new version warning was shown recently (within the interval),
    meaning we should NOT show the warning again yet.
    """
    if not last_time_shown:
        return False
    now = time.time()
    return last_time_shown >= now - NEW_VERSION_MESSAGE_INTERVAL


def get_new_version_msg() -> str | None:
    if should_ignore_new_version_warning():
        return None
    cache = _VersionCache()
    last_version = cache.get_last_version()
    last_time_shown = cache.get_last_time_shown()
    current_version = Version(VERSION)
    if (
        last_version
        and last_version > current_version
        and not was_warning_shown_recently(last_time_shown)
    ):
        cache.update_last_time_shown()
        return f"\nNew version of Snowflake CLI available. Newest: {last_version}, current: {VERSION}\n"
    return None


@dataclass
class CliVersionInfo:
    current_version: str
    latest_version: str | None
    update_available: bool


def get_version_info(*, force_refresh: bool = False) -> CliVersionInfo:
    """Resolve the current and latest available Snowflake CLI versions.

    Unlike the passive upgrade banner, this always reports the result: it
    ignores the ``ignore_new_version_warning`` config and the "shown recently"
    throttle so an explicit check is never silenced. ``force_refresh`` bypasses
    the on-disk cache and queries the package repositories directly.
    """
    latest_version = _VersionCache().get_last_version(force_refresh=force_refresh)
    update_available = bool(latest_version and latest_version > Version(VERSION))
    return CliVersionInfo(
        current_version=VERSION,
        latest_version=str(latest_version) if latest_version else None,
        update_available=update_available,
    )


def reset_new_version_banner_suppression() -> None:
    """Reset banner suppression; call once per app build."""
    global _suppress_new_version_banner
    _suppress_new_version_banner = False


def suppress_new_version_banner() -> None:
    """Mute the passive upgrade banner for the current invocation.

    Used by commands that already report version status themselves so the
    banner does not duplicate their output.
    """
    global _suppress_new_version_banner
    _suppress_new_version_banner = True


def show_new_version_banner_callback(msg):
    def _callback(*args, **kwargs):
        if msg and not _suppress_new_version_banner and not get_cli_context().silent:
            warn(msg)

    return _callback


class _VersionCache:
    _last_time = "last_time_check"
    _version = "version"
    _last_time_shown = "last_time_shown"

    @property
    def _version_cache_file(self):
        """Get version cache file path with lazy evaluation."""
        return SecurePath(get_config_manager().file_path.parent / ".cli_version.cache")

    def __init__(self):
        self._cache_file = self._version_cache_file

    def _save_latest_version(self, version: str):
        data = {
            _VersionCache._last_time: time.time(),
            _VersionCache._version: str(version),
        }
        if self._cache_file.exists():
            try:
                old_data = json.loads(self._cache_file.read_text(file_size_limit_mb=1))
                if _VersionCache._last_time_shown in old_data:
                    data[_VersionCache._last_time_shown] = old_data[
                        _VersionCache._last_time_shown
                    ]
            except Exception:
                pass
        self._cache_file.parent.mkdir(parents=True, exist_ok=True)
        self._cache_file.write_text(json.dumps(data))

    def update_last_time_shown(self):
        if self._cache_file.exists():
            try:
                data = json.loads(self._cache_file.read_text(file_size_limit_mb=1))
            except Exception:
                data = {}
        else:
            data = {}
        data[_VersionCache._last_time_shown] = time.time()
        self._cache_file.parent.mkdir(parents=True, exist_ok=True)
        self._cache_file.write_text(json.dumps(data))

    @staticmethod
    def _get_version_from_pypi() -> str | None:
        headers = {"Content-Type": "application/vnd.pypi.simple.v1+json"}
        response = requests.get(REPOSITORY_URL_PIP, headers=headers, timeout=3)
        response.raise_for_status()
        return response.json().get("info", {}).get("version", None)

    @staticmethod
    def _get_version_from_brew() -> str | None:
        response = requests.get(REPOSITORY_URL_BREW, timeout=3)
        response.raise_for_status()
        return response.json().get("versions", {}).get("stable", None)

    def _update_latest_version(self) -> Version | None:
        # Use brew version, fallback to pypi if brew is not available.
        # Brew repo takes longer to propagate the upgrade and is triggered later in our release process,
        # we treat it as "slowest point" and determinant that the released version is available.
        version = self._get_version_from_brew() or self._get_version_from_pypi()
        if version is None:
            return None
        self._save_latest_version(version)
        return Version(version)

    def _read_latest_version(self, force_refresh: bool = False) -> Version | None:
        if not force_refresh and self._cache_file.exists():
            data = json.loads(self._cache_file.read_text(file_size_limit_mb=1))
            now = time.time()
            if data[_VersionCache._last_time] > now - VERSION_CACHE_REFRESH_INTERVAL:
                return Version(data[_VersionCache._version])
        return self._update_latest_version()

    def get_last_version(self, force_refresh: bool = False) -> Version | None:
        try:
            return self._read_latest_version(force_refresh=force_refresh)
        except Exception:  # anything, this is not a crucial feature
            # Swallowed so a version check never breaks a command, but logged
            # so --debug/--verbose can reveal the real cause (network error,
            # HTTP 5xx, malformed JSON, ...) instead of only a generic message.
            log.debug("Failed to determine the latest CLI version", exc_info=True)
            return None

    def get_last_time_shown(self) -> float | int | None:
        if self._cache_file.exists():
            try:
                data = json.loads(self._cache_file.read_text(file_size_limit_mb=1))
                return data.get(_VersionCache._last_time_shown, 0)
            except Exception:
                return None
        return None
