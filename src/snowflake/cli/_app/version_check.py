import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass

import requests
from packaging.version import Version
from snowflake.cli.__about__ import VERSION
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.config import (
    CLI_SECTION,
    IGNORE_NEW_VERSION_WARNING_KEY,
    get_config_bool_value,
    get_config_manager,
    get_file_io_encoding,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

REPOSITORY_URL_PIP = "https://pypi.org/pypi/snowflake-cli/json"
REPOSITORY_URL_BREW = "https://formulae.brew.sh/api/formula/snowflake-cli.json"

# Set by an explicit version-reporting command (e.g. `snow helpers check-version`)
# so the passive upgrade banner does not duplicate the command's own output.
_suppress_new_version_banner = False
_banner_shown = False

# How often to refresh the version cache (seconds)
VERSION_CACHE_REFRESH_INTERVAL = 60 * 60  # 1 hour
# How often to show the new version message (seconds)
NEW_VERSION_MESSAGE_INTERVAL = 60 * 60 * 24 * 7  # 1 week
# Per-request timeout when querying PyPI/Homebrew (seconds)
VERSION_FETCH_TIMEOUT = 1
# Max wait for an in-flight background refresh before showing the passive banner
VERSION_REFRESH_JOIN_TIMEOUT = 2 * VERSION_FETCH_TIMEOUT + 0.5

_refresh_lock = threading.Lock()
_refresh_thread: threading.Thread | None = None


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


def start_background_refresh() -> None:
    """Start a daemon thread to refresh the version cache if needed."""
    if should_ignore_new_version_warning():
        return
    _VersionCache().schedule_background_refresh()


def wait_for_refresh(timeout: float = VERSION_REFRESH_JOIN_TIMEOUT) -> None:
    """Block until the background refresh thread finishes or times out."""
    thread = _refresh_thread
    if thread is not None and thread.is_alive():
        thread.join(timeout=timeout)


def reset_background_refresh_thread() -> None:
    """Wait for and clear any in-flight background refresh thread."""
    global _refresh_thread
    wait_for_refresh()
    _refresh_thread = None


def get_new_version_msg() -> str | None:
    try:
        if should_ignore_new_version_warning():
            return None
        wait_for_refresh()
        cache = _VersionCache()
        last_version = cache.get_cached_version()
        last_time_shown = cache.get_last_time_shown()
        current_version = Version(VERSION)
        if (
            last_version
            and last_version > current_version
            and not was_warning_shown_recently(last_time_shown)
        ):
            cache.update_last_time_shown()
            newest = sanitize_for_terminal(str(last_version))
            current = sanitize_for_terminal(VERSION)
            return (
                f"New version of Snowflake CLI available. "
                f"Newest: {newest}, current: {current}"
            )
        return None
    except Exception:
        log.debug("Failed to determine new version message", exc_info=True)
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


def reset_banner_display_state() -> None:
    """Reset per-invocation banner state before running a command."""
    global _banner_shown, _suppress_new_version_banner
    _banner_shown = False
    _suppress_new_version_banner = False


def reset_new_version_banner_suppression() -> None:
    """Reset banner suppression; alias kept for callers outside ``__call__``."""
    reset_banner_display_state()


def suppress_new_version_banner() -> None:
    """Mute the passive upgrade banner for the current invocation.

    Used by commands that already report version status themselves so the
    banner does not duplicate their output.
    """
    global _suppress_new_version_banner
    _suppress_new_version_banner = True


def record_version_check_displayed() -> None:
    """Record that the user saw an explicit version check.

    Uses the same throttle timestamp as the passive upgrade banner so a
    successful ``snow helpers check-version`` does not immediately re-prompt
    on the next command.
    """
    try:
        _VersionCache().update_last_time_shown()
    except Exception:
        log.debug("Failed to record version check display time", exc_info=True)


def maybe_show_new_version_banner() -> None:
    """Show the passive upgrade banner if an update is available."""
    global _banner_shown
    if _banner_shown or _suppress_new_version_banner or get_cli_context().silent:
        return
    msg = get_new_version_msg()
    if msg:
        _banner_shown = True
        cli_console.stderr_warning(msg)


def show_new_version_banner_callback(*args, **kwargs):
    """Result callback for the root command; shows the banner after execution."""
    maybe_show_new_version_banner()


class _VersionCache:
    _last_time = "last_time_check"
    _version = "version"
    _last_time_shown = "last_time_shown"
    _last_attempt = "last_attempt"

    @property
    def _version_cache_file(self):
        """Get version cache file path with lazy evaluation."""
        return SecurePath(get_config_manager().file_path.parent / ".cli_version.cache")

    def __init__(self):
        self._cache_file = self._version_cache_file

    def _atomic_write(self, content: str) -> None:
        """Write content to the cache file atomically via a temp-file + rename."""
        encoding = get_file_io_encoding()
        dest = self._cache_file.path
        SecurePath(dest).parent.mkdir(parents=True, exist_ok=True)
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(dir=dest.parent, suffix=".tmp")
            with os.fdopen(fd, "w", encoding=encoding) as f:
                f.write(content)
            os.replace(tmp_path, dest)
            tmp_path = None
        except OSError:
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            log.debug(
                "Atomic cache write failed, falling back to direct write",
                exc_info=True,
            )
            try:
                self._cache_file.write_text(content)
            except OSError:
                log.debug("Direct cache write also failed", exc_info=True)

    def _read_cache_data(self) -> dict | None:
        if not self._cache_file.exists():
            return None
        try:
            parsed = json.loads(self._cache_file.read_text(file_size_limit_mb=1))
            if not isinstance(parsed, dict):
                log.debug(
                    "Version cache has unexpected JSON type: %s",
                    type(parsed).__name__,
                )
                return None
            return parsed
        except Exception:
            log.debug("Failed to read version cache", exc_info=True)
            return None

    def schedule_background_refresh(self) -> None:
        cache_data = self._read_cache_data()
        if self._is_fresh(cache_data) or not self._refresh_is_overdue(cache_data):
            return

        def _run() -> None:
            try:
                self._mark_attempt()
                self._update_latest_version()
            except Exception:
                log.debug("Background version refresh failed", exc_info=True)

        global _refresh_thread
        with _refresh_lock:
            if _refresh_thread is not None and _refresh_thread.is_alive():
                return
            _refresh_thread = threading.Thread(
                target=_run,
                name="version-cache-refresh",
                daemon=True,
            )
            _refresh_thread.start()

    def _is_fresh(self, data: dict | None) -> bool:
        if not data:
            return False
        try:
            return (
                data[_VersionCache._last_time]
                > time.time() - VERSION_CACHE_REFRESH_INTERVAL
            )
        except (KeyError, TypeError):
            return False

    def _refresh_is_overdue(self, data: dict | None) -> bool:
        if data is None:
            return True
        last_attempt = data.get(_VersionCache._last_attempt, 0)
        return time.time() - last_attempt >= VERSION_CACHE_REFRESH_INTERVAL

    def _mark_attempt(self) -> None:
        try:
            data = self._read_cache_data() or {}
            data[_VersionCache._last_attempt] = time.time()
            self._atomic_write(json.dumps(data))
        except Exception:
            log.debug("Failed to write refresh attempt timestamp", exc_info=True)

    def _save_latest_version(self, version: str):
        data = {
            _VersionCache._last_time: time.time(),
            _VersionCache._version: str(version),
        }
        old_data = self._read_cache_data()
        if old_data:
            if _VersionCache._last_time_shown in old_data:
                data[_VersionCache._last_time_shown] = old_data[
                    _VersionCache._last_time_shown
                ]
            if _VersionCache._last_attempt in old_data:
                data[_VersionCache._last_attempt] = old_data[
                    _VersionCache._last_attempt
                ]
        self._atomic_write(json.dumps(data))

    def update_last_time_shown(self):
        data = self._read_cache_data() or {}
        data[_VersionCache._last_time_shown] = time.time()
        self._atomic_write(json.dumps(data))

    @staticmethod
    def _get_version_from_pypi() -> str | None:
        headers = {"Content-Type": "application/vnd.pypi.simple.v1+json"}
        response = requests.get(
            REPOSITORY_URL_PIP,
            headers=headers,
            timeout=VERSION_FETCH_TIMEOUT,
        )
        response.raise_for_status()
        return response.json().get("info", {}).get("version", None)

    @staticmethod
    def _get_version_from_brew() -> str | None:
        response = requests.get(
            REPOSITORY_URL_BREW,
            timeout=VERSION_FETCH_TIMEOUT,
        )
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

    def _read_fresh_cached_version(self) -> Version | None:
        data = self._read_cache_data()
        if not data:
            return None
        try:
            if (
                data[_VersionCache._last_time]
                > time.time() - VERSION_CACHE_REFRESH_INTERVAL
            ):
                return Version(data[_VersionCache._version])
        except (KeyError, TypeError, ValueError):
            log.debug(
                "Version cache is unreadable, refreshing from network",
                exc_info=True,
            )
        return None

    def _read_latest_version(self, force_refresh: bool = False) -> Version | None:
        if not force_refresh:
            cached = self._read_fresh_cached_version()
            if cached is not None:
                return cached
        return self._update_latest_version()

    def get_cached_version(self) -> Version | None:
        """Return the cached version without touching the network."""
        data = self._read_cache_data()
        if not data:
            return None
        try:
            return Version(data[_VersionCache._version])
        except (KeyError, TypeError, ValueError):
            return None

    def get_last_version(self, force_refresh: bool = False) -> Version | None:
        try:
            if force_refresh:
                return self._update_latest_version()
            cache_data = self._read_cache_data()
            if not self._is_fresh(cache_data):
                wait_for_refresh()
                cached = self._read_fresh_cached_version()
                if cached is not None:
                    return cached
            return self._read_latest_version(force_refresh=False)
        except Exception:  # anything, this is not a crucial feature
            # Swallowed so a version check never breaks a command, but logged
            # so --debug/--verbose can reveal the real cause (network error,
            # HTTP 5xx, malformed JSON, ...) instead of only a generic message.
            log.debug("Failed to determine the latest CLI version", exc_info=True)
            return None

    def get_last_time_shown(self) -> float | int | None:
        data = self._read_cache_data()
        if data is None:
            return None
        return data.get(_VersionCache._last_time_shown, 0)
