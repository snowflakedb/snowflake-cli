import json
import time
from warnings import warn

import requests
from packaging.version import Version
from snowflake.cli.__about__ import VERSION
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.config_manager import CONFIG_MANAGER

REPOSITORY_URL = "https://pypi.org/pypi/snowflake-cli/json"


def get_new_version_msg() -> str | None:
    last = _VersionCache().get_last_version()
    current = Version(VERSION)
    if last and last > current:
        return f"\nNew version of Snowflake CLI available. Newest: {last}, current: {VERSION}\n"
    return None


def show_new_version_banner_callback(msg):
    def _callback(*args, **kwargs):
        if msg and not get_cli_context().silent:
            warn(msg)

    return _callback


class _VersionCache:
    _last_time = "last_time_check"
    _version = "version"
    _version_cache_file = SecurePath(
        CONFIG_MANAGER.file_path.parent / ".cli_version.cache"
    )

    def __init__(self):
        self._cache_file = _VersionCache._version_cache_file

    def _save_latest_version(self, version: str):
        data = {
            _VersionCache._last_time: time.time(),
            _VersionCache._version: str(version),
        }
        self._cache_file.write_text(json.dumps(data))

    @staticmethod
    def _get_version_from_pypi() -> str | None:
        headers = {"Content-Type": "application/vnd.pypi.simple.v1+json"}
        response = requests.get(REPOSITORY_URL, headers=headers, timeout=3)
        response.raise_for_status()
        return response.json()["info"]["version"]

    def _update_latest_version(self) -> Version | None:
        version = self._get_version_from_pypi()
        if version is None:
            return None
        self._save_latest_version(version)
        return Version(version)

    def _read_latest_version(self) -> Version | None:
        if self._cache_file.exists():
            data = json.loads(self._cache_file.read_text(file_size_limit_mb=1))
            now = time.time()
            if data[_VersionCache._last_time] > now - 60 * 60:
                return Version(data[_VersionCache._version])

        return self._update_latest_version()

    def get_last_version(self) -> Version | None:
        try:
            return self._read_latest_version()
        except:  # anything, this it not crucial feature
            return None
