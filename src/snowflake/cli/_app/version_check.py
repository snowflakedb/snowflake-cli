import json
import time
from collections import namedtuple
from warnings import warn

import requests
from packaging.version import Version
from snowflake.cli.__about__ import VERSION
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.config_manager import CONFIG_MANAGER

REPOSITORY_URL = "https://pypi.org/pypi/snowflake-cli/json"

VersionAndTime = namedtuple("VersionAndTime", ["version", "upload_time"])


def get_new_version_msg() -> str | None:
    last = _VersionCache().get_last_version()
    current = Version(VERSION)
    new_version_available = last and last.version and Version(last.version) > current
    if new_version_available:
        return f"\nNew version of Snowflake CLI available. Newest: {last.version}, current: {VERSION}\n"  # type:ignore
    return None


def show_new_version_banner_callback(msg):
    def _callback(*args, **kwargs):
        if msg and not get_cli_context().silent:
            warn(msg)

    return _callback


class _VersionCache:
    _last_time = "last_time_check"
    _version = "version"
    _upload_time = "upload_time"
    _version_cache_file = SecurePath(
        CONFIG_MANAGER.file_path.parent / ".cli_version.cache"
    )

    def __init__(self):
        self._cache_file = _VersionCache._version_cache_file

    def _save_latest_version(self, version_and_time: VersionAndTime) -> None:
        data = {
            _VersionCache._last_time: time.time(),
            _VersionCache._version: str(version_and_time.version),
            _VersionCache._upload_time: version_and_time.upload_time,
        }
        self._cache_file.write_text(json.dumps(data))

    @staticmethod
    def _get_version_from_pypi() -> VersionAndTime:
        headers = {"Content-Type": "application/vnd.pypi.simple.v1+json"}
        response = requests.get(REPOSITORY_URL, headers=headers, timeout=3)
        response.raise_for_status()
        data = response.json()
        version = data["info"]["version"]
        upload_time = None
        try:
            if version:
                upload_time = data["releases"][version][0]["upload_time"]
        except (KeyError, IndexError):
            upload_time = None
        return VersionAndTime(version, upload_time)

    def _update_latest_version(self) -> VersionAndTime:
        version_and_time = self._get_version_from_pypi()
        if version_and_time.version:
            self._save_latest_version(version_and_time)
        return version_and_time

    def _read_latest_version(self) -> VersionAndTime:
        if self._cache_file.exists():
            data = json.loads(self._cache_file.read_text(file_size_limit_mb=1))
            now = time.time()
            if data[_VersionCache._last_time] > now - 60 * 60:
                version = data[_VersionCache._version]
                upload_time = data.get(_VersionCache._upload_time, None)
                return VersionAndTime(version, upload_time)

        return self._update_latest_version()

    def get_last_version(self) -> VersionAndTime | None:
        try:
            return self._read_latest_version()
        except:  # anything, this it not crucial feature
            return None
