import json
import time
from contextlib import contextmanager
from io import BytesIO
from itertools import cycle
from unittest import mock
from unittest.mock import patch

import pytest
import tomlkit
from packaging.version import Version
from requests import Response
from snowflake.cli._app.version_check import (
    NEW_VERSION_MESSAGE_INTERVAL,
    VERSION_CACHE_REFRESH_INTERVAL,
    VERSION_FETCH_TIMEOUT,
    _VersionCache,
    get_new_version_msg,
    get_version_info,
    record_version_check_displayed,
    reset_background_refresh_thread,
    wait_for_refresh,
    was_warning_shown_recently,
)
from snowflake.cli.api.config import config_init
from snowflake.cli.api.secure_path import SecurePath

_WARNING_MESSAGE = (
    "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
)
_PATCH_VERSION = ["snowflake.cli._app.version_check.VERSION", "1.0.0"]
_PATCH_CACHED_VERSION = [
    "snowflake.cli._app.version_check._VersionCache.get_cached_version",
    lambda _self: Version("2.0.0"),
]
_PATCH_LAST_VERSION = [
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _self, force_refresh=False: Version("2.0.0"),
]


class _ImmediateThread:
    """Run background refresh inline so thread scheduling cannot flake tests."""

    def __init__(self, target=None, name=None, daemon=None):
        self._target = target

    def start(self):
        if self._target:
            self._target()

    def is_alive(self):
        return False

    def join(self, timeout=None):
        pass


@contextmanager
def _immediate_background_refresh():
    with mock.patch(
        "snowflake.cli._app.version_check.threading.Thread", _ImmediateThread
    ):
        yield


@pytest.fixture(autouse=True)
def mock_last_time_shown():
    """
    Mock the last time the warning was shown to be in the past, so that the warning is shown.
    """
    with mock.patch(
        "snowflake.cli._app.version_check._VersionCache.get_last_time_shown",
        return_value=time.time() - NEW_VERSION_MESSAGE_INTERVAL - 10,
    ) as mock_last_time_shown:
        yield mock_last_time_shown


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_banner_shows_up_in_help(build_runner, test_snowcli_config, capsys):
    runner = build_runner()
    args = ["--config-file", str(test_snowcli_config), "--help"]
    runner.app(args, standalone_mode=False, prog_name="snow")
    assert _WARNING_MESSAGE in capsys.readouterr().err


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_banner_shows_up_in_version(build_runner, test_snowcli_config, capsys):
    runner = build_runner()
    args = ["--config-file", str(test_snowcli_config), "--version"]
    runner.app(args, standalone_mode=False, prog_name="snow")
    assert _WARNING_MESSAGE in capsys.readouterr().err


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_banner_shows_up_in_info(build_runner, test_snowcli_config, capsys):
    runner = build_runner()
    args = ["--config-file", str(test_snowcli_config), "--info"]
    runner.app(args, standalone_mode=False, prog_name="snow")
    assert _WARNING_MESSAGE in capsys.readouterr().err


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_banner_shows_up_in_command_invocation(
    build_runner, test_snowcli_config, capsys
):
    runner = build_runner()
    args = [
        "--config-file",
        str(test_snowcli_config),
        "connection",
        "set-default",
        "default",
    ]
    runner.app(args, standalone_mode=False, prog_name="snow")
    assert _WARNING_MESSAGE in capsys.readouterr().err


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_banner_do_not_shows_up_if_silent(build_runner, test_snowcli_config, capsys):
    runner = build_runner()
    args = [
        "--config-file",
        str(test_snowcli_config),
        "connection",
        "set-default",
        "default",
        "--silent",
    ]
    runner.app(args, standalone_mode=False, prog_name="snow")
    assert _WARNING_MESSAGE not in capsys.readouterr().err


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_check_version_command_suppresses_passive_banner(build_runner):
    """`snow helpers check-version` reports the update itself, so the passive
    banner must not also fire on top of its output."""
    result = build_runner().invoke(["helpers", "check-version"])
    assert result.exit_code == 0, result.output
    assert _WARNING_MESSAGE not in result.output


@patch("snowflake.cli._app.version_check._VersionCache.get_cached_version")
def test_version_check_exception_are_handled_safely(
    mock_get_cached_version, build_runner
):
    mock_get_cached_version.side_effect = Exception("Error")
    result = build_runner().invoke(["connection", "set-default", "default"])
    assert result.exit_code == 0
    assert _WARNING_MESSAGE not in result.output


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_get_new_version_msg_message_if_new_version_available():
    msg = get_new_version_msg()
    assert (
        msg == "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
    )


@patch(*_PATCH_VERSION)
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_cached_version",
    lambda _self: None,
)
def test_get_new_version_msg_does_not_show_message_if_no_new_version():
    assert get_new_version_msg() is None


@patch("snowflake.cli._app.version_check.VERSION", "3.0.0")
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_new_version_banner_does_not_show_message_if_local_version_is_newer():
    assert get_new_version_msg() is None


@patch("snowflake.cli._app.version_check.requests.get")
def test_get_version_from_pypi(mock_get):
    r = Response()
    r.status_code = 200
    r.raw = BytesIO(b'{"info": {"version": "1.2.3"}}')
    mock_get.return_value = r
    assert _VersionCache()._get_version_from_pypi() == "1.2.3"  # noqa
    mock_get.assert_called_once_with(
        "https://pypi.org/pypi/snowflake-cli/json",
        headers={"Content-Type": "application/vnd.pypi.simple.v1+json"},
        timeout=VERSION_FETCH_TIMEOUT,
    )


@patch("snowflake.cli._app.version_check.requests.get")
def test_get_version_from_brew(mock_get):
    r = Response()
    r.status_code = 200
    r.raw = BytesIO(b'{"versions": {"stable": "1.2.3"}}')
    mock_get.return_value = r
    assert _VersionCache()._get_version_from_brew() == "1.2.3"  # noqa
    mock_get.assert_called_once_with(
        "https://formulae.brew.sh/api/formula/snowflake-cli.json",
        timeout=VERSION_FETCH_TIMEOUT,
    )


@patch("snowflake.cli._app.version_check.time.time", lambda: 0.0)
def test_saves_latest_version(named_temporary_file):
    with named_temporary_file() as f:
        sf = SecurePath(f)
        vc = _VersionCache()
        vc._cache_file = sf  # noqa: SLF001
        vc._save_latest_version("1.2.3")  # noqa
        data = f.read_text()
    assert data == '{"last_time_check": 0.0, "version": "1.2.3"}'


@patch("snowflake.cli._app.version_check.time.time", lambda: 100.0)
def test_save_latest_version_preserves_last_attempt(named_temporary_file):
    with named_temporary_file() as f:
        sf = SecurePath(f)
        f.write_text(
            '{"last_time_check": 0.0, "version": "1.0.0", "last_attempt": 50.0}'
        )
        vc = _VersionCache()
        vc._cache_file = sf  # noqa: SLF001
        vc._save_latest_version("2.0.0")  # noqa: SLF001
        data = json.loads(f.read_text())
    assert data["version"] == "2.0.0"
    assert data["last_attempt"] == 50.0


@patch("snowflake.cli._app.version_check.time.time", lambda: 60)
def test_read_last_version(named_temporary_file):
    with named_temporary_file() as f:
        sf = SecurePath(f)
        vc = _VersionCache()
        vc._cache_file = sf  # noqa: SLF001
        f.write_text('{"last_time_check": 0.0, "version": "4.2.3"}')
        assert vc._read_latest_version() == Version("4.2.3")  # noqa


@pytest.mark.parametrize(
    "pypi_version, brew_version, expected",
    [
        ("8.0.0", "8.0.0", "8.0.0"),
        ("8.0.0", "8.0.1", "8.0.1"),
        ("8.0.1", "8.0.0", "8.0.0"),
        ("8.0.1", None, "8.0.1"),
        (None, "8.0.1", "8.0.1"),
        (None, None, None),
    ],
)
@patch("snowflake.cli._app.version_check._VersionCache._get_version_from_pypi")
@patch("snowflake.cli._app.version_check._VersionCache._get_version_from_brew")
@patch("snowflake.cli._app.version_check.time.time")
def test_read_last_version_and_updates_it(
    mock_time,
    mock_brew,
    mock_pypi,
    named_temporary_file,
    pypi_version,
    brew_version,
    expected,
):
    mock_time.side_effect = cycle((2 * 60 * 60, 120))
    mock_pypi.return_value = pypi_version
    mock_brew.return_value = brew_version

    with named_temporary_file() as f:
        f.write_text(old_data := '{"last_time_check": 0.0, "version": "1.2.3"}')
        sf = SecurePath(f)
        vc = _VersionCache()
        vc._cache_file = sf  # noqa: SLF001
        result = vc._read_latest_version()  # noqa
        data = sf.read_text(file_size_limit_mb=1)

        if expected:
            assert result == Version(expected)  # noqa
            assert data == f'{{"last_time_check": 120, "version": "{expected}"}}'
        else:
            assert result is None
            assert data == old_data


@patch("snowflake.cli._app.version_check.time.time", lambda: 60)
def test_corrupted_cache_falls_back_to_network(named_temporary_file):
    """A corrupted cache file must not prevent the network fetch from running."""
    with named_temporary_file() as f:
        f.write_text(
            '{"last_time_check": 59, "version": "4.2.3"}}'
        )  # trailing } breaks JSON
        vc = _VersionCache()
        vc._cache_file = SecurePath(f)  # noqa: SLF001
        with mock.patch.object(
            vc, "_update_latest_version", return_value=Version("5.0.0")
        ) as mock_update:
            result = vc._read_latest_version()  # noqa: SLF001
    assert result == Version("5.0.0")
    mock_update.assert_called_once()


@patch("snowflake.cli._app.version_check.time.time", lambda: 60)
def test_force_refresh_skips_fresh_cache(named_temporary_file):
    """force_refresh=True must bypass an in-interval cache and re-query."""
    with named_temporary_file() as f:
        # last_time_check=59 with now=60 is well within VERSION_CACHE_REFRESH_INTERVAL,
        # so without force_refresh this cache would be returned as-is.
        f.write_text('{"last_time_check": 59, "version": "4.2.3"}')
        vc = _VersionCache()
        vc._cache_file = SecurePath(f)  # noqa: SLF001

        with mock.patch.object(
            vc, "_update_latest_version", return_value=Version("9.9.9")
        ) as mock_update:
            assert vc._read_latest_version(  # noqa: SLF001
                force_refresh=False
            ) == Version("4.2.3")
            mock_update.assert_not_called()

            assert vc._read_latest_version(  # noqa: SLF001
                force_refresh=True
            ) == Version("9.9.9")
            mock_update.assert_called_once()


@pytest.mark.parametrize(
    "now,last_time_shown,expected",
    [
        (1000000, 1000000 - NEW_VERSION_MESSAGE_INTERVAL - 1, False),
        (1000000, 1000000 - NEW_VERSION_MESSAGE_INTERVAL + 100, True),
        (1000000, None, False),
    ],
    ids=[
        "not_shown_recently",
        "shown_recently",
        "never_shown",
    ],
)
@patch("snowflake.cli._app.version_check.time.time")
def test_was_warning_shown_recently_parametrized(
    mock_time, now, last_time_shown, expected
):
    mock_time.return_value = now
    assert was_warning_shown_recently(last_time_shown) is expected


@patch(*_PATCH_VERSION)
@patch(*_PATCH_LAST_VERSION)  # type: ignore
def test_get_version_info_update_available():
    info = get_version_info()
    assert info.current_version == "1.0.0"
    assert info.latest_version == "2.0.0"
    assert info.update_available is True


@patch("snowflake.cli._app.version_check.VERSION", "2.0.0")
@patch(*_PATCH_LAST_VERSION)  # type: ignore
def test_get_version_info_up_to_date():
    info = get_version_info()
    assert info.update_available is False


@patch(*_PATCH_VERSION)
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _self, force_refresh=False: None,
)
def test_get_version_info_latest_unavailable():
    info = get_version_info()
    assert info.latest_version is None
    assert info.update_available is False


@patch(*_PATCH_VERSION)
def test_get_version_info_force_refresh_is_forwarded():
    with mock.patch(
        "snowflake.cli._app.version_check._VersionCache.get_last_version",
        return_value=Version("2.0.0"),
    ) as mocked:
        get_version_info(force_refresh=True)
    mocked.assert_called_once_with(force_refresh=True)


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_get_new_version_msg_ignored_by_env():
    assert get_new_version_msg() == _WARNING_MESSAGE

    with mock.patch.dict(
        "os.environ", {"SNOWFLAKE_CLI_IGNORE_NEW_VERSION_WARNING": "true"}
    ):
        assert get_new_version_msg() is None


@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_get_new_version_msg_ignored_by_config_file(test_snowcli_config):
    assert get_new_version_msg() == _WARNING_MESSAGE

    config_text = test_snowcli_config.read_text()
    doc = tomlkit.parse(config_text)
    if "cli" not in doc:
        doc["cli"] = {}
    doc["cli"]["ignore_new_version_warning"] = True
    test_snowcli_config.write_text(tomlkit.dumps(doc))
    config_init(test_snowcli_config)

    assert get_new_version_msg() is None


@patch("snowflake.cli._app.version_check.time.time", lambda: 60)
def test_start_background_refresh_skipped_for_fresh_cache(named_temporary_file):
    reset_background_refresh_thread()
    with named_temporary_file() as f:
        f.write_text('{"last_time_check": 59, "version": "3.5.0"}')
        vc = _VersionCache()
        vc._cache_file = SecurePath(f)  # noqa: SLF001
        with mock.patch.object(vc, "_update_latest_version") as mock_update:
            vc.schedule_background_refresh()
            wait_for_refresh()
        mock_update.assert_not_called()


@patch("snowflake.cli._app.version_check.time.time", lambda: 2 * 60 * 60)
def test_background_refresh_updates_cache(named_temporary_file):
    reset_background_refresh_thread()
    with named_temporary_file() as f, _immediate_background_refresh():
        f.write_text('{"last_time_check": 0.0, "version": "3.5.0"}')
        vc = _VersionCache()
        vc._cache_file = SecurePath(f)  # noqa: SLF001
        with mock.patch.object(
            vc, "_update_latest_version", return_value=Version("5.0.0")
        ) as mock_update:
            vc.schedule_background_refresh()
            wait_for_refresh()
        mock_update.assert_called_once()


@patch(
    "snowflake.cli._app.version_check.time.time",
    lambda: VERSION_CACHE_REFRESH_INTERVAL + 1,
)
def test_schedule_background_refresh_skips_when_attempt_is_recent(
    named_temporary_file,
):
    reset_background_refresh_thread()
    now = VERSION_CACHE_REFRESH_INTERVAL + 1
    with named_temporary_file() as f, _immediate_background_refresh():
        f.write_text(f'{{"last_attempt": {now - 1}}}')
        vc = _VersionCache()
        vc._cache_file = SecurePath(f)  # noqa: SLF001
        with mock.patch.object(vc, "_update_latest_version") as mock_update:
            vc.schedule_background_refresh()
            wait_for_refresh()
        mock_update.assert_not_called()


@patch(
    "snowflake.cli._app.version_check.time.time",
    lambda: VERSION_CACHE_REFRESH_INTERVAL * 3,
)
def test_schedule_background_refresh_runs_when_attempt_is_stale(
    named_temporary_file,
):
    reset_background_refresh_thread()
    with named_temporary_file() as f, _immediate_background_refresh():
        f.write_text(
            '{"last_time_check": 0.0, "version": "3.5.0", "last_attempt": 0.0}'
        )
        vc = _VersionCache()
        vc._cache_file = SecurePath(f)  # noqa: SLF001
        with mock.patch.object(
            vc, "_update_latest_version", return_value=Version("5.0.0")
        ) as mock_update:
            vc.schedule_background_refresh()
            wait_for_refresh()
        mock_update.assert_called_once()


@pytest.mark.parametrize("payload", ["[]", "123", '"foo"'])
@patch("snowflake.cli._app.version_check.time.time", lambda: 2 * 60 * 60)
def test_schedule_background_refresh_treats_non_object_cache_as_stale(
    payload, named_temporary_file
):
    reset_background_refresh_thread()
    with named_temporary_file() as f, _immediate_background_refresh():
        f.write_text(payload)
        vc = _VersionCache()
        vc._cache_file = SecurePath(f)  # noqa: SLF001
        with mock.patch.object(
            vc, "_update_latest_version", return_value=Version("5.0.0")
        ) as mock_update:
            vc.schedule_background_refresh()
            wait_for_refresh()
        mock_update.assert_called_once()


@patch("snowflake.cli._app.version_check._VersionCache._update_latest_version")
def test_run_background_refresh_marks_attempt_before_network(
    mock_update, named_temporary_file
):
    mock_update.side_effect = Exception("network down")
    with named_temporary_file() as f:
        vc = _VersionCache()
        vc._cache_file = SecurePath(f)  # noqa: SLF001
        vc._mark_attempt()  # noqa: SLF001
        try:
            vc._update_latest_version()  # noqa: SLF001
        except Exception:
            pass
        data = json.loads(f.read_text())
    assert "last_attempt" in data


def test_atomic_write_fallback_on_permission_error(tmp_path):
    vc = _VersionCache()
    vc._cache_file = SecurePath(tmp_path / ".cli_version.cache")  # noqa: SLF001
    with mock.patch("os.replace", side_effect=PermissionError("locked")):
        vc._atomic_write('{"test": 1}')  # noqa: SLF001
    assert (tmp_path / ".cli_version.cache").read_text() == '{"test": 1}'


def test_atomic_write_swallows_unwritable_directory(tmp_path):
    vc = _VersionCache()
    cache_dir = tmp_path / "config"
    cache_dir.mkdir(mode=0o500)
    vc._cache_file = SecurePath(cache_dir / ".cli_version.cache")  # noqa: SLF001
    vc._atomic_write('{"test": 1}')  # noqa: SLF001


def test_record_version_check_displayed_swallows_write_errors():
    with mock.patch.object(
        _VersionCache,
        "update_last_time_shown",
        side_effect=PermissionError("denied"),
    ):
        record_version_check_displayed()


def test_get_new_version_msg_never_calls_network():
    with mock.patch(
        "snowflake.cli._app.version_check._VersionCache._update_latest_version"
    ) as mock_update, mock.patch(
        "snowflake.cli._app.version_check._VersionCache.get_cached_version",
        return_value=None,
    ):
        get_new_version_msg()
    mock_update.assert_not_called()


@patch("snowflake.cli._app.version_check.wait_for_refresh")
@patch(*_PATCH_VERSION)
@patch(*_PATCH_CACHED_VERSION)  # type: ignore
def test_get_new_version_msg_waits_for_refresh(mock_wait):
    get_new_version_msg()
    mock_wait.assert_called_once()


@patch(*_PATCH_VERSION)
@patch("snowflake.cli._app.version_check.wait_for_refresh")
def test_join_timeout_skips_banner(mock_wait):
    mock_wait.return_value = None
    with mock.patch(
        "snowflake.cli._app.version_check._VersionCache.get_cached_version",
        return_value=None,
    ):
        assert get_new_version_msg() is None
