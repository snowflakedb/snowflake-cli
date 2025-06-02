import json
from io import BytesIO
from itertools import cycle
from pathlib import Path
from unittest.mock import patch

import pytest
import tomlkit
from packaging.version import Version
from requests import Response
from snowflake.cli._app.version_check import (
    NEW_VERSION_MSG_INTERVAL,
    _VersionCache,
    get_new_version_msg,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector.config_manager import CONFIG_MANAGER

_WARNING_MESSAGE = (
    "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
)
_PATCH_VERSION = ["snowflake.cli._app.version_check.VERSION", "1.0.0"]
_PATCH_LAST_VERSION = [
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _: Version("2.0.0"),
]
_PATCH_SHOULD_SHOW_NEW_VERSION_MSG = [
    "snowflake.cli._app.version_check._VersionCache.should_show_new_version_msg",
    lambda _: True,
]


@pytest.fixture
def warning_is_thrown():
    with pytest.warns(UserWarning, match=_WARNING_MESSAGE):
        yield


@pytest.fixture
def warning_is_not_thrown():
    with pytest.warns() as recorded_warnings:
        yield
    for warning in recorded_warnings:
        assert _WARNING_MESSAGE not in str(warning.message)


@patch(*_PATCH_VERSION)
@patch(*_PATCH_LAST_VERSION)  # type: ignore
def test_banner_shows_up_in_help(build_runner, warning_is_thrown):
    build_runner().invoke(["--help"])


@patch(*_PATCH_VERSION)
@patch(*_PATCH_LAST_VERSION)  # type: ignore
def test_banner_shows_up_in_command_invocation(build_runner, warning_is_thrown):
    build_runner().invoke(["connection", "set-default", "default"])


@patch(*_PATCH_VERSION)
@patch(*_PATCH_LAST_VERSION)  # type: ignore
def test_banner_do_not_shows_up_if_silent(build_runner, warning_is_not_thrown):
    build_runner().invoke(["connection", "set-default", "default", "--silent"])


@patch("snowflake.cli._app.version_check._VersionCache._read_latest_version")
def test_version_check_exception_are_handled_safely(
    mock_read_latest_version, build_runner, warning_is_not_thrown
):
    mock_read_latest_version.side_effect = Exception("Error")
    result = build_runner().invoke(["connection", "set-default", "default"])
    assert result.exit_code == 0


@patch(*_PATCH_VERSION)
@patch(*_PATCH_LAST_VERSION)  # type: ignore
@patch(*_PATCH_SHOULD_SHOW_NEW_VERSION_MSG)  # type: ignore
def test_get_new_version_msg_message_if_new_version_available():
    msg = get_new_version_msg()
    assert (
        msg.strip()
        == "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
    )


@patch(*_PATCH_VERSION)
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_last_version", lambda _: None
)
def test_get_new_version_msg_does_not_show_message_if_no_new_version():
    assert get_new_version_msg() is None


@patch("snowflake.cli._app.version_check.VERSION", "3.0.0")
@patch(*_PATCH_LAST_VERSION)  # type: ignore
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
        timeout=3,
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
        timeout=3,
    )


@patch("snowflake.cli._app.version_check.time.time", lambda: 0.0)
def test_saves_latest_version(named_temporary_file):
    with named_temporary_file() as f:
        vc = _VersionCache()
        vc._cache_file = f  # noqa: SLF001
        vc._save_latest_version("1.2.3")  # noqa
        data = f.read_text()
    assert data == '{"last_time_check": 0.0, "version": "1.2.3"}'


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


@pytest.mark.parametrize(
    "now,last_time_shown,expected",
    [
        (1000000, 1000000 - NEW_VERSION_MSG_INTERVAL - 1, True),
        (1000000, 1000000 - NEW_VERSION_MSG_INTERVAL + 100, False),
        (1000000, None, True),
    ],
)
@patch("snowflake.cli._app.version_check.time.time")
def test_should_show_new_version_msg_parametrized(
    mock_time, named_temporary_file, now, last_time_shown, expected
):
    mock_time.return_value = now
    with named_temporary_file() as f:
        sf = SecurePath(f)
        vc = _VersionCache()
        vc._cache_file = sf  # noqa: SLF001
        cache_data = {"last_time_check": 0.0, "version": "2.0.0"}
        if last_time_shown is not None:
            cache_data["last_time_shown"] = last_time_shown
        f.write_text(json.dumps(cache_data))
        assert vc.should_show_new_version_msg() is expected


@patch(*_PATCH_VERSION)
@patch(*_PATCH_LAST_VERSION)  # type: ignore
@patch(*_PATCH_SHOULD_SHOW_NEW_VERSION_MSG)  # type: ignore
def test_get_new_version_msg_muted_by_env(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CLI_MUTE_NEW_VERSION_MESSAGE", "true")
    # Patch config to return None so env is checked
    with patch("snowflake.cli.api.config.get_config_section", lambda *a, **k: {}):
        assert get_new_version_msg() is None


@patch(*_PATCH_VERSION)
@patch(*_PATCH_LAST_VERSION)  # type: ignore
@patch(*_PATCH_SHOULD_SHOW_NEW_VERSION_MSG)  # type: ignore
def test_get_new_version_msg_muted_by_config_file(tmp_path):
    assert get_new_version_msg() is not None

    # Create a config.toml with mute_new_version_message = true
    config_path = tmp_path / "config.toml"
    config_data = tomlkit.document()
    config_data.add("cli", {"mute_new_version_message": True})
    config_path.write_text(tomlkit.dumps(config_data))

    # Point CONFIG_MANAGER.file_path to this config and reload config
    CONFIG_MANAGER.file_path = Path(config_path)
    CONFIG_MANAGER.read_config()

    # Now the mute should be respected
    assert get_new_version_msg() is None
