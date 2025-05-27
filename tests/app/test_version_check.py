from io import BytesIO
from itertools import cycle
from unittest.mock import patch

import pytest
from requests import Response
from snowflake.cli._app.version_check import (
    VersionAndTime,
    _VersionCache,
    get_new_version_msg,
)
from snowflake.cli.api.secure_path import SecurePath

_WARNING_MESSAGE = (
    "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
)
_PATCH_VERSION = ["snowflake.cli._app.version_check.VERSION", "1.0.0"]
_PATCH_LAST_VERSION = [
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _: VersionAndTime("2.0.0", None),
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
    r.raw = BytesIO(
        b'{"info": {"version": "1.2.3"}, "releases": {"1.2.3": [{"upload_time": "2020-04-01"}]}}'
    )
    mock_get.return_value = r
    assert _VersionCache()._get_version_from_pypi() == VersionAndTime(  # noqa
        version="1.2.3", upload_time="2020-04-01"
    )
    mock_get.assert_called_once_with(
        "https://pypi.org/pypi/snowflake-cli/json",
        headers={"Content-Type": "application/vnd.pypi.simple.v1+json"},
        timeout=3,
    )


@patch("snowflake.cli._app.version_check.time.time", lambda: 0.0)
@pytest.mark.parametrize(
    "upload_time, expected", [("2020-04-01", '"2020-04-01"'), (None, "null")]
)
def test_saves_latest_version(named_temporary_file, upload_time, expected):
    with named_temporary_file() as f:
        vc = _VersionCache()
        vc._cache_file = f  # noqa
        vc._save_latest_version(VersionAndTime("1.2.3", upload_time))  # noqa
        data = f.read_text()
    assert (
        data
        == f'{{"last_time_check": 0.0, "version": "1.2.3", "upload_time": {expected}}}'
    )


@patch("snowflake.cli._app.version_check.time.time", lambda: 60)
@pytest.mark.parametrize(
    "date_str, expected",
    [
        ("", None),
        (', "upload_time": null', None),
        (', "upload_time": "2020-04-01"', "2020-04-01"),
    ],
)
def test_read_last_version(named_temporary_file, date_str, expected):
    with named_temporary_file() as f:
        sf = SecurePath(f)
        vc = _VersionCache()
        vc._cache_file = sf  # noqa
        f.write_text(f'{{"last_time_check": 0.0, "version": "4.2.3"{date_str}}}')
        assert vc._read_latest_version() == VersionAndTime("4.2.3", expected)  # noqa


@patch(
    "snowflake.cli._app.version_check._VersionCache._get_version_from_pypi",
    lambda _: VersionAndTime("8.0.0", "2020-04-10"),
)
@patch("snowflake.cli._app.version_check.time.time")
def test_read_last_version_and_updates_it(mock_time, named_temporary_file):
    mock_time.side_effect = cycle((2 * 60 * 60, 120))

    with named_temporary_file() as f:
        f.write_text('{"last_time_check": 0.0, "version": "1.2.3"}')
        sf = SecurePath(f)
        vc = _VersionCache()
        vc._cache_file = sf  # noqa
        assert vc._read_latest_version() == VersionAndTime(  # noqa
            "8.0.0", "2020-04-10"
        )
        data = sf.read_text(file_size_limit_mb=1)
        assert (
            data
            == '{"last_time_check": 120, "version": "8.0.0", "upload_time": "2020-04-10"}'
        )
