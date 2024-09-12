from io import BytesIO
from unittest.mock import patch

from packaging.version import Version
from requests import Response
from snowflake.cli._app.version_check import _VersionCache, get_new_version_msg


@patch("snowflake.cli._app.version_check.VERSION", "1.0.0")
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _: Version("2.0.0"),
)
def test_banner_shows_up_in_help(build_runner):
    runner = build_runner()
    result = runner.invoke(["--help"])
    msg = "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
    assert msg in result.output


@patch("snowflake.cli._app.version_check.VERSION", "1.0.0")
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _: Version("2.0.0"),
)
def test_banner_shows_up_in_command_invocation(build_runner):
    runner = build_runner()
    result = runner.invoke(["connection", "set-default", "default"])
    msg = "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
    assert msg in result.output


@patch("snowflake.cli._app.version_check.VERSION", "1.0.0")
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _: Version("2.0.0"),
)
def test_banner_do_not_shows_up_if_silent(build_runner):
    runner = build_runner()
    result = runner.invoke(["connection", "set-default", "default", "--silent"])
    msg = "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
    assert msg not in result.output


@patch("snowflake.cli._app.version_check._VersionCache._read_latest_version")
def test_version_check_exception_are_handled_safely(
    mock_read_latest_version, build_runner
):
    mock_read_latest_version.side_effect = Exception("Error")
    runner = build_runner()
    result = runner.invoke(["connection", "set-default", "default"])

    msg = "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
    assert result.exit_code == 0
    assert msg not in result.output


@patch("snowflake.cli._app.version_check.VERSION", "1.0.0")
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _: Version("2.0.0"),
)
def test_get_new_version_msg_message_if_new_version_available():
    msg = get_new_version_msg()
    assert (
        msg.strip()
        == "New version of Snowflake CLI available. Newest: 2.0.0, current: 1.0.0"
    )


@patch("snowflake.cli._app.version_check.VERSION", "1.0.0")
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_last_version", lambda _: None
)
def test_get_new_version_msg_does_not_show_message_if_no_new_version():
    assert get_new_version_msg() is None


@patch("snowflake.cli._app.version_check.VERSION", "3.0.0")
@patch(
    "snowflake.cli._app.version_check._VersionCache.get_last_version",
    lambda _: Version("2.0.0"),
)
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
        "https://pypi.org/pypi/snowflake-cli-labs/json",
        headers={"Content-Type": "application/vnd.pypi.simple.v1+json"},
        timeout=3,
    )


@patch("snowflake.cli._app.version_check.time.time", lambda: 0.0)
def test_saves_latest_version(named_temporary_file):
    with named_temporary_file() as f:
        vc = _VersionCache()
        vc._cache_file = f  # noqa
        vc._save_latest_version("1.2.3")  # noqa
        data = f.read_text()
    assert data == '{"last_time_check": 0.0, "version": "1.2.3"}'


@patch("snowflake.cli._app.version_check.time.time", lambda: 60)
def test_read_last_version(named_temporary_file):
    with named_temporary_file() as f:
        vc = _VersionCache()
        vc._cache_file = f  # noqa
        f.write_text('{"last_time_check": 0.0, "version": "4.2.3"}')
        assert vc._read_latest_version() == Version("4.2.3")  # noqa


@patch(
    "snowflake.cli._app.version_check._VersionCache._get_version_from_pypi",
    lambda _: "8.0.0",
)
@patch("snowflake.cli._app.version_check.time.time")
def test_read_last_version_and_updates_it(mock_time, named_temporary_file):
    mock_time.side_effect = [2 * 60 * 60, 120]

    with named_temporary_file() as f:
        vc = _VersionCache()
        vc._cache_file = f  # noqa
        f.write_text('{"last_time_check": 0.0, "version": "1.2.3"}')
        assert vc._read_latest_version() == Version("8.0.0")  # noqa
        data = f.read_text()
        assert data == '{"last_time_check": 120, "version": "8.0.0"}'
