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

import json
from unittest import mock

from packaging.version import Version

COMMAND = ["helpers", "check-version"]

_VERSION = "snowflake.cli._app.version_check.VERSION"
_GET_LAST_VERSION = "snowflake.cli._app.version_check._VersionCache.get_last_version"


def _json_output(output: str) -> dict:
    return json.loads(output)


@mock.patch(_VERSION, "1.0.0")
@mock.patch(_GET_LAST_VERSION, lambda _self, force_refresh=False: Version("2.0.0"))
def test_reports_update_available(runner):
    result = runner.invoke([*COMMAND, "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _json_output(result.output)
    assert payload["current_version"] == "1.0.0"
    assert payload["latest_version"] == "2.0.0"
    assert payload["update_available"] is True


@mock.patch(_VERSION, "2.0.0")
@mock.patch(_GET_LAST_VERSION, lambda _self, force_refresh=False: Version("2.0.0"))
def test_reports_up_to_date(runner):
    result = runner.invoke([*COMMAND, "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _json_output(result.output)
    assert payload["current_version"] == "2.0.0"
    assert payload["latest_version"] == "2.0.0"
    assert payload["update_available"] is False


@mock.patch(_VERSION, "3.0.0")
@mock.patch(_GET_LAST_VERSION, lambda _self, force_refresh=False: Version("2.0.0"))
def test_local_version_newer_than_published(runner):
    result = runner.invoke([*COMMAND, "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _json_output(result.output)
    assert payload["update_available"] is False


@mock.patch(_GET_LAST_VERSION, lambda _self, force_refresh=False: None)
def test_latest_version_unavailable(runner):
    result = runner.invoke(COMMAND)
    assert result.exit_code != 0
    assert "Could not determine the latest Snowflake CLI version" in result.output


@mock.patch(_VERSION, "1.0.0")
def test_refresh_flag_forces_cache_bypass(runner):
    with mock.patch(
        "snowflake.cli._app.version_check._VersionCache.get_last_version",
        return_value=Version("2.0.0"),
    ) as mocked:
        result = runner.invoke([*COMMAND, "--refresh", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    mocked.assert_called_once_with(force_refresh=True)


@mock.patch(_VERSION, "1.0.0")
def test_default_uses_cache(runner):
    with mock.patch(
        "snowflake.cli._app.version_check._VersionCache.get_last_version",
        return_value=Version("2.0.0"),
    ) as mocked:
        result = runner.invoke([*COMMAND, "--format", "JSON"])
    assert result.exit_code == 0, result.output
    mocked.assert_called_once_with(force_refresh=False)


@mock.patch(_VERSION, "1.0.0")
@mock.patch(_GET_LAST_VERSION, lambda _self, force_refresh=False: Version("2.0.0"))
def test_does_not_require_connection(runner):
    result = runner.invoke(COMMAND)
    assert result.exit_code == 0, result.output


@mock.patch(_VERSION, "1.0.0")
@mock.patch(_GET_LAST_VERSION, lambda _self, force_refresh=False: Version("2.0.0"))
def test_ignores_new_version_warning_setting(runner, monkeypatch):
    """An explicit check always reports, even when the passive banner is muted."""
    monkeypatch.setenv("SNOWFLAKE_CLI_IGNORE_NEW_VERSION_WARNING", "true")
    result = runner.invoke([*COMMAND, "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _json_output(result.output)
    assert payload["update_available"] is True
