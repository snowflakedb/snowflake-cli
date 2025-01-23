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

from unittest import mock

import pytest

from tests_integration.nativeapp.native_apps_utils import get_org_and_account_name


@mock.patch(
    "snowflake.cli._plugins.nativeapp.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
    return_value=True,
)
@pytest.mark.integration
def test_release_channels_list_when_enabled(
    get_value_mock, runner, nativeapp_teardown, nativeapp_basic_pdf
):

    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "deploy"])
        assert result.exit_code == 0

        result = runner.invoke_with_connection_json(["app", "release-channel", "list"])
        assert result.exit_code == 0
        assert len(result.json) >= 3
        channel_names = [channel["name"] for channel in result.json]
        assert "DEFAULT" in channel_names
        assert "ALPHA" in channel_names
        assert "QA" in channel_names


@mock.patch(
    "snowflake.cli._plugins.nativeapp.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value"
)
@pytest.mark.integration
def test_release_channels_disabled_to_enabled_switch(
    get_value_mock, runner, nativeapp_teardown, nativeapp_basic_pdf
):

    with nativeapp_teardown():
        # disable release channels
        get_value_mock.return_value = False

        result = runner.invoke_with_connection(["app", "deploy"])
        assert result.exit_code == 0

        result = runner.invoke_with_connection_json(["app", "release-channel", "list"])
        assert result.exit_code == 0
        assert result.json == []

        # enable release channels
        get_value_mock.return_value = True

        result = runner.invoke_with_connection_json(["app", "deploy"])
        assert result.exit_code == 0

        result = runner.invoke_with_connection_json(["app", "release-channel", "list"])
        assert result.exit_code == 0
        assert len(result.json) >= 3
        channel_names = [channel["name"] for channel in result.json]
        assert "DEFAULT" in channel_names
        assert "ALPHA" in channel_names
        assert "QA" in channel_names


@mock.patch(
    "snowflake.cli._plugins.nativeapp.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
    return_value=True,
)
@pytest.mark.integration
def test_add_accounts_and_remove_accounts_and_set_accounts_from_release_channels(
    get_value_mock, runner, nativeapp_teardown, nativeapp_basic_pdf
):

    with nativeapp_teardown():
        # create app package
        result = runner.invoke_with_connection(["app", "deploy"])
        assert result.exit_code == 0

        # get a test account to use:
        current_account = get_org_and_account_name(runner)
        # Use current account as the target account to avoid hard coding account names in the test
        # We could use `show accounts` to pick more accounts, but requires extra privileges
        # We will add current account twice to ensure that comma separated accounts are handled correctly
        target_accounts = [current_account]

        # add accounts to the release channel:
        result = runner.invoke_with_connection(
            [
                "app",
                "release-channel",
                "add-accounts",
                "ALPHA",
                "--target-accounts",
                ",".join(target_accounts),
            ]
        )
        assert result.exit_code == 0

        # verify accounts are in the release channel
        result = runner.invoke_with_connection_json(
            ["app", "release-channel", "list", "ALPHA"]
        )
        assert result.exit_code == 0
        alpha_channel = result.json[0]
        unique_accounts = list(set(target_accounts))
        assert sorted(alpha_channel["targets"].get("accounts")) == sorted(
            unique_accounts
        )

        # remove accounts from the release channel:
        result = runner.invoke_with_connection(
            [
                "app",
                "release-channel",
                "remove-accounts",
                "ALPHA",
                "--target-accounts",
                ",".join(target_accounts),
            ]
        )
        assert result.exit_code == 0

        # verify accounts are removed from the release channel
        result = runner.invoke_with_connection_json(
            ["app", "release-channel", "list", "ALPHA"]
        )
        assert result.exit_code == 0
        alpha_channel = result.json[0]
        assert alpha_channel["targets"].get("accounts") == []

        # set accounts for the release channel
        result = runner.invoke_with_connection(
            [
                "app",
                "release-channel",
                "set-accounts",
                "ALPHA",
                "--target-accounts",
                ",".join(target_accounts),
            ]
        )
        assert result.exit_code == 0

        # verify accounts are in the release channel
        result = runner.invoke_with_connection_json(
            ["app", "release-channel", "list", "ALPHA"]
        )
        assert result.exit_code == 0
        alpha_channel = result.json[0]
        assert sorted(alpha_channel["targets"].get("accounts")) == sorted(
            unique_accounts
        )


@mock.patch(
    "snowflake.cli._plugins.nativeapp.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
    return_value=True,
)
@pytest.mark.integration
def test_add_version_and_remove_version_from_release_channels(
    get_value_mock, runner, nativeapp_teardown, nativeapp_basic_pdf
):

    with nativeapp_teardown():
        # create app package
        result = runner.invoke_with_connection(["app", "deploy"])
        assert result.exit_code == 0

        # create version v1:
        result = runner.invoke_with_connection(["app", "version", "create", "v1"])
        assert result.exit_code == 0

        # add version to the release channel:
        result = runner.invoke_with_connection(
            [
                "app",
                "release-channel",
                "add-version",
                "ALPHA",
                "--version",
                "v1",
            ]
        )
        assert result.exit_code == 0

        # create version v2.special_chars:
        result = runner.invoke_with_connection(
            ["app", "version", "create", "v2.special_chars"]
        )
        assert result.exit_code == 0

        # add version v2.special_chars to the release channel:
        result = runner.invoke_with_connection(
            [
                "app",
                "release-channel",
                "add-version",
                "ALPHA",
                "--version",
                "v2.special_chars",
            ]
        )
        assert result.exit_code == 0

        # verify that these versions are in the release channel
        result = runner.invoke_with_connection_json(
            ["app", "release-channel", "list", "ALPHA"]
        )
        assert result.exit_code == 0
        alpha_channel = result.json[0]
        assert sorted(alpha_channel["versions"]) == sorted(["V1", "v2.special_chars"])

        # remove version from the release channel:
        result = runner.invoke_with_connection(
            [
                "app",
                "release-channel",
                "remove-version",
                "ALPHA",
                "--version",
                "v1",
            ]
        )
        assert result.exit_code == 0

        # verify version is removed from the release channel
        result = runner.invoke_with_connection_json(
            ["app", "release-channel", "list", "ALPHA"]
        )
        assert result.exit_code == 0
        alpha_channel = result.json[0]
        assert alpha_channel["versions"] == ["v2.special_chars"]

        # remove version from the release channel:
        result = runner.invoke_with_connection(
            [
                "app",
                "release-channel",
                "remove-version",
                "ALPHA",
                "--version",
                "v2.special_chars",
            ]
        )
        assert result.exit_code == 0

        # verify version is removed from the release channel
        result = runner.invoke_with_connection_json(
            ["app", "release-channel", "list", "ALPHA"]
        )
        assert result.exit_code == 0
        alpha_channel = result.json[0]
        assert alpha_channel["versions"] == []
