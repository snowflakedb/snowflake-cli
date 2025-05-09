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
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
    return_value=False,
)
@pytest.mark.integration
def test_release_directives_with_disabled_channels(
    get_value_mock, runner, nativeapp_teardown, nativeapp_basic_pdf
):
    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "deploy"])
        assert result.exit_code == 0

        # create a version:
        result = runner.invoke_with_connection_json(["app", "version", "create", "v1"])
        assert result.exit_code == 0
        patch = result.json["patch"]

        # set default release directive
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "set",
                "default",
                "--version",
                "v1",
                "--patch",
                patch,
            ]
        )
        assert result.exit_code == 0

        # verify default release directive
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "DEFAULT"
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == patch

        # get a test account to use:
        current_account = get_org_and_account_name(runner)
        # Use current account as the target account to avoid hard coding account names in the test
        # We could use `show accounts` to pick more accounts, but requires extra privileges
        # We will add current account twice to ensure that comma separated accounts are handled correctly
        target_accounts = [current_account]

        # set a custom release directive
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "set",
                "my_directive",
                "--version",
                "v1",
                "--patch",
                patch,
                "--target-accounts",
                ",".join(target_accounts),
            ]
        )
        assert result.exit_code == 0

        # verify custom release directive
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 2
        custom_directive = [
            directive
            for directive in result.json
            if directive["name"] == "MY_DIRECTIVE"
        ][0]
        assert custom_directive["version"] == "V1"
        assert custom_directive["patch"] == patch
        # do not worry about sorting because there is only one account:
        assert custom_directive["target_name"] == f"[{target_accounts[0]}]"
        assert custom_directive["target_type"] == "ACCOUNT"

        # create new patch
        result = runner.invoke_with_connection_json(
            ["app", "version", "create", "v1", "--force"]
        )
        assert result.exit_code == 0
        new_patch = result.json["patch"]

        # update custom release directive
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "set",
                "my_directive",
                "--version",
                "v1",
                "--patch",
                new_patch,
            ]
        )
        assert result.exit_code == 0

        # verify custom release directive is updated
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 2
        custom_directive = [
            directive
            for directive in result.json
            if directive["name"] == "MY_DIRECTIVE"
        ][0]
        assert custom_directive["version"] == "V1"
        assert custom_directive["patch"] == new_patch
        # do not worry about sorting because there is only one account:
        assert custom_directive["target_name"] == f"[{target_accounts[0]}]"
        assert custom_directive["target_type"] == "ACCOUNT"

        # delete custom release directive
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "unset",
                "my_directive",
            ]
        )
        assert result.exit_code == 0

        # verify custom release directive is deleted
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "DEFAULT"


@mock.patch(
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
    return_value=True,
)
@pytest.mark.integration
def test_release_directives_with_enabled_channels(
    get_value_mock, runner, nativeapp_teardown, nativeapp_basic_pdf
):
    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "deploy"])
        assert result.exit_code == 0

        # create a version:
        result = runner.invoke_with_connection_json(["app", "version", "create", "v1"])
        assert result.exit_code == 0
        patch = result.json["patch"]

        # add version to release channel
        result = runner.invoke_with_connection(
            ["app", "release-channel", "add-version", "QA", "--version", "v1"]
        )
        assert result.exit_code == 0

        # set default release directive for QA channel
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "set",
                "default",
                "--version",
                "v1",
                "--patch",
                patch,
                "--channel",
                "QA",
            ]
        )
        assert result.exit_code == 0

        # verify default release directive
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list", "--channel", "QA"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "DEFAULT"
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == patch

        # get a test account to use:
        current_account = get_org_and_account_name(runner)
        # Use current account as the target account to avoid hard coding account names in the test
        # We could use `show accounts` to pick more accounts, but requires extra privileges
        # We will add current account twice to ensure that comma separated accounts are handled correctly
        target_accounts = [current_account]

        # set a custom release directive
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "set",
                "my_directive",
                "--version",
                "v1",
                "--patch",
                patch,
                "--channel",
                "QA",
                "--target-accounts",
                ",".join(target_accounts),
            ]
        )
        assert result.exit_code == 0

        # verify custom release directive
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list", "--channel", "QA"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 2
        custom_directive = [
            directive
            for directive in result.json
            if directive["name"] == "MY_DIRECTIVE"
        ][0]
        assert custom_directive["version"] == "V1"
        assert custom_directive["patch"] == patch
        # do not worry about sorting because there is only one account:
        assert custom_directive["target_name"] == f"[{target_accounts[0]}]"
        assert custom_directive["target_type"] == "ACCOUNT"

        # create new patch
        result = runner.invoke_with_connection_json(
            ["app", "version", "create", "v1", "--force"]
        )
        assert result.exit_code == 0
        new_patch = result.json["patch"]

        # add version to default release channel
        result = runner.invoke_with_connection(
            ["app", "release-channel", "add-version", "DEFAULT", "--version", "v1"]
        )
        assert result.exit_code == 0

        # set release directive on the default channel
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "set",
                "default",
                "--version",
                "v1",
                "--patch",
                new_patch,
            ]
        )
        assert result.exit_code == 0

        # verify release directives from all channels are present
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 3
        result_map = {
            f'{directive["release_channel_name"]}-{directive["name"]}': directive
            for directive in result.json
        }
        custom_directive = result_map["QA-MY_DIRECTIVE"]
        assert custom_directive["version"] == "V1"
        assert custom_directive["patch"] == patch

        default_directive_qa_channel = result_map["QA-DEFAULT"]
        assert default_directive_qa_channel["version"] == "V1"
        assert default_directive_qa_channel["patch"] == patch

        default_directive_default_channel = result_map["DEFAULT-DEFAULT"]
        assert default_directive_default_channel["version"] == "V1"
        assert default_directive_default_channel["patch"] == new_patch

        # delete custom release directive
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "unset",
                "my_directive",
                "--channel",
                "QA",
            ]
        )
        assert result.exit_code == 0

        # verify custom release directive is deleted
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list", "--channel", "QA"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "DEFAULT"


@mock.patch(
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
)
@pytest.mark.integration
@pytest.mark.parametrize("release_channels_enabled", [True, False])
def test_release_directive_add_and_remove_accounts(
    get_value_mock,
    runner,
    nativeapp_teardown,
    nativeapp_basic_pdf,
    release_channels_enabled,
):
    get_value_mock.return_value = release_channels_enabled

    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "deploy"])
        assert result.exit_code == 0

        # create a version:
        result = runner.invoke_with_connection_json(["app", "version", "create", "v1"])
        assert result.exit_code == 0
        patch = result.json["patch"]

        # add version to release channel
        if release_channels_enabled:
            result = runner.invoke_with_connection(
                ["app", "release-channel", "add-version", "DEFAULT", "--version", "v1"]
            )
            assert result.exit_code == 0

        # set custom release directive with a target account
        target_account = get_org_and_account_name(runner)
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "set",
                "my_directive",
                "--version",
                "v1",
                "--patch",
                patch,
                "--target-accounts",
                target_account,
            ]
        )
        assert result.exit_code == 0

        # verify custom release directive
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        custom_directive = result.json[0]
        assert custom_directive["version"] == "V1"
        assert custom_directive["patch"] == patch
        assert custom_directive["target_name"] == f"[{target_account}]"
        assert custom_directive["target_type"] == "ACCOUNT"

        # remove account from release directive
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "remove-accounts",
                "my_directive",
                "--target-accounts",
                target_account,
            ]
        )
        assert result.exit_code == 0

        # verify account is removed from release directive
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        custom_directive = result.json[0]
        assert custom_directive["version"] == "V1"
        assert custom_directive["patch"] == patch
        assert custom_directive["target_name"] is None
        assert custom_directive["target_type"] == "ACCOUNT"

        # add account back to release directive
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "add-accounts",
                "my_directive",
                "--target-accounts",
                target_account,
            ]
        )
        assert result.exit_code == 0

        # verify account is added back to release directive
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        custom_directive = result.json[0]
        assert custom_directive["version"] == "V1"
        assert custom_directive["patch"] == patch
        assert custom_directive["target_name"] == f"[{target_account}]"
        assert custom_directive["target_type"] == "ACCOUNT"
