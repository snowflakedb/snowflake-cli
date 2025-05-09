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
)
@pytest.mark.parametrize("release_channels_enabled", [True, False])
@pytest.mark.integration
def test_publish_with_default_release_directive(
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

        # publish a new version to the default release directive
        result = runner.invoke_with_connection_json(
            ["app", "publish", "--version", "v1", "--create-version"]
        )
        assert result.exit_code == 0

        # make sure version is created:
        result = runner.invoke_with_connection_json(["app", "version", "list"])
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == 0

        # make sure default release directive is set to the new version:
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "DEFAULT"
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == 0

        # publish a new patch to the default release directive
        result = runner.invoke_with_connection_json(
            ["app", "publish", "--version", "v1", "--create-version", "--force"]
        )
        assert result.exit_code == 0

        # make sure new patch is created:
        result = runner.invoke_with_connection_json(["app", "version", "list"])
        assert result.exit_code == 0
        assert len(result.json) == 2
        patches_of_v1 = [row["patch"] for row in result.json if row["version"] == "V1"]
        assert sorted(patches_of_v1) == [0, 1]

        # make sure default release directive is set to the new patch:
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "DEFAULT"
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == 1


@mock.patch(
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
)
@pytest.mark.parametrize("release_channels_enabled", [True, False])
@pytest.mark.integration
def test_publish_with_existing_version_and_patch(
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

        # publish the new version to the default release directive
        result = runner.invoke_with_connection_json(
            ["app", "publish", "--version", "v1", "--patch", "0"]
        )
        assert result.exit_code == 0

        # make sure default release directive is set to the new version:
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "DEFAULT"
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == 0

        # create a new patch
        result = runner.invoke_with_connection_json(
            ["app", "version", "create", "v1", "--force"]
        )
        assert result.exit_code == 0
        new_patch = result.json["patch"]

        test_account = get_org_and_account_name(runner)

        # create a custom release directive
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
                "--target-accounts",
                test_account,
            ]
        )

        # publish the new patch to the custom release directive
        result = runner.invoke_with_connection_json(
            [
                "app",
                "publish",
                "--version",
                "v1",
                "--patch",
                new_patch,
                "--directive",
                "my_directive",
            ]
        )

        # make sure custom release directive is set to the new patch:
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list", "--like", "MY_DIRECTIVE"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "MY_DIRECTIVE"
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == new_patch
        assert result.json[0]["target_name"] == f"[{test_account}]"


@mock.patch(
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
    return_value=True,
)
@pytest.mark.integration
def test_publish_with_non_default_release_channel(
    get_value_mock, runner, nativeapp_teardown, nativeapp_basic_pdf
):
    with nativeapp_teardown():
        result = runner.invoke_with_connection(["app", "deploy"])
        assert result.exit_code == 0

        # publish a new version to the default release directive of the QA channel
        result = runner.invoke_with_connection(
            ["app", "publish", "--version", "v1", "--create-version", "--channel", "QA"]
        )
        assert result.exit_code == 0

        # make sure default release directive is set to the new version:
        result = runner.invoke_with_connection_json(
            ["app", "release-directive", "list", "--channel", "QA"]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "DEFAULT"
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == 0

        # add version v1 to the alpha channel (to test another channel with custom release directive)
        result = runner.invoke_with_connection(
            ["app", "release-channel", "add-version", "ALPHA", "--version", "v1"]
        )
        assert result.exit_code == 0

        # create custom release directive in another channel
        test_account = get_org_and_account_name(runner)
        result = runner.invoke_with_connection(
            [
                "app",
                "release-directive",
                "set",
                "my_directive",
                "--version",
                "v1",
                "--patch",
                0,
                "--target-accounts",
                test_account,
                "--channel",
                "ALPHA",
            ]
        )
        assert result.exit_code == 0

        # create a new patch and publish it to the custom release directive of the other channel
        result = runner.invoke_with_connection(
            [
                "app",
                "publish",
                "--version",
                "v1",
                "--create-version",
                "--force",
                "--channel",
                "ALPHA",
                "--directive",
                "my_directive",
            ]
        )
        assert result.exit_code == 0

        # make sure default release directive of ALPHA channel is set to the new version:
        result = runner.invoke_with_connection_json(
            [
                "app",
                "release-directive",
                "list",
                "--channel",
                "ALPHA",
                "--like",
                "my_directive",
            ]
        )
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["name"] == "MY_DIRECTIVE"
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == 1


@mock.patch(
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
)
@pytest.mark.parametrize("release_channels_enabled", [True, False])
@pytest.mark.integration
def test_publish_with_version_label_and_from_stage(
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

        # create a file to make sure it doesn't get uploaded with --from-stage
        with open("TEST_UPDATE.md", "w") as f:
            f.write("Hello world!")

        # create a version with a label and using --from-stage with publish command
        result = runner.invoke_with_connection(
            [
                "app",
                "publish",
                "--version",
                "v1",
                "--create-version",
                "--label",
                "my_label",
                "--from-stage",
            ]
        )
        assert result.exit_code == 0

        # make sure the file has not been uploaded
        result = runner.invoke_with_connection(["app", "diff"])
        assert result.exit_code == 0
        assert "TEST_UPDATE.md" in result.output

        # make sure version has been created with the label
        result = runner.invoke_with_connection_json(["app", "version", "list"])
        assert result.exit_code == 0
        assert len(result.json) == 1
        assert result.json[0]["version"] == "V1"
        assert result.json[0]["patch"] == 0
        assert result.json[0]["label"] == "my_label"
