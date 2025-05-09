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

from shlex import split
from unittest import mock

from tests.project.fixtures import *
from tests_common import change_directory
from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)


# Tests a simple flow of initiating a new project, executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app run", "napp_init_v1"],
        ["app run", "napp_init_v2"],
        ["ws deploy --entity-id=app", "napp_init_v2"],
    ],
)
def test_nativeapp_init_run_without_modifications(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json(split(command))
        assert result.exit_code == 0

        # app + package exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{package_name}'",
                )
            ),
            dict(name=package_name),
        )
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show applications like '{app_name}'",
                )
            ),
            dict(name=app_name),
        )


@pytest.mark.integration
@pytest.mark.parametrize("test_project", ["napp_init_v2_multiple_entities"])
def test_nativeapp_init_run_multiple_pdf_entities(
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    with nativeapp_project_directory(
        test_project, teardown_args=["--package-entity-id", "pkg2"]
    ):
        result = runner.invoke_with_connection_json(
            ["app", "run", "--package-entity-id", "pkg2", "--app-entity-id", "app2"]
        )
        assert result.exit_code == 0

        # app + package exist
        package_name = (
            f"{project_name}_pkg_{default_username}_2{resource_suffix}".upper()
        )
        app_name = f"{project_name}_{default_username}_2{resource_suffix}".upper()
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{package_name}'",
                )
            ),
            dict(name=package_name),
        )
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show applications like '{app_name}'",
                )
            ),
            dict(name=app_name),
        )


# Tests a simple flow of an existing project, but executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration", "integration_v2"], indirect=True
)
def test_nativeapp_run_existing(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
    default_username,
    nativeapp_teardown,
    resource_suffix,
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with change_directory(project_dir):
        result = runner.invoke_with_connection_json(["app", "run"])
        assert result.exit_code == 0

        with nativeapp_teardown():
            # app + package exist
            package_name = (
                f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
            )
            app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{package_name}'",
                    )
                ),
                dict(name=package_name),
            )
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'"
                    )
                ),
                dict(name=app_name),
            )

            # sanity checks
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"select count(*) from {app_name}.core.shared_view"
                    )
                ),
                {"COUNT(*)": 1},
            )
            test_string = "TEST STRING"
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"call {app_name}.core.echo('{test_string}')"
                    )
                ),
                {"ECHO": test_string},
            )


# Tests a simple flow of initiating a project, executing snow app run and teardown, all with app spec enabled
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app run", "napp_app_manifest_v2"],
        ["app run --debug", "napp_app_manifest_v2"],
    ],
)
def test_nativeapp_init_manifest_v2(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection(split(command))
        assert result.exit_code == 0
        assert (
            "Did not apply debug mode to application because the manifest version is set to 2 or higher. Please use session debugging instead."
            in result.output
        )

        # app + package exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{package_name}'",
                )
            ),
            dict(name=package_name),
        )
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show applications like '{app_name}'",
                )
            ),
            dict(name=app_name),
        )
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"desc application {app_name}",
                )
            ),
            {"property": "debug_mode", "value": "false"},
        )


# Tests a simple flow of initiating a project, executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app run", "napp_init_v2"],
    ],
)
def test_nativeapp_init_run_handles_spaces(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json(split(command))
        assert result.exit_code == 0

        # app + package exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{package_name}'",
                )
            ),
            dict(name=package_name),
        )
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show applications like '{app_name}'",
                )
            ),
            dict(name=app_name),
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app run", "integration_external_v2"],
    ],
)
def test_nativeapp_run_existing_w_external(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "integration_external"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json(split(command))
        assert result.exit_code == 0

        # app + package exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{package_name}'",
                )
            ),
            dict(name=package_name),
        )
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(f"show applications like '{app_name}'")
            ),
            dict(name=app_name),
        )

        # app package contains distribution=external
        expect = row_from_snowflake_session(
            snowflake_session.execute_string(f"desc application package {package_name}")
        )
        assert contains_row_with(expect, {"property": "name", "value": package_name})
        assert contains_row_with(
            expect, {"property": "distribution", "value": "EXTERNAL"}
        )

        # sanity checks
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"select count(*) from {app_name}.core.shared_view"
                )
            ),
            {"COUNT(*)": 1},
        )
        test_string = "TEST STRING"
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"call {app_name}.core.echo('{test_string}')"
                )
            ),
            {"ECHO": test_string},
        )


# Verifies that running "app run" after "app deploy" upgrades the app
@pytest.mark.integration
@pytest.mark.parametrize(
    "base_command,test_project",
    [
        ["app", "napp_init_v2"],
    ],
)
def test_nativeapp_run_after_deploy(
    base_command,
    test_project,
    nativeapp_project_directory,
    runner,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"
    stage_fqn = f"{project_name}_pkg_{default_username}{resource_suffix}.app_src.stage"

    with nativeapp_project_directory(test_project):
        # Run #1
        if base_command == "ws":
            result = runner.invoke_with_connection_json(
                ["ws", "deploy", "--entity-id=app"]
            )
        else:
            result = runner.invoke_with_connection_json(["app", "run"])
        assert result.exit_code == 0

        # Make a change & deploy
        with open("app/README.md", "a") as file:
            file.write("### Test")
        if base_command == "ws":
            result = runner.invoke_with_connection_json(
                ["ws", "deploy", "--entity-id=pkg"]
            )
        else:
            result = runner.invoke_with_connection_json(["app", "run"])
        assert result.exit_code == 0

        # Run #2
        if base_command == "ws":
            result = runner.invoke_with_connection_json(
                ["ws", "deploy", "--entity-id=app", "--debug"]
            )
        else:
            result = runner.invoke_with_connection_json(["app", "run", "--debug"])
        assert result.exit_code == 0
        assert (
            f"alter application {app_name} upgrade using @{stage_fqn}" in result.output
        )


# Tests running an app whose package was dropped externally (requires dropping and recreating the app)
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app run", "integration_external_v2"],
    ],
)
@pytest.mark.parametrize("force_flag", [True, False])
def test_nativeapp_run_orphan(
    command,
    test_project,
    force_flag,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "integration_external"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json(split(command))
        assert result.exit_code == 0

        # app + package exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{package_name}'",
                )
            ),
            dict(name=package_name),
        )
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(f"show applications like '{app_name}'")
            ),
            dict(name=app_name, source=package_name),
        )

        result = runner.invoke_with_connection(
            ["sql", "-q", f"drop application package {package_name}"]
        )
        assert result.exit_code == 0, result.output

        # package doesn't exist, app not readable
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
        assert not_contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{package_name}'",
                )
            ),
            dict(name=package_name),
        )
        assert not_contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(f"show applications like '{app_name}'")
            ),
            dict(name=app_name, source=package_name),
        )

        if force_flag:
            command = [*split(command), "--force"]
            _input = None
        else:
            command = [*split(command), "--interactive"]  # show prompt in tests
            _input = "y\n"  # yes to drop app

        result = runner.invoke_with_connection(command, input=_input)
        assert result.exit_code == 0, result.output
        if not force_flag:
            assert (
                "Do you want the Snowflake CLI to drop the existing application object and recreate it?"
                in result.output
            ), result.output

        # app + package exist
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show application packages like '{package_name}'",
                )
            ),
            dict(name=package_name),
        )
        assert contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(f"show applications like '{app_name}'")
            ),
            dict(name=app_name, source=package_name),
        )


# Verifies that we can always cross-upgrade between different
# run configurations as long as we pass the --force flag to "app run"
# TODO: add back all parameterizations and implement --force for "app teardown"
@pytest.mark.integration
@pytest.mark.parametrize("test_project", ["napp_init_v2"])
@pytest.mark.parametrize(
    "run_args_from, run_args_to",
    [
        ([], []),
        ([], ["--version", "v1"]),
        ([], ["--from-release-directive"]),
        (["--version", "v1"], []),
        (["--version", "v1"], ["--version", "v1"]),
        (["--version", "v1"], ["--from-release-directive"]),
        (["--from-release-directive"], []),
        (["--from-release-directive"], ["--version", "v1"]),
        (["--from-release-directive"], ["--from-release-directive"]),
    ],
)
def test_nativeapp_force_cross_upgrade(
    test_project,
    nativeapp_project_directory,
    run_args_from,
    run_args_to,
    runner,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"
    pkg_name = f"{project_name}_pkg_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project):
        # Create version
        result = runner.invoke_with_connection(["app", "version", "create", "v1"])
        assert result.exit_code == 0

        # Set default release directive
        result = runner.invoke_with_connection(
            [
                "sql",
                "-q",
                f"alter application package {pkg_name} set default release directive version = v1 patch = 0",
            ]
        )
        assert result.exit_code == 0

        # Initial run
        result = runner.invoke_with_connection(["app", "run"] + run_args_from)
        assert result.exit_code == 0

        # (Cross-)upgrade
        is_cross_upgrade = run_args_from != run_args_to
        result = runner.invoke_with_connection(
            ["app", "run"] + run_args_to + ["--force"]
        )
        assert result.exit_code == 0
        if is_cross_upgrade:
            assert f"Dropping application object {app_name}." in result.output


@mock.patch(
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
)
@pytest.mark.integration
@pytest.mark.parametrize(
    "test_project",
    [
        "napp_init_v2",
    ],
)
@pytest.mark.parametrize("release_channels_enabled", [True, False])
def test_nativeapp_upgrade_from_release_directive_and_default_channel(
    get_value_mock,
    release_channels_enabled,
    test_project,
    nativeapp_project_directory,
    runner,
):

    get_value_mock.return_value = release_channels_enabled

    with nativeapp_project_directory(test_project):
        # Create version
        result = runner.invoke_with_connection(["app", "version", "create", "v1"])
        assert result.exit_code == 0

        # Set default release directive
        result = runner.invoke_with_connection(
            ["app", "publish", "--version", "v1", "--patch", "0"]
        )
        assert result.exit_code == 0

        # Initial create
        result = runner.invoke_with_connection(["app", "run"])
        assert result.exit_code == 0

        # (Cross-)upgrade
        result = runner.invoke_with_connection(
            [
                "app",
                "run",
                "--from-release-directive",
                "--channel",
                "default",
                "--force",
            ]
        )
        assert result.exit_code == 0


@mock.patch(
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
)
@pytest.mark.integration
@pytest.mark.parametrize(
    "test_project",
    [
        "napp_init_v2",
    ],
)
@pytest.mark.parametrize("release_channels_enabled", [True, False])
def test_nativeapp_create_from_release_directive_and_default_channel(
    get_value_mock,
    release_channels_enabled,
    test_project,
    nativeapp_project_directory,
    runner,
):

    get_value_mock.return_value = release_channels_enabled

    with nativeapp_project_directory(test_project):
        # Create version
        result = runner.invoke_with_connection(["app", "version", "create", "v1"])
        assert result.exit_code == 0

        # Set default release directive
        result = runner.invoke_with_connection(
            ["app", "publish", "--version", "v1", "--patch", "0"]
        )
        assert result.exit_code == 0

        # Initial create
        result = runner.invoke_with_connection(
            ["app", "run", "--from-release-directive", "--channel", "default"]
        )
        assert result.exit_code == 0


@mock.patch(
    "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_RELEASE_CHANNELS.get_value",
    return_value=True,
)
@pytest.mark.integration
@pytest.mark.parametrize(
    "test_project",
    [
        "napp_init_v2",
    ],
)
def test_nativeapp_run_from_non_default_release_channel(
    get_value_mock,
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    with nativeapp_project_directory(test_project):
        project_name = "myapp"

        # Create version and publish to default release directive of QA release channel
        result = runner.invoke_with_connection(
            ["app", "publish", "--version", "v1", "--create-version", "--channel", "QA"]
        )
        assert result.exit_code == 0

        # run from release directive of QA release channel
        result = runner.invoke_with_connection(
            ["app", "run", "--from-release-directive", "--channel", "QA"]
        )
        assert result.exit_code == 0

        app_name = f"{project_name}_{default_username}{resource_suffix}".upper()
        expect = row_from_snowflake_session(
            snowflake_session.execute_string(f"desc application {app_name}")
        )
        assert contains_row_with(
            expect, {"property": "release_channel_name", "value": "QA"}
        )
