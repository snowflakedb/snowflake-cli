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

import os
from shlex import split

from snowflake.cli.api.project.util import TEST_RESOURCE_SUFFIX_VAR
from tests.nativeapp.utils import touch

from tests.project.fixtures import *
from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)
from tests_integration.testing_utils import (
    assert_that_result_failed_with_message_containing,
    assert_that_result_is_usage_error,
)


@pytest.fixture
def sanitize_deploy_output(default_username, resource_suffix):
    def _sanitize_deploy_output(output):
        deploy_root = Path("output/deploy").resolve()
        user_and_suffix = f"{default_username}{resource_suffix}"
        return output.replace(user_and_suffix, "@@USER@@").replace(
            str(deploy_root), "@@DEPLOY_ROOT@@"
        )

    return _sanitize_deploy_output


# Tests a simple flow of executing "snow app deploy", verifying that an application package was created, and an application was not
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy", "napp_init_v1"],
        ["app deploy", "napp_init_v2"],
        ["ws deploy --entity-id=pkg", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    sanitize_deploy_output,
    snapshot,
    print_paths_as_posix,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection(split(command))
        assert result.exit_code == 0
        assert "Validating Snowflake Native App setup script." in result.output
        assert sanitize_deploy_output(result.output) == snapshot

        # package exist
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

        # manifest file exists
        stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(stage_files.json, {"name": "stage/manifest.yml"})

        # app does not exist
        assert not_contains_row_with(
            row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"show applications like '{app_name}'",
                )
            ),
            dict(name=app_name),
        )

        # re-deploying should be a no-op; make sure we don't issue any PUT commands
        result = runner.invoke_with_connection([*split(command), "--debug"])
        assert result.exit_code == 0
        assert "Successfully uploaded chunk 0 of file" not in result.output


@pytest.mark.integration
@pytest.mark.parametrize(
    "command,contains,not_contains,test_project",
    [
        # deploy --prune removes remote-only files
        [
            "app deploy --prune --no-validate",
            ["stage/manifest.yml"],
            ["stage/README.md"],
            "napp_init_v1",
        ],
        [
            "app deploy --prune --no-validate",
            ["stage/manifest.yml"],
            ["stage/README.md"],
            "napp_init_v2",
        ],
        [
            "ws deploy --entity-id=pkg --prune --no-validate",
            ["stage/manifest.yml"],
            ["stage/README.md"],
            "napp_init_v2",
        ],
        # deploy removes remote-only files (--prune is the default value)
        [
            "app deploy --no-validate",
            ["stage/manifest.yml"],
            ["stage/README.md"],
            "napp_init_v1",
        ],
        [
            "app deploy --no-validate",
            ["stage/manifest.yml"],
            ["stage/README.md"],
            "napp_init_v2",
        ],
        [
            "ws deploy --entity-id=pkg --no-validate",
            ["stage/manifest.yml"],
            ["stage/README.md"],
            "napp_init_v2",
        ],
        # deploy --no-prune does not delete remote-only files
        [
            "app deploy --no-prune",
            ["stage/README.md"],
            [],
            "napp_init_v1",
        ],
        [
            "app deploy --no-prune",
            ["stage/README.md"],
            [],
            "napp_init_v2",
        ],
        [
            "ws deploy --entity-id=pkg --no-prune",
            ["stage/README.md"],
            [],
            "napp_init_v2",
        ],
    ],
)
def test_nativeapp_deploy_prune(
    command,
    contains,
    not_contains,
    test_project,
    nativeapp_project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
    default_username,
    resource_suffix,
    sanitize_deploy_output,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json(["app", "deploy"])
        assert result.exit_code == 0

        # delete a file locally
        os.remove(os.path.join("app", "README.md"))

        # deploy
        result = runner.invoke_with_connection(split(command))
        assert result.exit_code == 0
        assert sanitize_deploy_output(result.output) == snapshot

        # verify the file does not exist on the stage
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        for name in contains:
            assert contains_row_with(stage_files.json, {"name": name})
        for name in not_contains:
            assert not_contains_row_with(stage_files.json, {"name": name})


# Tests a simple flow of executing "snow app deploy [files]", verifying that only the specified files are synced to the stage
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy --no-validate", "napp_init_v1"],
        ["app deploy --no-validate", "napp_init_v2"],
        ["ws deploy --entity-id=pkg --no-validate", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_files(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
    default_username,
    resource_suffix,
    sanitize_deploy_output,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        # sync only two specific files to stage
        result = runner.invoke_with_connection(
            [
                *split(command),
                "app/manifest.yml",
                "app/setup_script.sql",
            ]
        )
        assert result.exit_code == 0
        assert sanitize_deploy_output(result.output) == snapshot

        # manifest and script files exist, readme doesn't exist
        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(stage_files.json, {"name": "stage/manifest.yml"})
        assert contains_row_with(stage_files.json, {"name": "stage/setup_script.sql"})
        assert not_contains_row_with(stage_files.json, {"name": "stage/README.md"})


# Tests that files inside of a symlinked directory are deployed
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy --no-validate", "napp_init_v1"],
        ["app deploy --no-validate", "napp_init_v2"],
        ["ws deploy --entity-id=pkg --no-validate", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_nested_directories(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
    default_username,
    resource_suffix,
    sanitize_deploy_output,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        # create nested file under app/
        touch("app/nested/dir/file.txt")

        result = runner.invoke_with_connection(
            [*split(command), "app/nested/dir/file.txt"]
        )
        assert result.exit_code == 0
        assert sanitize_deploy_output(result.output) == snapshot

        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(
            stage_files.json, {"name": "stage/nested/dir/file.txt"}
        )


# Tests that deploying a directory recursively syncs all of its contents
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy --no-validate", "napp_init_v1"],
        ["app deploy --no-validate", "napp_init_v2"],
        ["ws deploy --entity-id=pkg --no-validate", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_directory(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    default_username,
    resource_suffix,
    sanitize_deploy_output,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        touch("app/dir/file.txt")
        result = runner.invoke_with_connection(
            [*split(command), "app/dir", "--no-recursive"]
        )
        assert_that_result_failed_with_message_containing(
            result, "Add the -r flag to deploy directories."
        )

        result = runner.invoke_with_connection([*split(command), "app/dir", "-r"])
        assert result.exit_code == 0

        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(stage_files.json, {"name": "stage/dir/file.txt"})


# Tests that deploying a directory without specifying -r returns an error
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy --no-validate", "napp_init_v1"],
        ["app deploy --no-validate", "napp_init_v2"],
        ["ws deploy --entity-id=pkg --no-validate", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_directory_no_recursive(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
):
    with nativeapp_project_directory(test_project):
        touch("app/nested/dir/file.txt")
        result = runner.invoke_with_connection_json([*split(command), "app/nested"])
        assert result.exit_code == 1, result.output


# Tests that specifying an unknown path to deploy results in an error
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy --no-validate", "napp_init_v1"],
        ["app deploy --no-validate", "napp_init_v2"],
        ["ws deploy --entity-id=pkg --no-validate", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_unknown_path(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
):
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json([*split(command), "does_not_exist"])
        assert result.exit_code == 1
        assert "The following path does not exist:" in result.output


# Tests that specifying a path with no deploy artifact results in an error
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy --no-validate", "napp_init_v1"],
        ["app deploy --no-validate", "napp_init_v2"],
        ["ws deploy --entity-id=pkg --no-validate", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_path_with_no_mapping(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
):
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json([*split(command), "snowflake.yml"])
        assert result.exit_code == 1
        assert "No artifact found for" in result.output


# Tests that specifying a path and pruning result in an error
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy", "napp_init_v1"],
        ["app deploy", "napp_init_v2"],
        ["ws deploy --entity-id=pkg", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_rejects_pruning_when_path_is_specified(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
):
    with nativeapp_project_directory(test_project):
        os.unlink("app/README.md")
        result = runner.invoke_with_connection_json(
            [*split(command), "app/README.md", "--prune"]
        )

        assert_that_result_is_usage_error(
            result,
            "Parameters 'paths' and '--prune' are incompatible and cannot be used simultaneously.",
        )


# Tests that specifying a path with no direct mapping falls back to search for prefix matches
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy", "napp_deploy_prefix_matches_v1"],
        ["app deploy", "napp_deploy_prefix_matches_v2"],
        ["ws deploy --entity-id=pkg", "napp_deploy_prefix_matches_v2"],
    ],
)
def test_nativeapp_deploy_looks_for_prefix_matches(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
    default_username,
    resource_suffix,
    sanitize_deploy_output,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection([*split(command), "-r", "app"])
        assert result.exit_code == 0
        assert sanitize_deploy_output(result.output) == snapshot

        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(stage_files.json, {"name": "stage/manifest.yml"})
        assert contains_row_with(stage_files.json, {"name": "stage/setup_script.sql"})
        assert contains_row_with(stage_files.json, {"name": "stage/README.md"})
        assert not_contains_row_with(stage_files.json, {"name": "stage/src/main.py"})
        assert not_contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/c/c.py"}
        )

        result = runner.invoke_with_connection(
            [*split(command), "-r", "lib/parent/child/c"]
        )
        assert result.exit_code == 0
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/c/c.py"}
        )
        assert not_contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/a.py"}
        )
        assert not_contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/b.py"}
        )

        result = runner.invoke_with_connection(
            [*split(command), "lib/parent/child/a.py"]
        )
        assert result.exit_code == 0
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/c/c.py"}
        )
        assert contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/a.py"}
        )
        assert not_contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/b.py"}
        )

        result = runner.invoke_with_connection([*split(command), "lib", "-r"])
        assert result.exit_code == 0
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/c/c.py"}
        )
        assert contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/a.py"}
        )
        assert contains_row_with(
            stage_files.json, {"name": "stage/parent-lib/child/b.py"}
        )


# Tests that snow app deploy -r . deploys all changes
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy", "napp_init_v1"],
        ["app deploy", "napp_init_v2"],
        ["ws deploy --entity-id=pkg", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_dot(
    command,
    test_project,
    nativeapp_project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
    default_username,
    resource_suffix,
    sanitize_deploy_output,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection([*split(command), "-r", "."])
        assert result.exit_code == 0
        assert sanitize_deploy_output(result.output) == snapshot

        package_name = f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
        stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
        stage_files = runner.invoke_with_connection_json(
            ["stage", "list-files", f"{package_name}.{stage_name}"]
        )
        assert contains_row_with(stage_files.json, {"name": "stage/manifest.yml"})
        assert contains_row_with(stage_files.json, {"name": "stage/setup_script.sql"})
        assert contains_row_with(stage_files.json, {"name": "stage/README.md"})


@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy", "napp_init_v1"],
        ["app deploy", "napp_init_v2"],
        ["ws deploy --entity-id=pkg", "napp_init_v2"],
    ],
)
def test_nativeapp_deploy_validate_failing(
    command, test_project, nativeapp_project_directory, runner
):
    with nativeapp_project_directory(test_project):
        # Create invalid SQL file
        Path("app/setup_script.sql").write_text("Lorem ipsum dolor sit amet")

        # validate the app's setup script, this will fail
        # because we include an empty file
        result = runner.invoke_with_connection(split(command))
        assert result.exit_code == 1, result.output
        assert "Snowflake Native App setup script failed validation." in result.output
        assert "syntax error" in result.output
