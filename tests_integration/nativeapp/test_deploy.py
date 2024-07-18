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
import uuid

import yaml

from snowflake.cli.api.project.util import generate_user_env


from tests.nativeapp.utils import touch

from tests.project.fixtures import *
from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    pushd,
    row_from_snowflake_session,
    enable_definition_v2_feature_flag,
)
from tests_integration.testing_utils import (
    assert_that_result_failed_with_message_containing,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


def sanitize_deploy_output(output):
    deploy_root = Path("output/deploy").resolve()
    return output.replace(USER_NAME, "@@USER@@").replace(
        str(deploy_root), "@@DEPLOY_ROOT@@"
    )


# Tests a simple flow of executing "snow app deploy", verifying that an application package was created, and an application was not
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy(
    definition_version,
    project_directory,
    runner,
    snowflake_session,
    snapshot,
    print_paths_as_posix,
):
    project_name = "myapp"
    with project_directory(f"napp_init_{definition_version}"):
        result = runner.invoke_with_connection(
            ["app", "deploy"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0
        assert sanitize_deploy_output(result.output) == snapshot

        try:
            # package exist
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{USER_NAME}".upper()
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
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
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
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "--debug"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert "Successfully uploaded chunk 0 of file" not in result.output

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize(
    "command,contains,not_contains",
    [
        # deploy --prune removes remote-only files
        [
            "app deploy --prune --no-validate",
            ["stage/manifest.yml"],
            ["stage/README.md"],
        ],
        # deploy removes remote-only files (--prune is the default value)
        ["app deploy --no-validate", ["stage/manifest.yml"], ["stage/README.md"]],
        # deploy --no-prune does not delete remote-only files
        ["app deploy --no-prune", ["stage/README.md"], []],
    ],
)
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_prune(
    command,
    contains,
    not_contains,
    definition_version,
    project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
):
    project_name = "myapp"
    with project_directory(f"napp_init_{definition_version}"):
        result = runner.invoke_with_connection_json(
            ["app", "deploy"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # delete a file locally
            os.remove(os.path.join("app", "README.md"))

            # deploy
            result = runner.invoke_with_connection(
                command.split(),
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert sanitize_deploy_output(result.output) == snapshot

            # verify the file does not exist on the stage
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
            )
            for name in contains:
                assert contains_row_with(stage_files.json, {"name": name})
            for name in not_contains:
                assert not_contains_row_with(stage_files.json, {"name": name})

            # make sure we always delete the app
            result = runner.invoke_with_connection_json(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests a simple flow of executing "snow app deploy [files]", verifying that only the specified files are synced to the stage
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_files(
    definition_version,
    project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
):
    project_name = "myapp"
    with project_directory(f"napp_init_{definition_version}"):
        # sync only two specific files to stage
        result = runner.invoke_with_connection(
            [
                "app",
                "deploy",
                "app/manifest.yml",
                "app/setup_script.sql",
                "--no-validate",
            ],
            env=TEST_ENV,
        )
        assert result.exit_code == 0
        assert sanitize_deploy_output(result.output) == snapshot

        try:
            # manifest and script files exist, readme doesn't exist
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
            )
            assert contains_row_with(stage_files.json, {"name": "stage/manifest.yml"})
            assert contains_row_with(
                stage_files.json, {"name": "stage/setup_script.sql"}
            )
            assert not_contains_row_with(stage_files.json, {"name": "stage/README.md"})

            # make sure we always delete the app
            result = runner.invoke_with_connection_json(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests that files inside of a symlinked directory are deployed
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_nested_directories(
    definition_version,
    project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
):
    project_name = "myapp"
    with project_directory(f"napp_init_{definition_version}"):
        # create nested file under app/
        touch("app/nested/dir/file.txt")

        result = runner.invoke_with_connection(
            ["app", "deploy", "app/nested/dir/file.txt", "--no-validate"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0
        assert sanitize_deploy_output(result.output) == snapshot

        try:
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
            )
            assert contains_row_with(
                stage_files.json, {"name": "stage/nested/dir/file.txt"}
            )

            # make sure we always delete the app
            result = runner.invoke_with_connection_json(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests that deploying a directory recursively syncs all of its contents
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_directory(
    definition_version,
    project_directory,
    runner,
):
    project_name = "myapp"
    with project_directory(f"napp_init_{definition_version}"):
        touch("app/dir/file.txt")
        result = runner.invoke_with_connection(
            ["app", "deploy", "app/dir", "--no-recursive", "--no-validate"],
            env=TEST_ENV,
        )
        assert_that_result_failed_with_message_containing(
            result, "Add the -r flag to deploy directories."
        )

        result = runner.invoke_with_connection(
            ["app", "deploy", "app/dir", "-r", "--no-validate"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
            )
            assert contains_row_with(stage_files.json, {"name": "stage/dir/file.txt"})

            # make sure we always delete the app
            result = runner.invoke_with_connection_json(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests that deploying a directory without specifying -r returns an error
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_directory_no_recursive(
    definition_version,
    project_directory,
    runner,
):
    with project_directory(f"napp_init_{definition_version}"):
        try:
            touch("app/nested/dir/file.txt")
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "app/nested", "--no-validate"],
                env=TEST_ENV,
            )
            assert result.exit_code == 1, result.output

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests that specifying an unknown path to deploy results in an error
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_unknown_path(
    definition_version,
    project_directory,
    runner,
):
    with project_directory(f"napp_init_{definition_version}"):
        try:
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "does_not_exist", "--no-validate"],
                env=TEST_ENV,
            )
            assert result.exit_code == 1
            assert "The following path does not exist:" in result.output

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests that specifying a path with no deploy artifact results in an error
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_path_with_no_mapping(
    definition_version,
    project_directory,
    runner,
):
    with project_directory(f"napp_init_{definition_version}"):
        try:
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "snowflake.yml", "--no-validate"],
                env=TEST_ENV,
            )
            assert result.exit_code == 1
            assert "No artifact found for" in result.output

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests that specifying a path and pruning result in an error
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_rejects_pruning_when_path_is_specified(
    definition_version,
    project_directory,
    runner,
):
    with project_directory(f"napp_init_{definition_version}"):
        try:
            os.unlink("app/README.md")
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "app/README.md", "--prune"],
                env=TEST_ENV,
            )
            assert result.exit_code == 1
            assert (
                "--prune cannot be used when paths are also specified" in result.output
            )

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests that specifying a path with no direct mapping falls back to search for prefix matches
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_looks_for_prefix_matches(
    definition_version,
    project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
):
    project_name = "myapp"

    with project_directory(f"napp_deploy_prefix_matches_{definition_version}"):
        try:
            result = runner.invoke_with_connection(
                ["app", "deploy", "-r", "app"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert sanitize_deploy_output(result.output) == snapshot

            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
            )
            assert contains_row_with(stage_files.json, {"name": "stage/manifest.yml"})
            assert contains_row_with(
                stage_files.json, {"name": "stage/setup_script.sql"}
            )
            assert contains_row_with(stage_files.json, {"name": "stage/README.md"})
            assert not_contains_row_with(
                stage_files.json, {"name": "stage/src/main.py"}
            )
            assert not_contains_row_with(
                stage_files.json, {"name": "stage/parent-lib/child/c/c.py"}
            )

            result = runner.invoke_with_connection(
                ["app", "deploy", "-r", "lib/parent/child/c"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
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
                ["app", "deploy", "lib/parent/child/a.py"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
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

            result = runner.invoke_with_connection(
                ["app", "deploy", "lib", "-r"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
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

        finally:
            result = runner.invoke_with_connection(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests that snow app deploy -r . deploys all changes
@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize("definition_version", ["v1", "v2"])
def test_nativeapp_deploy_dot(
    definition_version,
    project_directory,
    runner,
    snapshot,
    print_paths_as_posix,
):
    project_name = "myapp"
    with project_directory(f"napp_init_{definition_version}"):
        try:
            result = runner.invoke_with_connection(
                ["app", "deploy", "-r", "."],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert sanitize_deploy_output(result.output) == snapshot

            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            stage_name = "app_src.stage"  # as defined in native-apps-templates/basic
            stage_files = runner.invoke_with_connection_json(
                ["stage", "list-files", f"{package_name}.{stage_name}"],
                env=TEST_ENV,
            )
            assert contains_row_with(stage_files.json, {"name": "stage/manifest.yml"})
            assert contains_row_with(
                stage_files.json, {"name": "stage/setup_script.sql"}
            )
            assert contains_row_with(stage_files.json, {"name": "stage/README.md"})

        finally:
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
