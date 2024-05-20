import os
import uuid

from snowflake.cli.api.project.util import generate_user_env


from tests.nativeapp.utils import touch

from tests.project.fixtures import *
from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    pushd,
    row_from_snowflake_session,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


# Tests a simple flow of executing "snow app deploy", verifying that an application package was created, and an application was not
@pytest.mark.integration
def test_nativeapp_deploy(
    runner,
    snowflake_session,
    temporary_working_directory,
):
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_name)):
        result = runner.invoke_with_connection_json(
            ["app", "deploy"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

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

            # make sure we always delete the package
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "command,contains,not_contains",
    [
        # deploy --prune removes remote-only files
        ["app deploy --prune", ["stage/manifest.yml"], ["stage/README.md"]],
        # deploy removes remote-only files (--prune is the default value)
        ["app deploy", ["stage/manifest.yml"], ["stage/README.md"]],
        # deploy --no-prune does not delete remote-only files
        ["app deploy --no-prune", ["stage/README.md"], []],
    ],
)
def test_nativeapp_deploy_prune(
    command,
    contains,
    not_contains,
    runner,
    snowflake_session,
    temporary_working_directory,
):
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_name)):
        result = runner.invoke_with_connection_json(
            ["app", "deploy"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # delete a file locally
            os.remove(os.path.join("app", "README.md"))

            # deploy
            result = runner.invoke_with_connection_json(
                command.split(),
                env=TEST_ENV,
            )
            assert result.exit_code == 0

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
def test_nativeapp_deploy_files(
    runner,
    temporary_working_directory,
):
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_name)):
        # sync only two specific files to stage
        result = runner.invoke_with_connection_json(
            ["app", "deploy", "app/manifest.yml", "app/setup_script.sql"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

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
def test_nativeapp_deploy_nested_directories(
    runner,
    temporary_working_directory,
):
    project_name = "myapp"
    project_dir = "app root"
    result = runner.invoke_json(
        ["app", "init", project_dir, "--name", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_dir)):
        # create nested file under app/
        touch("app/nested/dir/file.txt")

        result = runner.invoke_with_connection_json(
            ["app", "deploy", "app/nested/dir/file.txt"],
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
def test_nativeapp_deploy_directory(
    runner,
    temporary_working_directory,
):
    project_name = "myapp"
    project_dir = "app root"
    result = runner.invoke_json(
        ["app", "init", project_dir, "--name", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_dir)):
        touch("app/dir/file.txt")
        result = runner.invoke_with_connection_json(
            ["app", "deploy", "app/dir", "-r"],
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
def test_nativeapp_deploy_directory_no_recursive(
    runner,
    temporary_working_directory,
):
    project_name = "myapp"
    project_dir = "app root"
    result = runner.invoke_json(
        ["app", "init", project_dir, "--name", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_dir)):
        try:
            touch("app/nested/dir/file.txt")
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "app/nested"],
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
def test_nativeapp_deploy_unknown_path(
    runner,
    temporary_working_directory,
):
    project_name = "myapp"
    project_dir = "app root"
    result = runner.invoke_json(
        ["app", "init", project_dir, "--name", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_dir)):
        try:
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "does_not_exist"],
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


# Tests that specifying an path with no deploy artifact results in an error
@pytest.mark.integration
def test_nativeapp_deploy_path_with_no_mapping(
    runner,
    temporary_working_directory,
):
    project_name = "myapp"
    project_dir = "app root"
    result = runner.invoke_json(
        ["app", "init", project_dir, "--name", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_dir)):
        try:
            result = runner.invoke_with_connection_json(
                ["app", "deploy", "snowflake.yml"],
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
