# Tests that application post-deploy scripts are executed by creating a post_deploy_log table and having each post-deploy script add a record to it
import uuid
import pytest

from snowflake.cli.api.project.util import generate_user_env
from tests_integration.test_utils import (
    enable_definition_v2_feature_flag,
    row_from_snowflake_session,
)
from tests_integration.testing_utils.working_directory_utils import (
    WorkingDirectoryChanger,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


def run(runner, args):
    result = runner.invoke_with_connection_json(
        ["app", "run"] + args,
        env=TEST_ENV,
    )
    assert result.exit_code == 0


def deploy(runner, args):
    result = runner.invoke_with_connection_json(
        ["app", "deploy"] + args,
        env=TEST_ENV,
    )
    assert result.exit_code == 0


def teardown(runner, args):
    result = runner.invoke_with_connection_json(
        ["app", "teardown", "--force"] + args,
        env=TEST_ENV,
    )
    assert result.exit_code == 0


def create_version(runner, version, args):
    result = runner.invoke_with_connection_json(
        ["app", "version", "create", version] + args,
        env=TEST_ENV,
    )
    assert result.exit_code == 0


def drop_version(runner, version, args):
    result = runner.invoke_with_connection_json(
        ["app", "version", "drop", version, "--force"] + args,
        env=TEST_ENV,
    )
    assert result.exit_code == 0


def verify_app_post_deploy_log(snowflake_session, app_name, expected_rows):
    assert (
        row_from_snowflake_session(
            snowflake_session.execute_string(
                f"select * from {app_name}.app_schema.post_deploy_log",
            )
        )
        == expected_rows
    )


def verify_pkg_post_deploy_log(snowflake_session, pkg_name, expected_rows):
    assert (
        row_from_snowflake_session(
            snowflake_session.execute_string(
                f"select * from {pkg_name}.pkg_schema.post_deploy_log",
            )
        )
        == expected_rows
    )


@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize(
    "test_project",
    ["napp_application_post_deploy_v1", "napp_application_post_deploy_v2"],
)
@pytest.mark.parametrize("with_project_flag", [True, False])
def test_nativeapp_post_deploy(
    runner,
    snowflake_session,
    project_directory,
    test_project,
    with_project_flag,
):
    project_name = "myapp"
    app_name = f"{project_name}_{USER_NAME}"
    pkg_name = f"{project_name}_pkg_{USER_NAME}"

    with project_directory(test_project) as tmp_dir:
        project_args = ["--project", f"{tmp_dir}"] if with_project_flag else []

        if with_project_flag:
            working_directory_changer = WorkingDirectoryChanger()
            working_directory_changer.change_working_directory_to("app")

        try:
            # first run, application is created
            run(runner, project_args)

            # Verify both scripts were executed
            verify_app_post_deploy_log(
                snowflake_session,
                app_name,
                [
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                ],
            )

            verify_pkg_post_deploy_log(
                snowflake_session,
                pkg_name,
                [
                    {"TEXT": "package-post-deploy-part-1"},
                    {"TEXT": "package-post-deploy-part-2"},
                ],
            )

            # Second run, application is upgraded
            run(runner, project_args)

            verify_app_post_deploy_log(
                snowflake_session,
                app_name,
                [
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                ],
            )
            verify_pkg_post_deploy_log(
                snowflake_session,
                pkg_name,
                [
                    {"TEXT": "package-post-deploy-part-1"},
                    {"TEXT": "package-post-deploy-part-2"},
                    {"TEXT": "package-post-deploy-part-1"},
                    {"TEXT": "package-post-deploy-part-2"},
                ],
            )

            deploy(runner, project_args)

            verify_app_post_deploy_log(
                snowflake_session,
                app_name,
                [
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                ],
            )
            verify_pkg_post_deploy_log(
                snowflake_session,
                pkg_name,
                [
                    {"TEXT": "package-post-deploy-part-1"},
                    {"TEXT": "package-post-deploy-part-2"},
                    {"TEXT": "package-post-deploy-part-1"},
                    {"TEXT": "package-post-deploy-part-2"},
                    {"TEXT": "package-post-deploy-part-1"},
                    {"TEXT": "package-post-deploy-part-2"},
                ],
            )

        finally:
            teardown(runner, project_args)


@pytest.mark.integration
@enable_definition_v2_feature_flag
@pytest.mark.parametrize(
    "test_project",
    ["napp_application_post_deploy_v1", "napp_application_post_deploy_v2"],
)
def test_nativeapp_post_deploy_with_version(
    runner,
    snowflake_session,
    project_directory,
    test_project,
):
    version = "v1"
    project_name = "myapp"
    app_name = f"{project_name}_{USER_NAME}"
    pkg_name = f"{project_name}_pkg_{USER_NAME}"

    with project_directory(test_project) as tmp_dir:
        version_run_args = ["--version", version]

        try:
            create_version(runner, version, [])
            run(runner, version_run_args)

            verify_app_post_deploy_log(
                snowflake_session,
                app_name,
                [
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                ],
            )

            create_version(runner, version, [])
            run(runner, version_run_args)

            verify_app_post_deploy_log(
                snowflake_session,
                app_name,
                [
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                ],
            )

            deploy(runner, [])

            verify_app_post_deploy_log(
                snowflake_session,
                app_name,
                [
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                    {"TEXT": "app-post-deploy-part-1"},
                    {"TEXT": "app-post-deploy-part-2"},
                ],
            )
            verify_pkg_post_deploy_log(
                snowflake_session,
                pkg_name,
                [
                    {"TEXT": "package-post-deploy-part-1"},
                    {"TEXT": "package-post-deploy-part-2"},
                ],
            )

        finally:
            # need to drop the version before we can teardown
            drop_version(runner, version, [])
            teardown(runner, [])
