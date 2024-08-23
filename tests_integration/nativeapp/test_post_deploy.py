# Tests that application post-deploy scripts are executed by creating a post_deploy_log table and having each post-deploy script add a record to it

import pytest

from tests_integration.test_utils import (
    enable_definition_v2_feature_flag,
    row_from_snowflake_session,
)
from tests_integration.testing_utils.working_directory_utils import (
    WorkingDirectoryChanger,
)


def run(runner, args):
    result = runner.invoke_with_connection_json(["app", "run"] + args)
    assert result.exit_code == 0


def deploy(runner, args):
    result = runner.invoke_with_connection_json(["app", "deploy"] + args)
    assert result.exit_code == 0


def teardown(runner, args):
    result = runner.invoke_with_connection_json(["app", "teardown", "--force"] + args)
    assert result.exit_code == 0


def create_version(runner, version, args):
    result = runner.invoke_with_connection_json(
        ["app", "version", "create", version] + args
    )
    assert result.exit_code == 0


def drop_version(runner, version, args):
    result = runner.invoke_with_connection_json(
        ["app", "version", "drop", version, "--force"] + args
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
@pytest.mark.parametrize("is_versioned", [True, False])
@pytest.mark.parametrize("with_project_flag", [True, False])
def test_nativeapp_post_deploy(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    test_project,
    is_versioned,
    with_project_flag,
):
    version = "v1"
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"
    pkg_name = f"{project_name}_pkg_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project) as tmp_dir:
        project_args = ["--project", f"{tmp_dir}"] if with_project_flag else []
        version_run_args = ["--version", version] if is_versioned else []

        if with_project_flag:
            working_directory_changer = WorkingDirectoryChanger()
            working_directory_changer.change_working_directory_to("app")

        try:
            # first run, application is created
            if is_versioned:
                create_version(runner, version, project_args)
            run(runner, project_args + version_run_args)

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
            if is_versioned:
                create_version(runner, version, project_args)
            run(runner, project_args + version_run_args)

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
            if is_versioned:
                # need to drop the version before we can teardown
                drop_version(runner, version, project_args)
            teardown(runner, project_args)
