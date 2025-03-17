# Tests that application post-deploy scripts are executed by creating a post_deploy_log table and having each post-deploy script add a record to it
from pathlib import Path

import pytest
import yaml

from tests.nativeapp.factories import ProjectV11Factory
from tests_common import IS_WINDOWS
from tests_integration.test_utils import (
    row_from_snowflake_session,
)
from tests_integration.testing_utils.working_directory_utils import (
    WorkingDirectoryChanger,
)


def run(runner, base_command, args):
    if base_command == "ws":
        result = runner.invoke_with_connection_json(
            ["ws", "deploy", "--entity-id", "app"] + args
        )
    else:
        result = runner.invoke_with_connection_json(["app", "run"] + args)

    assert result.exit_code == 0


def deploy(runner, base_command, args):
    if base_command == "ws":
        result = runner.invoke_with_connection_json(
            ["ws", "deploy", "--entity-id=pkg"] + args
        )
    else:
        result = runner.invoke_with_connection_json([base_command, "deploy"] + args)
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
@pytest.mark.parametrize(
    "base_command,test_project",
    [
        ["app", "napp_application_post_deploy_v1"],
        ["app", "napp_application_post_deploy_v2"],
        ["ws", "napp_application_post_deploy_v2"],
    ],
)
def test_nativeapp_post_deploy(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    base_command,
    test_project,
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"
    pkg_name = f"{project_name}_pkg_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project):
        try:
            # first run, application is created
            run(runner, base_command, [])

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
            run(runner, base_command, [])

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

            deploy(runner, base_command, [])

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
            teardown(runner, [])


@pytest.mark.integration
@pytest.mark.parametrize(
    "base_command,test_project",
    [
        ["app", "napp_application_post_deploy_v2"],
    ],
)
def test_nativeapp_post_deploy_versioned(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    base_command,
    test_project,
):
    version = "v1"
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"
    pkg_name = f"{project_name}_pkg_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project):
        version_run_args = ["--version", version]

        try:
            # first run, application is created
            create_version(runner, version, [])
            run(runner, base_command, version_run_args)

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
            create_version(runner, version, [])
            run(runner, base_command, version_run_args)

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

            deploy(runner, base_command, [])

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
            drop_version(runner, version, [])
            teardown(runner, [])


@pytest.mark.integration
@pytest.mark.parametrize(
    "base_command,test_project",
    [
        ["app", "napp_application_post_deploy_v2"],
    ],
)
def test_nativeapp_post_deploy_with_project_flag(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    base_command,
    test_project,
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"
    pkg_name = f"{project_name}_pkg_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project) as tmp_dir:
        project_args = ["--project", f"{tmp_dir}"]

        working_directory_changer = WorkingDirectoryChanger()
        working_directory_changer.change_working_directory_to("app")

        try:
            # first run, application is created
            run(runner, base_command, project_args)

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
            run(runner, base_command, project_args)

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

            deploy(runner, base_command, project_args)

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
@pytest.mark.skipif(
    not IS_WINDOWS, reason="Testing windows path should only be done on Windows"
)
@pytest.mark.parametrize(
    "base_command,test_project",
    [
        ["app", "napp_application_post_deploy_v2"],
    ],
)
def test_nativeapp_post_deploy_with_windows_path(
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
    nativeapp_project_directory,
    base_command,
    test_project,
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"
    pkg_name = f"{project_name}_pkg_{default_username}{resource_suffix}"

    with nativeapp_project_directory(test_project) as tmp_dir:
        try:
            snowflake_yml_path = tmp_dir / "snowflake.yml"
            with open(snowflake_yml_path, "r", encoding="utf-8") as file:
                yml = yaml.safe_load(file)
                if yml["definition_version"] >= 2:
                    artifact = yml["entities"]["pkg"]["artifacts"][0]
                    pkg_scripts = yml["entities"]["pkg"]["meta"]["post_deploy"]
                    app_scripts = yml["entities"]["app"]["meta"]["post_deploy"]
                else:
                    artifact = yml["native_app"]["artifacts"][0]
                    pkg_scripts = yml["native_app"]["package"]["post_deploy"]
                    app_scripts = yml["native_app"]["application"]["post_deploy"]

                # use backslashes in artifacts src
                artifact["src"] = artifact["src"].replace("/", "\\")

                # use backslashes in post_deploy paths, as well as full paths with backslashes
                pkg_scripts[0]["sql_script"] = pkg_scripts[0]["sql_script"].replace(
                    "/", "\\"
                )
                pkg_scripts[1]["sql_script"] = str(
                    (tmp_dir / pkg_scripts[1]["sql_script"]).absolute()
                )

                # use backslashes in post_deploy paths, as well as full paths with backslashes
                app_scripts[0]["sql_script"] = app_scripts[0]["sql_script"].replace(
                    "/", "\\"
                )
                app_scripts[1]["sql_script"] = str(
                    (tmp_dir / app_scripts[1]["sql_script"]).absolute()
                )

            with open(snowflake_yml_path, "w", encoding="utf-8") as file:
                yaml.dump(yml, file)

            run(runner, base_command, [])

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

        finally:
            teardown(runner, [])


@pytest.mark.integration
@pytest.mark.parametrize(
    "base_command,test_project",
    [["app", "napp_application_post_deploy_v2"]],
)
def test_nativeapp_post_deploy_logs_relative_paths(
    runner, nativeapp_project_directory, base_command, test_project
):
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection(["app", "run"])
        for filename in [
            "scripts/app_post_deploy1.sql",
            "scripts/app_post_deploy2.sql",
            "scripts/package_post_deploy1.sql",
            "scripts/package_post_deploy2.sql",
        ]:
            assert f"Executing SQL script: {filename}" in result.output


@pytest.mark.integration
def test_nativeapp_converted_package_scripts_logs_relative_paths(
    runner, nativeapp_teardown, temporary_directory
):
    package_scripts = {
        "scripts/package_script1.sql": "select 'package script 1 for {{ package_name }}'",
        "scripts/package_script2.sql": "select 'package script 2 for {{ package_name }}'",
    }
    manifest = yaml.safe_dump(
        dict(
            manifest_version=1,
            artifacts={
                "setup_script": "setup.sql",
                "readme": "README.md",
            },
        )
    )
    ProjectV11Factory(
        pdf__native_app__package__scripts=list(package_scripts),
        pdf__native_app__artifacts=["README.md", "setup.sql", "manifest.yml"],
        files={
            "README.md": "",
            "setup.sql": "select 1",
            "manifest.yml": manifest,
        }
        | package_scripts,
    )
    with nativeapp_teardown(project_dir=Path(temporary_directory)):
        result = runner.invoke_with_connection(["app", "run"])
        for filename in package_scripts:
            assert f"Executing SQL script: {filename}" in result.output
