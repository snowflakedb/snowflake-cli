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

from snowflake.cli._plugins.nativeapp.init import OFFICIAL_TEMPLATES_GITHUB_URL
from snowflake.cli.api.secure_path import SecurePath
from tests.project.fixtures import *
from tests_integration.test_utils import (
    pushd,
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)


# Tests a simple flow of initiating a new project, executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
def test_nativeapp_init_run_without_modifications(
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json(["app", "run"])
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
    with pushd(project_dir):
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


# Tests a simple flow of initiating a project, executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
def test_nativeapp_init_run_handles_spaces(
    test_project,
    nativeapp_project_directory,
    runner,
    snowflake_session,
    default_username,
    resource_suffix,
):
    project_name = "myapp"
    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection_json(["app", "run"])
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


# Tests a simple flow of an existing project, but executing snow app run and teardown, all with distribution=external
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files",
    ["integration_external", "integration_external_v2"],
    indirect=True,
)
def test_nativeapp_run_existing_w_external(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
    default_username,
    nativeapp_teardown,
    resource_suffix,
):
    project_name = "integration_external"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
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

            # app package contains distribution=external
            expect = row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"desc application package {package_name}"
                )
            )
            assert contains_row_with(
                expect, {"property": "name", "value": package_name}
            )
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
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
def test_nativeapp_run_after_deploy(
    test_project, nativeapp_project_directory, runner, default_username, resource_suffix
):
    project_name = "myapp"
    app_name = f"{project_name}_{default_username}{resource_suffix}"
    stage_fqn = f"{project_name}_pkg_{default_username}{resource_suffix}.app_src.stage"

    with nativeapp_project_directory(test_project):
        # Run #1
        result = runner.invoke_with_connection_json(["app", "run"])
        assert result.exit_code == 0

        # Make a change & deploy
        with open("app/README.md", "a") as file:
            file.write("### Test")
        result = runner.invoke_with_connection_json(["app", "deploy"])
        assert result.exit_code == 0

        # Run #2
        result = runner.invoke_with_connection_json(["app", "run", "--debug"])
        assert result.exit_code == 0
        assert (
            f"alter application {app_name} upgrade using @{stage_fqn}" in result.output
        )


# Tests initialization of a project from a repo with a single template
@pytest.mark.integration
def test_nativeapp_init_from_repo_with_single_template(
    runner,
    snowflake_session,
    temporary_working_directory,
):
    from git import Repo
    from git import rmtree as git_rmtree

    with SecurePath.temporary_directory() as all_templates_local_repo_path:
        # prepare a local repository with only one template (basic)
        all_templates_repo = Repo.clone_from(
            url=OFFICIAL_TEMPLATES_GITHUB_URL,
            to_path=all_templates_local_repo_path.path,
            filter=["tree:0"],
            depth=1,
        )
        all_templates_repo.close()
        git_rmtree((all_templates_local_repo_path / ".git").path)

        single_template_repo_path = all_templates_local_repo_path / "basic"
        single_template_repo = Repo.init(single_template_repo_path.path)
        single_template_repo.index.add(["**/*", "*", ".gitignore"])
        single_template_repo.index.commit("initial commit")

        # confirm that no error is thrown when initializing a project from a repo with a single template
        project_name = "myapp"
        try:
            result = runner.invoke_json(
                [
                    "app",
                    "init",
                    "--template-repo",
                    f"file://{single_template_repo_path.path}",
                    project_name,
                ]
            )
            assert result.exit_code == 0
        finally:
            single_template_repo.close()


# Tests running an app whose package was dropped externally (requires dropping and recreating the app)
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration", "integration_v2"], indirect=True
)
@pytest.mark.parametrize("force_flag", [True, False])
def test_nativeapp_run_orphan(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
    force_flag,
    default_username,
    resource_suffix,
    nativeapp_teardown,
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
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
                dict(name=app_name, source=package_name),
            )

            result = runner.invoke_with_connection(
                ["sql", "-q", f"drop application package {package_name}"]
            )
            assert result.exit_code == 0, result.output

            # package doesn't exist, app not readable
            package_name = (
                f"{project_name}_pkg_{default_username}{resource_suffix}".upper()
            )
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
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'"
                    )
                ),
                dict(name=app_name, source=package_name),
            )

            if force_flag:
                command = ["app", "run", "--force"]
                _input = None
            else:
                command = ["app", "run", "--interactive"]  # show prompt in tests
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
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'"
                    )
                ),
                dict(name=app_name, source=package_name),
            )


# Verifies that we can always cross-upgrade between different
# run configurations as long as we pass the --force flag to "app run"
# TODO: add back all parameterizations and implement --force for "app teardown"
@pytest.mark.integration
@pytest.mark.parametrize("test_project", ["napp_init_v1", "napp_init_v2"])
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
