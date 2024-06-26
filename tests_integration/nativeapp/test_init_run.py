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

from snowflake.cli.api.project.util import generate_user_env
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.nativeapp.init import OFFICIAL_TEMPLATES_GITHUB_URL

from tests.project.fixtures import *
from tests_integration.test_utils import (
    pushd,
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
    rows_from_snowflake_session,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


# Tests a simple flow of initiating a new project, executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
def test_nativeapp_init_run_without_modifications(
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
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
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
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'",
                    )
                ),
                dict(name=app_name),
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


# Tests a simple flow of an existing project, but executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize("project_definition_files", ["integration"], indirect=True)
def test_nativeapp_run_existing(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
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


# Tests a simple flow of initiating a project, executing snow app run and teardown, all with distribution=internal
@pytest.mark.integration
def test_nativeapp_init_run_handles_spaces(
    runner,
    snowflake_session,
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
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
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
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'",
                    )
                ),
                dict(name=app_name),
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


# Tests a simple flow of an existing project, but executing snow app run and teardown, all with distribution=external
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration_external"], indirect=True
)
def test_nativeapp_run_existing_w_external(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration_external"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
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

            # make sure we always delete the app, --force required for external distribution
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

            expect = snowflake_session.execute_string(
                f"show applications like '{app_name}'"
            )
            assert not_contains_row_with(
                row_from_snowflake_session(expect), {"name": app_name}
            )

            expect = snowflake_session.execute_string(
                f"show application packages like '{package_name}'"
            )
            assert not_contains_row_with(
                row_from_snowflake_session(expect), {"name": package_name}
            )

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Verifies that running "app run" after "app deploy" upgrades the app
@pytest.mark.integration
def test_nativeapp_run_after_deploy(
    runner,
    temporary_working_directory,
):
    project_name = "myapp"
    app_name = f"{project_name}_{USER_NAME}"
    stage_fqn = f"{project_name}_pkg_{USER_NAME}.app_src.stage"

    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_name)):
        try:
            # Run #1
            result = runner.invoke_with_connection_json(
                ["app", "run"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

            # Make a change & deploy
            with open("app/README.md", "a") as file:
                file.write("### Test")
            result = runner.invoke_with_connection_json(
                ["app", "deploy"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

            # Run #2
            result = runner.invoke_with_connection_json(
                ["app", "run", "--debug"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert (
                f"alter application {app_name} upgrade using @{stage_fqn}"
                in result.output
            )

        finally:
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


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
                ],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
        finally:
            single_template_repo.close()


# Tests that application post-deploy scripts are executed by creating a post_deploy_log table and having each post-deploy script add a record to it
@pytest.mark.integration
def test_nativeapp_app_post_deploy(runner, snowflake_session, project_directory):
    project_name = "myapp"
    app_name = f"{project_name}_{USER_NAME}"
    with project_directory("napp_application_post_deploy") as tmp_dir:
        try:
            # First run, application is created
            result = runner.invoke_with_connection_json(
                ["app", "run"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

            # Verify both scripts were executed
            assert row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"select * from {app_name}.public.post_deploy_log",
                )
            ) == [
                {"TEXT": "post-deploy-part-1"},
                {"TEXT": "post-deploy-part-2"},
            ]

            # Second run, application is upgraded
            result = runner.invoke_with_connection_json(
                ["app", "run"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

            # Verify both scripts were executed
            assert row_from_snowflake_session(
                snowflake_session.execute_string(
                    f"select * from {app_name}.public.post_deploy_log",
                )
            ) == [
                {"TEXT": "post-deploy-part-1"},
                {"TEXT": "post-deploy-part-2"},
                {"TEXT": "post-deploy-part-1"},
                {"TEXT": "post-deploy-part-2"},
            ]

        finally:
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


# Tests running an app whose package was dropped externally (requires dropping and recreating the app)
@pytest.mark.integration
@pytest.mark.parametrize("project_definition_files", ["integration"], indirect=True)
@pytest.mark.parametrize("force_flag", [True, False])
def test_nativeapp_run_orphan(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
    force_flag,
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
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
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'"
                    )
                ),
                dict(name=app_name, source=package_name),
            )

            result = runner.invoke_with_connection(
                ["sql", "-q", f"drop application package {package_name}"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0, result.output

            # package doesn't exist, app not readable
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{USER_NAME}".upper()
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
            result = runner.invoke_with_connection(command, input=_input, env=TEST_ENV)
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

            # make sure we always delete the app
            result = runner.invoke_with_connection_json(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

        finally:
            # manually drop the application in case the test failed and it wasn't dropped
            result = runner.invoke_with_connection(
                ["sql", "-q", f"drop application if exists {app_name} cascade"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0, result.output

            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
