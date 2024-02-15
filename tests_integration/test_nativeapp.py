import os
import uuid

from snowflake.cli.api.project.util import generate_user_env

from tests.project.fixtures import *
from tests_integration.test_utils import (
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@contextmanager
def pushd(directory: Path):
    cwd = os.getcwd()
    os.chdir(directory)
    try:
        yield directory
    finally:
        os.chdir(cwd)


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


# Tests a simple flow of an existing project, executing snow app version create, drop and teardown, all with distribution=internal
@pytest.mark.integration
@pytest.mark.parametrize("project_definition_files", ["integration"], indirect=True)
def test_nativeapp_version_create_and_drop(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        result_create = runner.invoke_with_connection_json(
            ["app", "version", "create", "v1", "--force", "--skip-git-check"],
            env=TEST_ENV,
        )
        assert result_create.exit_code == 0

        try:
            # package exist
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show application packages like '{package_name}'",
                    )
                ),
                dict(name=package_name),
            )

            # app package contains version v1
            expect = snowflake_session.execute_string(
                f"show versions in application package {package_name}"
            )
            actual = runner.invoke_with_connection_json(
                ["app", "version", "list"], env=TEST_ENV
            )
            assert actual.json == row_from_snowflake_session(expect)

            result_drop = runner.invoke_with_connection_json(
                ["app", "version", "drop", "v1", "--force"],
                env=TEST_ENV,
            )
            assert result_drop.exit_code == 0
            actual = runner.invoke_with_connection_json(
                ["app", "version", "list"], env=TEST_ENV
            )
            assert len(actual.json) == 0

            # make sure we always delete the package
            result = runner.invoke_with_connection_json(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

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


# Tests upgrading an app from an existing loose files installation to versioned installation.
@pytest.mark.integration
@pytest.mark.parametrize("project_definition_files", ["integration"], indirect=True)
def test_nativeapp_upgrade(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    with pushd(project_dir):
        runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        runner.invoke_with_connection_json(
            ["app", "version", "create", "v1", "--force", "--skip-git-check"],
            env=TEST_ENV,
        )

        try:
            # package exist
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{USER_NAME}".upper()
            # app package contains version v1
            expect = snowflake_session.execute_string(
                f"show versions in application package {package_name}"
            )
            actual = runner.invoke_with_connection_json(
                ["app", "version", "list"], env=TEST_ENV
            )
            assert actual.json == row_from_snowflake_session(expect)

            runner.invoke_with_connection_json(
                ["app", "run", "--version", "v1", "--force"], env=TEST_ENV
            )

            expect = row_from_snowflake_session(
                snowflake_session.execute_string(f"desc application {app_name}")
            )
            assert contains_row_with(expect, {"property": "name", "value": app_name})
            assert contains_row_with(expect, {"property": "version", "value": "V1"})
            assert contains_row_with(expect, {"property": "patch", "value": "0"})

            runner.invoke_with_connection_json(
                ["app", "version", "drop", "v1", "--force"],
                env=TEST_ENV,
            )

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
