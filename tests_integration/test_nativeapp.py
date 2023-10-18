import pytest
import os
import uuid
from pathlib import Path
from contextlib import contextmanager

from snowcli.cli.project.util import generate_user_env
from tests.project.fixtures import *
from tests.testing_utils.fixtures import temp_dir
from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.test_utils import (
    row_from_snowflake_session,
    contains_row_with,
)


USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@contextmanager
def pushd(dir: Path):
    cwd = os.getcwd()
    os.chdir(dir)
    try:
        yield dir
    finally:
        os.chdir(cwd)


@pytest.mark.integration
def test_nativeapp_init_run_without_modifications(
    runner,
    snowflake_session,
    temp_dir,
):
    project_name = "myapp"
    result = runner.invoke_with_config(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_name)):
        result = runner.invoke_integration(
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

        finally:
            # make sure we always delete the app
            result = runner.invoke_integration(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0


@pytest.mark.integration
@pytest.mark.parametrize("project_definition_files", ["integration"], indirect=True)
def test_nativeapp_run_existing(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    dir = project_definition_files[0].parent
    with pushd(dir):
        result = runner.invoke_integration(
            ["app", "run"],
            env=TEST_ENV,
        )
        print(result.output.encode("utf-8") if result.output else None)
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
        finally:
            # make sure we always delete the app
            result = runner.invoke_integration(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
