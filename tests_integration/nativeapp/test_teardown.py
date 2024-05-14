import os
import uuid
from textwrap import dedent

from snowflake.cli.api.project.util import generate_user_env

from tests.project.fixtures import *
from tests_integration.test_utils import (
    pushd,
    contains_row_with,
    not_contains_row_with,
    row_from_snowflake_session,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@pytest.mark.integration
@pytest.mark.parametrize(
    "command,expected_error",
    [
        # "snow app teardown --cascade" should drop both application and application objects
        ["app teardown --cascade", None],
        # "snow app teardown --force --no-cascade" should attempt to drop the application and fail
        [
            "app teardown --force --no-cascade",
            "Could not successfully execute the Snowflake SQL statements",
        ],
        # "snow app teardown" with owned application objects should abort the teardown
        ["app teardown", "Aborted"],
    ],
)
def test_nativeapp_teardown_cascade(
    command,
    expected_error,
    runner,
    snowflake_session,
    temporary_working_directory,
):
    project_name = "myapp"
    app_name = f"{project_name}_{USER_NAME}".upper()
    db_name = f"{project_name}_db_{USER_NAME}".upper()

    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_name)):
        # Add a procedure to the setup script that creates an app-owned database
        with open("app/setup_script.sql", "a") as file:
            file.write(
                dedent(
                    f"""
                    create or replace procedure core.create_db()
                        returns boolean
                        language sql
                        as $$
                            begin
                                create or replace database {db_name};
                                return true;
                            end;
                        $$;
                    """
                )
            )
        with open("app/manifest.yml", "a") as file:
            file.write(
                dedent(
                    f"""
                    privileges:
                    - CREATE DATABASE:
                        description: "Permission to create databases"
                    """
                )
            )

        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # Grant permission to create databases
            snowflake_session.execute_string(
                f"grant create database on account to application {app_name}",
            )

            # Create the database
            snowflake_session.execute_string("use warehouse xsmall")
            snowflake_session.execute_string(
                f"call {app_name}.core.create_db()",
            )

            # Verify the database is owned by the app
            assert contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(f"show databases like '{db_name}'")
                ),
                dict(name=db_name, owner=app_name),
            )

            # Run the teardown command
            result = runner.invoke_with_connection_json(
                command.split(),
                env=TEST_ENV,
            )
            if expected_error is not None:
                assert result.exit_code == 1
                assert expected_error in result.output
                return

            assert result.exit_code == 0

            # Verify the database is dropped
            assert not_contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(f"show databases like '{db_name}'")
                ),
                dict(name=db_name, owner=app_name),
            )

            # Verify the app is dropped
            assert not_contains_row_with(
                row_from_snowflake_session(
                    snowflake_session.execute_string(
                        f"show applications like '{app_name}'",
                    )
                ),
                dict(name=app_name),
            )

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force", "--cascade"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
