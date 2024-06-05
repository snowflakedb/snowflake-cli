import os
import os.path
import uuid
from textwrap import dedent

from snowflake.cli.api.project.util import generate_user_env

from tests.nativeapp.utils import assert_dir_snapshot
from tests.project.fixtures import *
from tests_integration.test_utils import (
    contains_row_with,
    row_from_snowflake_session,
    rows_from_snowflake_session,
)
from tests_integration.testing_utils import (
    assert_that_result_failed_with_message_containing,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)

# TODO: add idempotency check after symlink->copy changes are made.
@pytest.mark.integration
def test_full_lifecycle_with_codegen(
    runner, snowflake_session, project_directory, snapshot
):
    project_name = "nativeapp"
    with project_directory(project_name) as project_dir:

        # Run includes bundle
        result = runner.invoke_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert_dir_snapshot(project_dir, snapshot)

        try:
            package_name = f"{project_name}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{USER_NAME}".upper()

            # Sanity Checks
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

            # Enable debug mode to explore app structure
            snowflake_session.execute_string(
                f"ALTER APPLICATION {app_name} SET DEBUG_MODE = TRUE"
            )
            app_name_and_schema = f"{app_name}.ext_code_schema"

            curr = snowflake_session.execute_string(
                f"show schemas in application {app_name}"
            )  # should be 1
            assert len(rows_from_snowflake_session(curr)) == 1
            assert contains_row_with(
                row_from_snowflake_session(curr), {"name": "EXT_CODE_SCHEMA"}
            )

            curr = snowflake_session.execute_string(
                f"show application roles in application {app_name}"
            )  # should be 1
            assert len(rows_from_snowflake_session(curr)) == 1
            assert contains_row_with(
                row_from_snowflake_session(curr), {"name": "APP_INSTANCE_ROLE"}
            )

            curr = snowflake_session.execute_string(
                f"show functions in {app_name_and_schema}"
            )  # should be ?
            assert len(rows_from_snowflake_session(curr)) == 12

            curr = snowflake_session.execute_string(
                f"show procedures in {app_name_and_schema}"
            )  # should be ?
            assert len(rows_from_snowflake_session(curr)) == 1

            # Disable debug mode to call functions and procedures
            snowflake_session.execute_string(
                f"ALTER APPLICATION {app_name} SET DEBUG_MODE = FALSE"
            )

            # Test ext code that user wrote manually
            f"call {app_name_and_schema}.py_echo_proc('test')"
            f"select {app_name_and_schema}.py_echo_fn('test')"

            # User wrote ext code using codegen feature
            f"select {app_name_and_schema}.echo_fn_3('test')"
            f"select {app_name_and_schema}.echo_fn_4('test')"
            f"call {app_name_and_schema}.add_sp(1, 2)"

            # code gen UDAF
            f"select {app_name_and_schema}.sum_int(10)"
            f"select {app_name_and_schema}.sum_int_dec(10)"

            # code gen UDTF
            f"select TABLE({app_name_and_schema}.PrimeSieve(10))"
            f"select TABLE({app_name_and_schema}.alt_int(10))"

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
