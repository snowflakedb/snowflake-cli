import uuid

from snowflake.cli.api.project.util import generate_user_env

from tests.project.fixtures import *
from tests_integration.test_utils import (
    pushd,
    contains_row_with,
    row_from_snowflake_session,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
DEFAULT_TEST_ENV = generate_user_env(USER_NAME)

# Tests a simple flow of native app with template reading env variables from OS
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration_templated"], indirect=True
)
def test_nativeapp_pdf_templating_use_env_from_os(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    test_ci_env = "prod"
    local_test_env = dict(DEFAULT_TEST_ENV)
    local_test_env["CI_ENV"] = test_ci_env
    local_test_env["APP_DIR"] = "app"

    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
            package_name = f"{project_name}_{test_ci_env}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{test_ci_env}_{USER_NAME}".upper()
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
                env=local_test_env,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=local_test_env,
            )
            assert result.exit_code == 0


# Tests a simple flow of native app with template reading default env values from project definition file
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files", ["integration_templated"], indirect=True
)
def test_nativeapp_pdf_templating_use_default_env_from_pdf(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    default_ci_env = "dev"
    local_test_env = dict(DEFAULT_TEST_ENV)
    local_test_env["APP_DIR"] = "app"

    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
            package_name = f"{project_name}_{default_ci_env}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{default_ci_env}_{USER_NAME}".upper()
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
                env=local_test_env,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=local_test_env,
            )
            assert result.exit_code == 0


# Tests a simple flow of native app with template containing variable referencing another var through another var
@pytest.mark.integration
@pytest.mark.parametrize(
    "project_definition_files",
    ["integration_templated_multi_indirection"],
    indirect=True,
)
def test_nativeapp_pdf_templating_chain_of_templating(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent

    test_ci_env = "prod"
    local_test_env = dict(DEFAULT_TEST_ENV)
    local_test_env["CI_ENV"] = test_ci_env

    with pushd(project_dir):
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=local_test_env,
        )
        assert result.exit_code == 0

        try:
            # app + package exist
            package_name = f"{project_name}_{test_ci_env}_pkg_{USER_NAME}".upper()
            app_name = f"{project_name}_{test_ci_env}_{USER_NAME}".upper()
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
                env=local_test_env,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=local_test_env,
            )
            assert result.exit_code == 0
