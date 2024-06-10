import os
import uuid

from snowflake.cli.api.project.util import generate_user_env
from tests.project.fixtures import *
from tests_integration.test_utils import (
    pushd,
)

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


@pytest.mark.integration
def test_nativeapp_validate(runner, temporary_working_directory):
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0, result.output

    with pushd(Path(os.getcwd(), project_name)):
        try:
            # validate the app's setup script
            result = runner.invoke_with_connection(
                ["app", "validate"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0, result.output
            assert "Native App validation succeeded." in result.output
        finally:
            result = runner.invoke_with_connection(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0, result.output


@pytest.mark.integration
def test_nativeapp_validate_failing(runner, temporary_working_directory):
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0, result.output

    with pushd(Path(os.getcwd(), project_name)):
        # Create invalid SQL file
        Path("app/setup_script.sql").write_text("Lorem ipsum dolor sit amet")

        try:
            # validate the app's setup script, this will fail
            # because we include an empty file
            result = runner.invoke_with_connection(
                ["app", "validate"],
                env=TEST_ENV,
            )
            assert result.exit_code == 1, result.output
            assert (
                "Snowflake Native App setup script failed validation." in result.output
            )
            assert "syntax error" in result.output
        finally:
            result = runner.invoke_with_connection(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0, result.output
