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
def test_nativeapp_validate(runner, snowflake_session):
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_name)):
        result = runner.invoke_with_connection_json(
            ["app", "deploy"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # validate the app's setup script
            result = runner.invoke_with_connection_json(
                ["app", "validate"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert "Native App validation succeeded." in result.output

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
