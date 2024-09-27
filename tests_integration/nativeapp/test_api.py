import os.path
from shlex import split

from tests.project.fixtures import *

from snowflake.connector import ProgrammingError

from tests_integration.testing_utils import (
    assert_that_result_failed_with_message_containing,
)

# TODO: replace with factory after PR goes in
@pytest.mark.integration
@pytest.mark.parametrize(
    "command,test_project",
    [
        ["app deploy", "napp_v1_invalid_role"],
        ["app run", "napp_v1_invalid_role"],
    ],
)
def test_invalid_role(runner, nativeapp_project_directory, command, test_project):

    with nativeapp_project_directory(test_project):
        result = runner.invoke_with_connection(split(command))
        assert result.exit_code == 1
        assert_that_result_failed_with_message_containing(
            result, "Could not use role non_existent_role."
        )
