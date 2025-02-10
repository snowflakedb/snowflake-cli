from typing import List

from snowflake.connector import SnowflakeConnection

from tests_integration.conftest import SnowCLIRunner
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_output_json_equals,
)


class ImageRepositoryTestSetup:
    def __init__(
        self,
        runner: SnowCLIRunner,
        snowflake_session: SnowflakeConnection,
    ):
        self.runner = runner
        self.snowflake_session = snowflake_session


class ImageRepositoryTestSteps:
    def __init__(self, setup: ImageRepositoryTestSetup):
        self._setup = setup

    def create_from_project_definition(
        self, image_repository_name: str, additional_flags: List[str] = []
    ) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "image-repository",
                "deploy",
                *additional_flags,
            ]
        )
        assert_that_result_is_successful_and_output_json_equals(
            result,
            {
                "status": f"Image Repository {image_repository_name.upper()} successfully created."
            },
        )
