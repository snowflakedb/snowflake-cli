from snowflake.connector import SnowflakeConnection

from tests_integration.conftest import SnowCLIRunner
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_output_json_contains,
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

    def deploy_image_repository(self, image_repository_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "image-repository",
                "deploy",
            ]
        )
        assert_that_result_is_successful_and_output_json_contains(
            result,
            {
                "message": f"Image repository '{image_repository_name}' successfully deployed."
            },
        )

    def deploy_image_repository_with_replace(self, image_repository_name: str) -> None:
        result = self._setup.runner.invoke_with_connection_json(
            [
                "spcs",
                "image-repository",
                "deploy",
                "--replace",
            ]
        )
        assert_that_result_is_successful_and_output_json_contains(
            result,
            {
                "message": f"Image repository '{image_repository_name}' successfully deployed."
            },
        )
