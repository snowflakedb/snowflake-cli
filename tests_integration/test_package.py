import os
import pytest
from pathlib import Path
from tempfile import NamedTemporaryFile

from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.test_utils import contains_row_with, row_from_snowflake_session


class TestPackage:
    STAGE_NAME = "PACKAGE_TEST"

    @pytest.mark.integration
    def test_package_upload(
        self, runner, example_file, snowflake_session, test_database
    ):
        result = runner.invoke_with_config_and_integration_connection(
            [
                "--debug",
                "snowpark",
                "package",
                "upload",
                "--file",
                f"{example_file}",
                "--stage",
                f"{self.STAGE_NAME}",
            ]
        )

        stage_list = snowflake_session.execute_string(f"LIST @{self.STAGE_NAME}")

        assert result.exit_code == 0
        assert contains_row_with(
            row_from_snowflake_session(stage_list),
            {"name": f"{self.STAGE_NAME.lower()}/{example_file.name}"},
        )

        snowflake_session.execute_string(f"DROP STAGE IF EXISTS {self.STAGE_NAME};")

    @pytest.fixture
    def example_file(self):
        file = NamedTemporaryFile("r", suffix=".py")
        yield Path(file.name)
