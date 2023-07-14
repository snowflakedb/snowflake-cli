
import os

from pathlib import Path

from unittest.mock import MagicMock, patch
from tests_integration.snowflake_connector import snowflake_session
from tests_integration.test_utils import extract_data_from_cursor

import pytest


class TestPackage:
    DATABASE_NAME = "JSIKORSKI_DB"
    SCHEMA_NAME = "TEST_SCHEMA"
    STAGE_NAME = "JSTS_2"  # TODO change this to generated one in the future

    @pytest.mark.integration
    def test_package_upload(self, mock_log, runner, example_file, snowflake_session):
        mock_log = MagicMock()
        runner.invoke(
            [
                "--debug",
                "snowpark",
                "package",
                "upload",
                "-f",
                f"{example_file.name}",
                "-s",
                f"{self.STAGE_NAME}",
            ]
        )

        result = snowflake_session.execute_string(
            f"USE DATABASE {self.DATABASE_NAME}; USE SCHEMA {self.SCHEMA_NAME}; LIST @{self.STAGE_NAME}"
        )

        assert f"{self.STAGE_NAME.lower()}/{example_file.name}" in map(
            lambda x: x["name"], extract_data_from_cursor(result[-1])
        )

        snowflake_session.execute_string(f"DROP STAGE IF EXISTS {self.STAGE_NAME};")


    @pytest.fixture
    def example_file(self):
        file = open("example.py", "a")
        yield Path(file.name)
        os.remove(file.name)
