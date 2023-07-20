import logging
import os
import shutil
from pathlib import Path
from snowcli.cli.snowpark import package
from unittest.mock import MagicMock, patch
from tests_integration.snowflake_connector import snowflake_session, create_database
from tests_integration.test_utils import contains_row_with, row_from_snowflake_session
from tempfile import NamedTemporaryFile

import pytest


class TestPackage:
    STAGE_NAME = "PACKAGE_TEST"

    @pytest.mark.integration
    def test_package_upload(self, runner, example_file, snowflake_session):

        runner.invoke_with_config(
            [
                "--debug",
                "snowpark",
                "package",
                "upload",
                "-f",
                f"{example_file}",
                "-s",
                f"{self.STAGE_NAME}",
            ]
        )

        result = snowflake_session.execute_string(f"LIST @{self.STAGE_NAME}")

        assert contains_row_with(
            row_from_snowflake_session(result),
            {"name": f"{self.STAGE_NAME.lower()}/{example_file.name}"},
        )

        snowflake_session.execute_string(f"DROP STAGE IF EXISTS {self.STAGE_NAME};")

    @pytest.fixture
    def example_file(self):
        file = NamedTemporaryFile("r", suffix=".py")
        yield Path(file.name)
        os.remove(file.name)
