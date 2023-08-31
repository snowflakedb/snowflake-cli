import os
import tempfile
from zipfile import ZipFile

import pytest
from pathlib import Path
from tempfile import NamedTemporaryFile

from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.test_utils import contains_row_with, row_from_snowflake_session
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful_and_output_json_contains,
    assert_that_result_is_successful,
)


class TestPackage:
    STAGE_NAME = "PACKAGE_TEST"

    @pytest.mark.integration
    def test_package_upload(
        self, runner, example_file, snowflake_session, test_database
    ):

        result = runner.invoke_integration(
            [
                "snowpark",
                "package",
                "upload",
                "-f",
                f"{example_file}",
                "-s",
                f"{self.STAGE_NAME}",
            ]
        )
        assert result.exit_code == 0

        expect = snowflake_session.execute_string(f"LIST @{self.STAGE_NAME}")

        assert contains_row_with(
            row_from_snowflake_session(expect),
            {"name": f"{self.STAGE_NAME.lower()}/{example_file.name}"},
        )

        snowflake_session.execute_string(f"DROP STAGE IF EXISTS {self.STAGE_NAME};")

    @pytest.mark.integration
    def test_package_create_with_non_anaconda_package(self, directory_for_test, runner):
        result = runner.invoke_integration(
            ["snowpark", "package", "create", "PyRTF3", "-y"]
        )

        assert result.exit_code == 0
        assert os.path.isfile("PyRTF3.zip")

        zip_file = ZipFile("PyRTF3.zip", "r")

        assert ".packages/PyRTF/utils.py" in zip_file.namelist()

    @pytest.mark.integration
    def test_package_create_with_non_anaconda_package_without_install(
        self, directory_for_test, runner
    ):
        result = runner.invoke_integration(["snowpark", "package", "create", "PyRTF3"])

        assert_that_result_is_successful(result)
        assert result.json == [
            {
                "result": "Lookup for package PyRTF3 resulted in some error. Please check the package name or try again with -y option"
            }
        ]
        assert not os.path.exists("PyRTF3.zip")

    @pytest.fixture
    def example_file(self):
        file = NamedTemporaryFile("r", suffix=".py")
        yield Path(file.name)
        os.remove(file.name)

    @pytest.fixture(scope="function")
    def directory_for_test(self):
        init_dir = os.getcwd()

        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            yield tmp
            os.chdir(init_dir)
