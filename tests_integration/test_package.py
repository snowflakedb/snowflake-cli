import os
import tempfile
from pathlib import Path
from typing import List
from zipfile import ZipFile

import pytest

from tests_integration.test_utils import contains_row_with, row_from_snowflake_session
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful,
)


class TestPackage:
    STAGE_NAME = "PACKAGE_TEST"

    @pytest.mark.integration
    def test_package_upload(self, runner, snowflake_session, test_database):
        file_name = "package_upload.py"
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, file_name)
            Path(file_path).touch()

            result = runner.invoke_with_connection_json(
                [
                    "snowpark",
                    "package",
                    "upload",
                    "-f",
                    f"{file_path}",
                    "-s",
                    f"{self.STAGE_NAME}",
                ]
            )
            assert result.exit_code == 0

            expect = snowflake_session.execute_string(f"LIST @{self.STAGE_NAME}")

            assert contains_row_with(
                row_from_snowflake_session(expect),
                {"name": f"{self.STAGE_NAME.lower()}/{file_name}"},
            )

        snowflake_session.execute_string(f"DROP STAGE IF EXISTS {self.STAGE_NAME};")

    @pytest.mark.integration
    def test_package_create_with_non_anaconda_package(self, directory_for_test, runner):
        result = runner.invoke_with_connection_json(
            ["snowpark", "package", "create", "dummy_pkg_for_tests_with_deps", "-y"]
        )

        assert result.exit_code == 0
        assert os.path.isfile("dummy_pkg_for_tests_with_deps.zip")
        assert "dummy_pkg_for_tests/shrubbery.py" in self._get_filenames_from_zip(
            "dummy_pkg_for_tests_with_deps.zip"
        )
        assert (
            "dummy_pkg_for_tests_with_deps/shrubbery.py"
            in self._get_filenames_from_zip("dummy_pkg_for_tests_with_deps.zip")
        )

    @pytest.mark.integration
    def test_package_create_with_non_anaconda_package_without_install(
        self, directory_for_test, runner
    ):
        result = runner.invoke_with_connection_json(
            ["snowpark", "package", "create", "dummy_pkg_for_tests_with_deps"]
        )

        assert_that_result_is_successful(result)
        assert result.json == {
            "message": "Lookup for package dummy_pkg_for_tests_with_deps resulted in some error. Please check the package name or try again with -y option"
        }
        assert not os.path.exists("dummy_pkg_for_tests_with_deps.zip")

    @pytest.mark.integration
    def test_create_package_with_deps(self, directory_for_test, runner):
        result = runner.invoke_with_connection_json(
            ["snowpark", "package", "create", "dummy_pkg_for_tests_with_deps", "-y"]
        )

        assert result.exit_code == 0
        assert (
            "Package dummy_pkg_for_tests_with_deps.zip created. You can now upload it to a stage"
            in result.json["message"]
        )

        files = self._get_filenames_from_zip("dummy_pkg_for_tests_with_deps.zip")
        assert "dummy_pkg_for_tests/shrubbery.py" in files

    @pytest.fixture(scope="function")
    def directory_for_test(self):
        init_dir = os.getcwd()

        with tempfile.TemporaryDirectory() as tmp:
            os.chdir(tmp)
            yield tmp
            os.chdir(init_dir)

    def _get_filenames_from_zip(self, filename: str) -> List[str]:
        zip_file = ZipFile(filename, "r")
        filenames_in_zip = zip_file.namelist()
        zip_file.close()
        return filenames_in_zip
