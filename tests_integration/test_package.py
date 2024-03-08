import os
import sys
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
            ["snowpark", "package", "create", "dummy-pkg-for-tests-with-deps", "-y"]
        )

        assert result.exit_code == 0
        assert Path("dummy-pkg-for-tests-with-deps.zip").is_file()
        assert "dummy_pkg_for_tests/shrubbery.py" in self._get_filenames_from_zip(
            "dummy-pkg-for-tests-with-deps.zip"
        )
        assert (
            "dummy_pkg_for_tests_with_deps/shrubbery.py"
            in self._get_filenames_from_zip("dummy-pkg-for-tests-with-deps.zip")
        )

    @pytest.mark.integration
    def test_package_create_with_non_anaconda_package_without_install(
        self, directory_for_test, runner, snapshot
    ):
        result = runner.invoke_with_connection_json(
            ["snowpark", "package", "create", "dummy_pkg_for_tests_with_deps"]
        )

        assert_that_result_is_successful(result)
        assert result.json == snapshot
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
        assert any(["shrubbery.py" in file for file in files])

    @pytest.mark.integration
    def test_package_with_conda_dependencies(
        self, directory_for_test, runner
    ):  # TODO think how to make this test with packages controlled by us
        # test case is: We have a non-conda package, that has a dependency present on conda
        # but not in latest version - here the case is matplotlib.
        result = runner.invoke_with_connection_json(
            [
                "snowpark",
                "package",
                "create",
                "july",
                "--pypi-download",
                "--allow-native-libraries",
                "yes",
            ]
        )

        assert result.exit_code == 0
        assert Path("july.zip").exists()

        files = self._get_filenames_from_zip("july.zip")
        assert any(["colormaps.py" in name for name in files])
        assert not any(["matplotlib" in name for name in files])

    @pytest.mark.integration
    def test_package_from_github(self, directory_for_test, runner):
        result = runner.invoke_with_connection_json(
            [
                "snowpark",
                "package",
                "create",
                "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests-with-deps.git",
                "-y",
            ]
        )

        assert result.exit_code == 0
        assert Path("dummy-pkg-for-tests-with-deps.zip").exists()

        files = self._get_filenames_from_zip("dummy-pkg-for-tests-with-deps.zip")

        assert any(
            ["dummy_pkg_for_tests_with_deps-1.0.dist-info" in file for file in files]
        )
        assert any(["dummy_pkg_for_tests-1.0.dist-info" in file for file in files])

    @pytest.mark.integration
    @pytest.mark.skipif(
        sys.platform.startswith("win"),
        reason="Windows version of PyGame has no native libraries",
    )
    def test_package_with_native_libraries(self, directory_for_test, runner):
        result = runner.invoke_with_connection(
            [
                "snowpark",
                "package",
                "create",
                "pygame",
                "-y",
            ]
        )

        assert result.exit_code == 0
        assert "at https://support.anaconda.com/" in result.output

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
