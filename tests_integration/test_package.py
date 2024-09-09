# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import tempfile
from pathlib import Path
from typing import List
from zipfile import ZipFile

import pytest


from tests_integration.test_utils import contains_row_with, row_from_snowflake_session


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
    @pytest.mark.parametrize(
        "extra_flags",
        [
            [],
            ["--ignore-anaconda"],
            ["--index-url", "https://pypi.org/simple"],
            ["--skip-version-check"],
        ],
    )
    def test_package_create_with_non_anaconda_package(
        self, directory_for_test, runner, extra_flags, caplog
    ):
        result = runner.invoke_with_connection_json(
            [
                "snowpark",
                "package",
                "create",
                "dummy-pkg-for-tests-with-deps",
            ]
            + extra_flags
        )

        assert result.exit_code == 0
        assert Path("dummy_pkg_for_tests_with_deps.zip").is_file()
        assert "dummy_pkg_for_tests/shrubbery.py" in self._get_filenames_from_zip(
            "dummy_pkg_for_tests_with_deps.zip"
        )
        assert (
            "dummy_pkg_for_tests_with_deps/shrubbery.py"
            in self._get_filenames_from_zip("dummy_pkg_for_tests_with_deps.zip")
        )

        def _assert_message_in_logs(expected_message):
            return any(expected_message in record.message for record in caplog.records)

        _assert_message_in_logs("Running pip wheel with command: %s")
        _assert_message_in_logs("Pip wheel command executed successfully")

    @pytest.mark.integration
    @pytest.mark.parametrize("ignore_anaconda", (True, False))
    def test_create_package_with_deps(
        self, directory_for_test, runner, ignore_anaconda
    ):
        command = [
            "snowpark",
            "package",
            "create",
            "dummy_pkg_for_tests_with_deps",
        ]
        if ignore_anaconda:
            command.append("--ignore-anaconda")
        result = runner.invoke_with_connection_json(command)

        assert result.exit_code == 0
        assert (
            "Package dummy_pkg_for_tests_with_deps.zip created. You can now upload it to a stage"
            in result.json["message"]
        )

        files = self._get_filenames_from_zip("dummy_pkg_for_tests_with_deps.zip")
        assert any(["shrubbery.py" in file for file in files])

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "flags",
        [
            ["--allow-shared-libraries"],
            ["--allow-shared-libraries", "--ignore-anaconda"],
        ],
    )
    def test_package_with_conda_dependencies(
        self, directory_for_test, runner, flags
    ):  # TODO think how to make this test with packages controlled by us
        # test case is: We have a non-conda package, that has a dependency present on conda
        # but not in latest version - here the case is matplotlib.
        result = runner.invoke_with_connection(
            ["snowpark", "package", "create", "july", *flags]
        )

        assert result.exit_code == 0
        assert Path("july.zip").exists(), result.output

        files = self._get_filenames_from_zip("july.zip")
        assert any(["colormaps.py" in name for name in files])
        assert any(["matplotlib" in name for name in files]) == (
            "--ignore-anaconda" in flags
        )

    @pytest.mark.integration
    def test_package_create_skip_version_check(self, directory_for_test, runner):
        # test case: package is available in Anaconda, but not in required version
        result = runner.invoke_with_connection(
            [
                "snowpark",
                "package",
                "create",
                "matplotlib>=1000",
                "--skip-version-check",
            ]
        )
        assert result.exit_code == 0, result.output
        assert (
            "Package matplotlib>=1000 is already available in Snowflake Anaconda Channel."
            in result.output
        )

        # test case: all dependencies are available in Anaconda, but not in their latest version
        result = runner.invoke_with_connection(
            [
                "snowpark",
                "package",
                "create",
                "july",
                "--skip-version-check",
            ]
        )
        assert result.exit_code == 0, result.output
        assert Path("july.zip").exists()
        files = self._get_filenames_from_zip("july.zip")
        # july is not available on anaconda
        assert all([name.startswith("july") for name in files])

    @pytest.mark.integration
    def test_package_from_github(self, directory_for_test, runner):
        result = runner.invoke_with_connection_json(
            [
                "snowpark",
                "package",
                "create",
                "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests-with-deps.git",
            ]
        )

        assert result.exit_code == 0
        assert Path("dummy_pkg_for_tests_with_deps.zip").exists()

        files = self._get_filenames_from_zip("dummy_pkg_for_tests_with_deps.zip")

        assert any(
            ["dummy_pkg_for_tests_with_deps-1.0.dist-info" in file for file in files]
        )
        assert any(["dummy_pkg_for_tests-1.0.dist-info" in file for file in files])

    @pytest.mark.integration
    def test_package_with_native_libraries(self, directory_for_test, runner):
        result = runner.invoke(
            ["snowpark", "package", "create", "numpy", "--ignore-anaconda"]
        )
        assert result.exit_code == 1
        assert "at https://support.anaconda.com/" in result.output

    @pytest.mark.integration
    def test_incorrect_input(self, runner):
        from packaging.requirements import InvalidRequirement

        with pytest.raises(InvalidRequirement) as err:
            runner.invoke_with_connection(
                ["snowpark", "package", "create", "this is incorrect"]
            )
        assert (
            "Expected end or semicolon (after name and no valid version specifier)"
            in str(err)
        )

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
