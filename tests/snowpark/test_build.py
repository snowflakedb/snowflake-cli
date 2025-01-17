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
from typing import Set
from unittest.mock import patch
from zipfile import ZipFile

import pytest
from snowflake.cli._plugins.snowpark.package_utils import (
    DownloadUnavailablePackagesResult,
)


@patch("snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages")
def test_snowpark_build_no_deprecated_warnings_by_default(
    mock_download, runner, project_directory
):
    mock_download.return_value = DownloadUnavailablePackagesResult()
    with project_directory("snowpark_functions"):
        result = runner.invoke(["snowpark", "build", "--ignore-anaconda"])
        assert result.exit_code == 0, result.output
        assert "flag is deprecated" not in result.output


@pytest.mark.parametrize(
    "artifacts, zip_name, expected_files",
    [
        ("src", "src.zip", {"app.py", "dir/dir_app.py"}),
        ("src/", "src.zip", {"app.py", "dir/dir_app.py"}),
        ("src/*", "src.zip", {"app.py", "dir/dir_app.py"}),
        ("src/*.py", "src.zip", {"app.py"}),
        ("src/**/*.py", "src.zip", {"app.py", "dir/dir_app.py"}),
    ],
)
def test_build_with_glob_patterns_in_artifacts(
    runner,
    enable_snowpark_glob_support_feature_flag,
    project_directory,
    alter_snowflake_yml,
    artifacts,
    zip_name,
    expected_files,
):
    with project_directory("glob_patterns") as tmp_dir:
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml", "entities.hello_procedure.artifacts", [artifacts]
        )

        result = runner.invoke(["snowpark", "build", "--ignore-anaconda"])
        assert result.exit_code == 0, result.output
        _assert_zip_contains(
            tmp_dir / "output" / "bundle" / "snowpark" / zip_name, expected_files
        )


def _assert_zip_contains(app_zip: str, expected_files: Set[str]):
    zip_file = ZipFile(app_zip)
    assert set(zip_file.namelist()) == expected_files
