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

from unittest.mock import patch

import pytest
from snowflake.cli.plugins.snowpark.package_utils import (
    DownloadUnavailablePackagesResult,
)


@pytest.mark.parametrize(
    "flags",
    [
        ["--pypi-download", "yes"],
        ["--pypi-download", "no"],
        ["--pypi-download", "ask"],
        ["--check-anaconda-for-pypi-deps"],
        ["-a"],
        ["--no-check-anaconda-for-pypi-deps"],
        ["--package-native-libraries", "yes"],
        ["--package-native-libraries", "no"],
        ["--package-native-libraries", "ask"],
    ],
)
@patch("snowflake.cli.plugins.snowpark.package_utils.download_unavailable_packages")
def test_snowpark_build_deprecated_flags_warning(
    mock_download, flags, runner, project_directory
):
    mock_download.return_value = DownloadUnavailablePackagesResult(succeeded=True)
    with project_directory("snowpark_functions"):
        result = runner.invoke(["snowpark", "build", "--ignore-anaconda", *flags])
        assert result.exit_code == 0, result.output
        assert "flag is deprecated" in result.output


@patch("snowflake.cli.plugins.snowpark.package_utils.download_unavailable_packages")
def test_snowpark_build_no_deprecated_warnings_by_default(
    mock_download, runner, project_directory
):
    mock_download.return_value = DownloadUnavailablePackagesResult(succeeded=True)
    with project_directory("snowpark_functions"):
        result = runner.invoke(["snowpark", "build", "--ignore-anaconda"])
        assert result.exit_code == 0, result.output
        assert "flag is deprecated" not in result.output
