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
