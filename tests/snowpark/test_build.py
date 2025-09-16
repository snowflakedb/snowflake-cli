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
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest
from snowflake.cli._plugins.snowpark.models import (
    Requirement,
    WheelMetadata,
)
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AvailablePackage,
)
from snowflake.cli._plugins.snowpark.package_utils import (
    DownloadUnavailablePackagesResult,
    split_downloaded_dependencies,
)
from snowflake.cli.api.secure_path import SecurePath


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


@patch("snowflake.cli._plugins.snowpark.package_utils.log")
def test_split_downloaded_dependencies_handles_duplicates(mock_log, tmp_path):
    """Test that split_downloaded_dependencies properly handles duplicate package versions.

    This test prevents regression of the bug where multiple versions of the same package
    (e.g., httpx-0.27.0.whl and httpx-0.28.1.whl) would both be included in dependencies.zip,
    causing Snowflake deployment to fail with 'Package specified with multiple versions'.
    """
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir()

    httpx_v1_wheel = downloads_dir / "httpx-0.27.0-py3-none-any.whl"
    httpx_v2_wheel = downloads_dir / "httpx-0.28.1-py3-none-any.whl"
    httpx_v1_wheel.touch()
    httpx_v2_wheel.touch()

    requirements_file = tmp_path / "requirements.txt"
    requirements_file.write_text("httpx\n")

    original_from_wheel = WheelMetadata.from_wheel

    def mock_from_wheel(wheel_path):
        if "httpx-0.27.0" in str(wheel_path):
            return WheelMetadata(name="httpx", wheel_path=wheel_path, dependencies=[])
        elif "httpx-0.28.1" in str(wheel_path):
            return WheelMetadata(name="httpx", wheel_path=wheel_path, dependencies=[])
        return original_from_wheel(wheel_path)

    with patch.object(WheelMetadata, "from_wheel", side_effect=mock_from_wheel):
        mock_anaconda = MagicMock(spec=AnacondaPackages)
        mock_anaconda.is_package_available.return_value = False

        result = split_downloaded_dependencies(
            requirements_file=SecurePath(requirements_file),
            downloads_dir=downloads_dir,
            anaconda_packages=mock_anaconda,
            skip_version_check=False,
        )

        # Verify that 2 warnings were logged about duplicate packages
        assert mock_log.warning.call_count >= 2

        # Check the first warning call (multiple versions found)
        first_call = mock_log.warning.call_args_list[0]
        assert "Multiple versions of package '%s' found" in first_call.args[0]
        assert first_call.args[1] == "httpx"  # package name
        assert "httpx-" in first_call.args[2]  # using wheel filename
        assert "httpx-" in first_call.args[3]  # ignoring wheel filename

        # Check the second warning call (duplicate packages summary)
        second_call = mock_log.warning.call_args_list[1]
        assert "Found duplicate packages: %s" in second_call.args[0]
        assert second_call.args[1] == "httpx"

        # Verify that only one version of httpx is in the result
        httpx_packages = [
            pkg
            for pkg in result.unavailable_dependencies_wheels
            if pkg.requirement.name == "httpx"
        ]
        assert (
            len(httpx_packages) == 1
        ), f"Expected 1 httpx package, got {len(httpx_packages)}"

        # Verify that one of the duplicate wheel files was removed
        remaining_wheels = list(downloads_dir.glob("httpx-*.whl"))
        assert (
            len(remaining_wheels) == 1
        ), f"Expected 1 remaining wheel file, got {len(remaining_wheels)}"


@patch("snowflake.cli._plugins.snowpark.package.anaconda_packages.log")
def test_write_requirements_file_deduplicates_anaconda_packages(mock_log, tmp_path):
    """Test that write_requirements_file_in_snowflake_format deduplicates packages.

    This test prevents regression of the bug where multiple entries for the same package
    (e.g., 'httpx==0.28.1' and 'httpx>=0.20.0') would both be written to requirements.snowflake.txt,
    causing Snowflake deployment issues.
    """
    packages = {
        "httpx": AvailablePackage(snowflake_name="httpx", versions={"0.28.1", "0.27.0"})
    }

    anaconda_packages = AnacondaPackages(packages)

    requirements = [
        Requirement.parse_line("httpx==0.28.1"),
        Requirement.parse_line("httpx>=0.20.0"),
    ]

    output_file = tmp_path / "requirements.snowflake.txt"

    anaconda_packages.write_requirements_file_in_snowflake_format(
        file_path=SecurePath(output_file), requirements=requirements
    )

    # Verify 2 warnings were logged
    assert mock_log.warning.call_count >= 2

    # Check the first warning call (duplicate package found)
    first_call = mock_log.warning.call_args_list[0]
    assert "Duplicate package '%s' found in Anaconda requirements" in first_call.args[0]
    assert first_call.args[1] == "httpx"  # package name
    assert first_call.args[2] == "httpx>=0.20.0"  # ignored requirement

    # Check the second warning call (duplicate packages summary)
    second_call = mock_log.warning.call_args_list[1]
    assert "Found duplicate Anaconda packages: %s" in second_call.args[0]
    assert second_call.args[1] == "httpx"

    # Verify only one entry was written to the file
    content = output_file.read_text().strip()
    lines = [line.strip() for line in content.split("\n") if line.strip()]

    # Should only have one httpx entry
    httpx_lines = [line for line in lines if "httpx" in line]
    assert (
        len(httpx_lines) == 1
    ), f"Expected 1 httpx line, got {len(httpx_lines)}: {httpx_lines}"
    assert httpx_lines[0] == "httpx==0.28.1"  # Should keep the first one


def test_similar_package_names_not_treated_as_duplicates():
    """Test that packages with similar names are treated as separate packages.

    This test ensures that packages like 'httpx' and 'httpx-retries' are correctly
    treated as different packages and don't trigger duplicate detection.
    """
    req1 = Requirement.parse_line("httpx==0.28.1")
    req2 = Requirement.parse_line("httpx-retries==0.4.2")

    assert req1.name == "httpx"
    assert req2.name == "httpx_retries"  # Note: hyphen becomes underscore
    assert req1.name != req2.name

    wheel1 = "httpx-0.28.1-py3-none-any.whl"
    wheel2 = "httpx_retries-0.4.2-py3-none-any.whl"

    name1 = WheelMetadata._get_name_from_wheel_filename(wheel1)  # noqa: SLF001
    name2 = WheelMetadata._get_name_from_wheel_filename(wheel2)  # noqa: SLF001

    assert name1 == "httpx"
    assert name2 == "httpx_retries"
    assert name1 != name2


@patch("snowflake.cli._plugins.snowpark.package_utils.log")
def test_multiple_different_packages_no_duplicates_detected(mock_log, tmp_path):
    """Test that multiple different packages don't trigger duplicate detection.

    This is a regression test to ensure that legitimate different packages
    (like httpx, httpx-retries, requests, etc.) don't get flagged as duplicates.
    """
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir()

    wheels = [
        "httpx-0.28.1-py3-none-any.whl",
        "httpx_retries-0.4.2-py3-none-any.whl",
        "requests-2.31.0-py3-none-any.whl",
    ]

    for wheel in wheels:
        (downloads_dir / wheel).touch()

    requirements_file = tmp_path / "requirements.txt"
    requirements_file.write_text("httpx\nhttpx-retries\nrequests\n")

    def mock_from_wheel(wheel_path):
        wheel_name = wheel_path.name
        if "httpx-0.28.1" in wheel_name:
            return WheelMetadata(name="httpx", wheel_path=wheel_path, dependencies=[])
        elif "httpx_retries-0.4.2" in wheel_name:
            return WheelMetadata(
                name="httpx_retries", wheel_path=wheel_path, dependencies=[]
            )
        elif "requests-2.31.0" in wheel_name:
            return WheelMetadata(
                name="requests", wheel_path=wheel_path, dependencies=[]
            )
        return None

    with patch.object(WheelMetadata, "from_wheel", side_effect=mock_from_wheel):
        mock_anaconda = MagicMock(spec=AnacondaPackages)
        mock_anaconda.is_package_available.return_value = False

        result = split_downloaded_dependencies(
            requirements_file=SecurePath(requirements_file),
            downloads_dir=downloads_dir,
            anaconda_packages=mock_anaconda,
            skip_version_check=False,
        )

        # Verify NO duplicate warnings were logged
        warning_calls = [str(call) for call in mock_log.warning.call_args_list]
        duplicate_warnings = [
            call
            for call in warning_calls
            if "Multiple versions of package" in call
            or "Found duplicate packages" in call
        ]
        assert (
            len(duplicate_warnings) == 0
        ), f"Unexpected duplicate warnings: {duplicate_warnings}"

        # Verify three packages are in the result
        package_names = {
            pkg.requirement.name for pkg in result.unavailable_dependencies_wheels
        }
        assert "httpx" in package_names
        assert "httpx_retries" in package_names
        assert "requests" in package_names
        assert len(package_names) == 3

        # Verify all wheel files are still present
        remaining_wheels = list(downloads_dir.glob("*.whl"))
        assert len(remaining_wheels) == 3
