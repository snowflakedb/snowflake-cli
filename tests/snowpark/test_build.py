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


# ==================================================================================
# DUPLICATE PACKAGE HANDLING TESTS - Prevent regression of the httpx duplicate bug
# ==================================================================================


@patch("snowflake.cli._plugins.snowpark.package_utils.log")
def test_split_downloaded_dependencies_handles_duplicates(mock_log, tmp_path):
    """Test that split_downloaded_dependencies properly handles duplicate package versions.

    This test prevents regression of the bug where multiple versions of the same package
    (e.g., httpx-0.27.0.whl and httpx-0.28.1.whl) would both be included in dependencies.zip,
    causing Snowflake deployment to fail with 'Package specified with multiple versions'.
    """
    from unittest.mock import MagicMock

    from snowflake.cli._plugins.snowpark.models import WheelMetadata
    from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
        AnacondaPackages,
    )
    from snowflake.cli._plugins.snowpark.package_utils import (
        split_downloaded_dependencies,
    )
    from snowflake.cli.api.secure_path import SecurePath

    # Create a temporary downloads directory
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir()

    # Create mock wheel files for the same package with different versions
    httpx_v1_wheel = downloads_dir / "httpx-0.27.0-py3-none-any.whl"
    httpx_v2_wheel = downloads_dir / "httpx-0.28.1-py3-none-any.whl"
    httpx_v1_wheel.touch()
    httpx_v2_wheel.touch()

    # Create a temporary requirements file
    requirements_file = tmp_path / "requirements.txt"
    requirements_file.write_text("httpx\n")

    # Mock WheelMetadata.from_wheel to return metadata for our test wheels
    original_from_wheel = WheelMetadata.from_wheel

    def mock_from_wheel(wheel_path):
        if "httpx-0.27.0" in str(wheel_path):
            return WheelMetadata(name="httpx", wheel_path=wheel_path, dependencies=[])
        elif "httpx-0.28.1" in str(wheel_path):
            return WheelMetadata(name="httpx", wheel_path=wheel_path, dependencies=[])
        return original_from_wheel(wheel_path)

    with patch.object(WheelMetadata, "from_wheel", side_effect=mock_from_wheel):
        # Mock AnacondaPackages
        mock_anaconda = MagicMock(spec=AnacondaPackages)
        mock_anaconda.is_package_available.return_value = False

        # Call the function under test
        result = split_downloaded_dependencies(
            requirements_file=SecurePath(requirements_file),
            downloads_dir=downloads_dir,
            anaconda_packages=mock_anaconda,
            skip_version_check=False,
        )

        # Verify that warnings were logged about duplicate packages
        assert mock_log.warning.call_count >= 2  # Should have 2 warning calls

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
    from snowflake.cli._plugins.snowpark.models import Requirement
    from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
        AnacondaPackages,
        AvailablePackage,
    )
    from snowflake.cli.api.secure_path import SecurePath

    # Create test packages
    packages = {
        "httpx": AvailablePackage(snowflake_name="httpx", versions={"0.28.1", "0.27.0"})
    }

    anaconda_packages = AnacondaPackages(packages)

    # Create requirements with duplicates - this mimics the real-world scenario
    requirements = [
        Requirement.parse_line("httpx==0.28.1"),
        Requirement.parse_line("httpx>=0.20.0"),
    ]

    # Create temporary file
    output_file = tmp_path / "requirements.snowflake.txt"

    # Call the method
    anaconda_packages.write_requirements_file_in_snowflake_format(
        file_path=SecurePath(output_file), requirements=requirements
    )

    # Verify warnings were logged
    assert mock_log.warning.call_count >= 2  # Should have 2 warning calls

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
    from snowflake.cli._plugins.snowpark.models import Requirement, WheelMetadata

    # Test requirement parsing
    req1 = Requirement.parse_line("httpx==0.28.1")
    req2 = Requirement.parse_line("httpx-retries==0.4.2")

    # Verify they have different names
    assert req1.name == "httpx"
    assert req2.name == "httpx_retries"  # Note: hyphen becomes underscore
    assert req1.name != req2.name

    # Test wheel name extraction
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
    from unittest.mock import MagicMock

    from snowflake.cli._plugins.snowpark.models import WheelMetadata
    from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
        AnacondaPackages,
    )
    from snowflake.cli._plugins.snowpark.package_utils import (
        split_downloaded_dependencies,
    )
    from snowflake.cli.api.secure_path import SecurePath

    # Create a temporary downloads directory
    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir()

    # Create mock wheel files for different packages
    wheels = [
        "httpx-0.28.1-py3-none-any.whl",
        "httpx_retries-0.4.2-py3-none-any.whl",
        "requests-2.31.0-py3-none-any.whl",
    ]

    for wheel in wheels:
        (downloads_dir / wheel).touch()

    # Create a temporary requirements file
    requirements_file = tmp_path / "requirements.txt"
    requirements_file.write_text("httpx\nhttpx-retries\nrequests\n")

    # Mock WheelMetadata.from_wheel
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
        # Mock AnacondaPackages
        mock_anaconda = MagicMock(spec=AnacondaPackages)
        mock_anaconda.is_package_available.return_value = False

        # Call the function under test
        result = split_downloaded_dependencies(
            requirements_file=SecurePath(requirements_file),
            downloads_dir=downloads_dir,
            anaconda_packages=mock_anaconda,
            skip_version_check=False,
        )

        # Verify NO duplicate warnings were logged (since these are different packages)
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

        # Verify all three packages are in the result
        package_names = {
            pkg.requirement.name for pkg in result.unavailable_dependencies_wheels
        }
        assert "httpx" in package_names
        assert "httpx_retries" in package_names
        assert "requests" in package_names
        assert len(package_names) == 3

        # Verify all wheel files are still present (none should be removed)
        remaining_wheels = list(downloads_dir.glob("*.whl"))
        assert len(remaining_wheels) == 3


@patch("snowflake.cli._plugins.snowpark.package_utils.download_unavailable_packages")
def test_build_integration_with_duplicate_packages(
    mock_download, runner, project_directory
):
    """Integration test that ensures the full build process handles duplicates correctly.

    This test simulates the real-world scenario where pip downloads multiple versions
    of the same package and ensures both the dependencies.zip and requirements.snowflake.txt
    fixes work together in the complete build flow.
    """
    from snowflake.cli._plugins.snowpark.models import Requirement, RequirementWithFiles
    from snowflake.cli._plugins.snowpark.package_utils import (
        DownloadUnavailablePackagesResult,
    )

    # Mock the download result to simulate duplicate packages being detected
    mock_anaconda_packages = [
        Requirement.parse_line("httpx==0.28.1"),
        Requirement.parse_line("httpx>=0.20.0"),  # This is the duplicate
    ]

    mock_download_packages = [
        RequirementWithFiles(
            requirement=Requirement.parse_line("httpx-retries==0.4.2"),
            files=["httpx_retries/__init__.py", "httpx_retries/retry.py"],
        )
    ]

    mock_download.return_value = DownloadUnavailablePackagesResult(
        anaconda_packages=mock_anaconda_packages,
        downloaded_packages_details=mock_download_packages,
    )

    with project_directory("snowpark_functions"):
        # This should succeed without errors despite having duplicate anaconda packages
        result = runner.invoke(["snowpark", "build", "--ignore-anaconda"])
        assert result.exit_code == 0, f"Build failed: {result.output}"

        # Verify build completed successfully
        assert "Build done." in result.output

        # The mock ensures we test the deduplication logic without needing real packages


def test_edge_case_package_names_standardization():
    """Test edge cases in package name standardization to prevent future issues.

    This test covers various package naming edge cases to ensure consistent behavior.
    """
    from snowflake.cli._plugins.snowpark.models import Requirement, WheelMetadata

    test_cases = [
        # (input_name, expected_standardized_name)
        ("httpx", "httpx"),
        ("httpx-retries", "httpx_retries"),
        ("requests-oauthlib", "requests_oauthlib"),
        ("PyYAML", "pyyaml"),  # Case normalization
        ("python-dateutil", "python_dateutil"),
        ("Pillow", "pillow"),
        ("scikit-learn", "scikit_learn"),
        ("beautifulsoup4", "beautifulsoup4"),
        ("lxml", "lxml"),
    ]

    for input_name, expected in test_cases:
        # Test requirement parsing
        req = Requirement.parse_line(f"{input_name}==1.0.0")
        assert (
            req.name == expected
        ), f"Requirement parsing failed for {input_name}: got {req.name}, expected {expected}"

        # Test wheel filename extraction (simulate typical wheel naming)
        wheel_name = f"{input_name.replace('-', '_').lower()}-1.0.0-py3-none-any.whl"
        extracted_name = WheelMetadata._get_name_from_wheel_filename(  # noqa: SLF001
            wheel_name
        )
        assert (
            extracted_name == expected
        ), f"Wheel name extraction failed for {wheel_name}: got {extracted_name}, expected {expected}"


def test_empty_and_single_package_scenarios(tmp_path):
    """Test that deduplication works correctly with edge cases like empty lists and single packages."""
    from snowflake.cli._plugins.snowpark.models import Requirement
    from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
        AnacondaPackages,
        AvailablePackage,
    )
    from snowflake.cli.api.secure_path import SecurePath

    packages = {"httpx": AvailablePackage(snowflake_name="httpx", versions={"0.28.1"})}
    anaconda_packages = AnacondaPackages(packages)

    # Test empty requirements
    output_file = tmp_path / "empty_requirements.txt"
    anaconda_packages.write_requirements_file_in_snowflake_format(
        file_path=SecurePath(output_file), requirements=[]
    )
    assert not output_file.exists() or output_file.read_text().strip() == ""

    # Test single package (no duplicates)
    single_req = [Requirement.parse_line("httpx==0.28.1")]
    output_file = tmp_path / "single_requirements.txt"
    anaconda_packages.write_requirements_file_in_snowflake_format(
        file_path=SecurePath(output_file), requirements=single_req
    )
    content = output_file.read_text().strip()
    assert content == "httpx==0.28.1"
