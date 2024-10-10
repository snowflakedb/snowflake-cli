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

from __future__ import annotations

import dataclasses
import locale
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

from click import ClickException
from snowflake.cli._plugins.snowpark.models import (
    Requirement,
    RequirementWithFiles,
    RequirementWithWheel,
    WheelMetadata,
)
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
)
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

PIP_PATH = os.environ.get("SNOWCLI_PIP_PATH", "pip")


def parse_requirements(
    requirements_file: SecurePath = SecurePath("requirements.txt"),
) -> List[Requirement]:
    """Reads and parses a Python requirements.txt file.

    Args:
        requirements_file (str, optional): The name of the file.
        Defaults to 'requirements.txt'.

    Returns:
        list[Requirement]: A flat list of necessary packages
    """
    reqs = []
    if requirements_file.exists():
        for line in requirements_file.read_text(
            file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB
        ).splitlines():
            line = re.sub(r"\s*#.*", "", line).strip()
            if line:
                reqs.append(Requirement.parse_line(line))
    return reqs


def generate_deploy_stage_name(identifier: str) -> str:
    return (
        identifier.replace("()", "")
        .replace(
            "(",
            "_",
        )
        .replace(
            ")",
            "",
        )
        .replace(
            " ",
            "_",
        )
        .replace(
            ",",
            "",
        )
    )


def get_package_name_from_pip_wheel(package: str, index_url: str | None = None) -> str:
    """Downloads the package using pip and returns the package name.
    If the package name cannot be determined, it returns the [package]."""
    with SecurePath.temporary_directory() as tmp_dir:
        pip_result = pip_wheel(
            package_name=package,
            requirements_file=None,
            download_dir=tmp_dir.path,
            index_url=index_url,
            dependencies=False,
            raise_on_error=False,
        )
        file_list = [
            f.path.name for f in tmp_dir.iterdir() if f.path.name.endswith(".whl")
        ]

        if pip_result != 0 or len(file_list) != 1:
            # cannot determine package name
            return package
        return WheelMetadata.from_wheel((tmp_dir / file_list[0]).path).name


def _write_requirements_file(file_path: SecurePath, requirements: List[Requirement]):
    log.info("Writing %s file", file_path.path)
    with file_path.open("w", encoding="utf-8") as f:
        for req in requirements:
            f.write(f"{req.line}\n")


@dataclasses.dataclass
class DownloadUnavailablePackagesResult:
    error_message: str | None = None
    anaconda_packages: List[Requirement] = dataclasses.field(default_factory=list)
    downloaded_packages_details: List[RequirementWithFiles] = dataclasses.field(
        default_factory=list
    )


def download_unavailable_packages(
    requirements: List[Requirement],
    target_dir: SecurePath,
    # available packages lookup specs
    anaconda_packages: AnacondaPackages,
    skip_version_check: bool = False,
    # pip lookup specs
    pip_index_url: str | None = None,
) -> DownloadUnavailablePackagesResult:
    """Download packages unavailable on Snowflake Anaconda Channel to target directory.

    Returns an object with fields:
    - download_successful - whether packages were successfully downloaded
    - error_message - error message if download was not successful
    - anaconda_packages - list of omitted packages
    - downloaded_packages - list of downloaded packages details
    """
    # pre-check of available packages to avoid potentially heavy downloads
    split_requirements = anaconda_packages.filter_available_packages(
        requirements, skip_version_check=skip_version_check
    )
    packages_in_snowflake = split_requirements.in_snowflake
    requirements = split_requirements.unavailable
    if not requirements:
        # all packages are available in Snowflake
        return DownloadUnavailablePackagesResult(
            anaconda_packages=packages_in_snowflake
        )

    # download all packages with their dependencies
    with SecurePath.temporary_directory() as downloads_dir:
        # This is a Windows workaround where use TemporaryDirectory instead of NamedTemporaryFile
        requirements_file = downloads_dir / "requirements.txt"
        _write_requirements_file(requirements_file, requirements)  # type: ignore
        pip_wheel(
            package_name=None,
            requirements_file=requirements_file.path,  # type: ignore
            download_dir=downloads_dir.path,
            index_url=pip_index_url,
            dependencies=True,
            raise_on_error=True,
        )

        # scan all downloaded packages and filter out ones available on Anaconda
        dependencies = split_downloaded_dependencies(
            requirements_file,
            downloads_dir=downloads_dir.path,
            anaconda_packages=anaconda_packages,
            skip_version_check=skip_version_check,
        )
        log.info(
            "Downloaded packages: %s",
            ", ".join(
                dep.requirement.name
                for dep in dependencies.unavailable_dependencies_wheels
            ),
        )
        _log_dependencies_found_in_conda(dependencies.snowflake_dependencies)
        packages_in_snowflake += dependencies.snowflake_dependencies

        # move filtered packages to target directory
        target_dir.mkdir(exist_ok=True)
        for package in dependencies.unavailable_dependencies_wheels:
            package.extract_files(target_dir.path)
        return DownloadUnavailablePackagesResult(
            anaconda_packages=packages_in_snowflake,
            downloaded_packages_details=[
                RequirementWithFiles(requirement=dep.requirement, files=dep.namelist())
                for dep in dependencies.unavailable_dependencies_wheels
            ],
        )


def pip_wheel(
    requirements_file: Optional[str],
    package_name: Optional[str],
    download_dir: Path,
    index_url: Optional[str],
    dependencies: bool = True,
    raise_on_error: bool = True,
) -> int:
    command = ["-m", "pip", "wheel", "-w", str(download_dir)]
    if package_name:
        command.append(package_name)
    if requirements_file:
        command.extend(["-r", str(requirements_file)])
    if index_url:
        command.extend(["-i", index_url])
    if not dependencies:
        command.append("--no-deps")

    log.info(
        "Running pip wheel with command: %s",
        " ".join([str(com) for com in command]),
    )
    result = subprocess.run(
        ["python", *command],
        capture_output=True,
        text=True,
        encoding=locale.getpreferredencoding(),
    )
    if result.returncode != 0:
        log.info(
            "pip wheel finished with error code %d. Details: %s",
            result.returncode,
            result.stdout + result.stderr,
        )
        if raise_on_error:
            raise ClickException(
                f"pip wheel finished with error code {result.returncode}. Please re-run with --verbose or --debug for more details."
            )
    else:
        log.info("pip wheel command executed successfully")

    return result.returncode


@dataclasses.dataclass
class SplitDownloadedDependenciesResult:
    snowflake_dependencies: List[Requirement] = dataclasses.field(default_factory=list)
    unavailable_dependencies_wheels: List[RequirementWithWheel] = dataclasses.field(
        default_factory=list
    )


def split_downloaded_dependencies(
    requirements_file: SecurePath,
    downloads_dir: Path,
    anaconda_packages: AnacondaPackages,
    skip_version_check: bool,
) -> SplitDownloadedDependenciesResult:
    packages_metadata: Dict[str, WheelMetadata] = {
        meta.name: meta
        for meta in (
            WheelMetadata.from_wheel(wheel_path)
            for wheel_path in downloads_dir.glob("*.whl")
        )
        if meta is not None
    }
    available_in_snowflake_dependencies: Dict = {}
    unavailable_dependencies: Dict = {}

    def _get_dependencies(package: Requirement):
        if (
            package.name not in available_in_snowflake_dependencies
            and package.name not in unavailable_dependencies
        ):
            if anaconda_packages.is_package_available(
                package, skip_version_check=skip_version_check
            ):
                available_in_snowflake_dependencies[package.name] = package
            else:
                meta = packages_metadata.get(
                    WheelMetadata.to_wheel_name_format(package.name)
                )
                wheel_path = meta.wheel_path if meta else None
                requires = meta.dependencies if meta else []
                unavailable_dependencies[package.name] = RequirementWithWheel(
                    requirement=package,
                    wheel_path=wheel_path,
                )

                log.debug(
                    "Checking package %s, with dependencies: %s", package.name, requires
                )

                for package in requires:
                    _get_dependencies(Requirement.parse_line(package))

    with requirements_file.open("r", read_file_limit_mb=512) as req_file:
        for line in req_file:
            _get_dependencies(Requirement.parse_line(line))

    return SplitDownloadedDependenciesResult(
        snowflake_dependencies=list(available_in_snowflake_dependencies.values()),
        unavailable_dependencies_wheels=list(unavailable_dependencies.values()),
    )


def detect_and_log_shared_libraries(dependencies: List[RequirementWithFiles]):
    shared_libraries = [
        dependency.requirement.name
        for dependency in dependencies
        if any(
            file.endswith(".so") or file.endswith(".dll") for file in dependency.files
        )
    ]
    if shared_libraries:
        _log_shared_libraries(shared_libraries)
        return True
    else:
        log.info("Unsupported native libraries not found in packages (Good news!)...")
        return False


def _log_shared_libraries(
    shared_libraries: List[str],
) -> None:
    log.error(
        "Following dependencies utilise shared libraries, not supported by Conda:"
    )
    log.error("\n".join(set(shared_libraries)))
    log.error(
        "You may still try to create your package with --allow-shared-libraries, but the might not work."
    )
    log.error("You may also request adding the package to Snowflake Conda channel")
    log.error("at https://support.anaconda.com/")


def _log_dependencies_found_in_conda(available_dependencies: List[Requirement]) -> None:
    if len(available_dependencies) > 0:
        log.info(
            "Good news! Packages available in Anaconda and excluded from the zip archive: %s",
            format(", ".join(dep.name for dep in available_dependencies)),
        )
    else:
        log.info("None of the package dependencies were found on Anaconda")
