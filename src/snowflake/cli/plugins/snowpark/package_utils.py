from __future__ import annotations

import dataclasses
import logging
import os
from textwrap import dedent
from typing import List

import requirements
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.models import (
    Requirement,
    RequirementWithFiles,
    RequirementWithWheelAndDeps,
    YesNoAsk,
)
from snowflake.cli.plugins.snowpark.package.anaconda import AnacondaChannel
from snowflake.cli.plugins.snowpark.venv import Venv

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
        list[str]: A flat list of package names, without versions
    """
    if not requirements_file.exists():
        return []
    with requirements_file.open(
        "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"
    ) as f:
        return list(requirements.parse(f))


def get_snowflake_packages() -> List[str]:
    requirements_file = SecurePath("requirements.snowflake.txt")
    if requirements_file.exists():
        with requirements_file.open(
            "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"
        ) as f:
            return [req for line in f if (req := line.split("#")[0].strip())]
    else:
        return []


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


def _write_requirements_file(file_path: SecurePath, requirements: List[Requirement]):
    log.info("Writing %s file", file_path.path)
    with file_path.open("w", encoding="utf-8") as f:
        for req in requirements:
            f.write(f"{req.line}\n")


@dataclasses.dataclass
class DownloadUnavailablePackagesResult:
    succeeded: bool
    error_message: str | None = None
    packages_available_in_anaconda: List[Requirement] = dataclasses.field(
        default_factory=list
    )
    downloaded_packages_details: List[RequirementWithFiles] = dataclasses.field(
        default_factory=list
    )


def download_unavailable_packages(
    requirements: List[Requirement],
    target_dir: SecurePath,
    # anaconda lookup specs
    ignore_anaconda: bool = False,
    skip_version_check: bool = False,
    # pip lookup specs
    pip_index_url: str | None = None,
    allow_shared_libraries: YesNoAsk = YesNoAsk.ASK,
) -> DownloadUnavailablePackagesResult:
    """Download packages unavailable on Snowflake Anaconda Channel to target directory.
    If [ignore_anaconda] is set to True, all packages from [requirements] will be downloaded.

    Returns an object with fields:
    - download_successful - whether packages were successfully downloaded
    - error_message - error message if download was not successful
    - packages_available_in_anaconda - list of omitted packages
    - downloaded_packages - list of downloaded packages details
    """
    # pre-check on Anaconda to avoid potentially heavy downloads
    omitted_packages = []
    if not ignore_anaconda:
        anaconda = AnacondaChannel.from_snowflake()
        split_requirements = anaconda.parse_anaconda_packages(
            requirements, skip_version_check=skip_version_check
        )
        omitted_packages = split_requirements.snowflake
        requirements = split_requirements.other
        if not requirements:
            # all packages are available in Anaconda
            return DownloadUnavailablePackagesResult(
                succeeded=True,
                packages_available_in_anaconda=omitted_packages,
            )

    # download all packages with their dependencies
    with Venv() as v, SecurePath.temporary_directory() as downloads_dir:
        # This is a Windows workaround where use TemporaryDirectory instead of NamedTemporaryFile
        requirements_file = SecurePath(v.directory.name) / "requirements.txt"
        _write_requirements_file(requirements_file, requirements)  # type: ignore
        pip_wheel_result = v.pip_wheel(
            requirements_file.path, download_dir=downloads_dir.path, index_url=pip_index_url  # type: ignore
        )
        if pip_wheel_result != 0:
            log.info(_pip_failed_log_msg(pip_wheel_result))
            return DownloadUnavailablePackagesResult(
                succeeded=False,
                error_message=_pip_failed_log_msg(pip_wheel_result),
            )

        # detect all downloaded packages
        dependencies = v.get_package_dependencies(
            requirements_file, downloads_dir=downloads_dir.path
        )
        dependency_requirements = [d.requirement for d in dependencies]
        log.info(
            "Downloaded packages: %s",
            ", ".join([d.name for d in dependency_requirements]),
        )

        # check whether some dependencies are available on Snowflake Anaconda Channel
        if ignore_anaconda:
            dependencies_to_be_packed = dependencies
        else:
            log.info("Checking for dependencies available in Anaconda...")
            split_dependencies = anaconda.parse_anaconda_packages(  # type: ignore
                packages=dependency_requirements, skip_version_check=skip_version_check
            )
            _log_dependencies_found_in_conda(split_dependencies.snowflake)
            omitted_packages += split_dependencies.snowflake
            dependencies_to_be_packed = _filter_dependencies_not_available_in_conda(
                dependencies, split_dependencies.snowflake
            )

        # move filtered packages to target directory
        target_dir.mkdir(exist_ok=True)
        for package in dependencies_to_be_packed:
            package.extract_files(target_dir.path)
        return DownloadUnavailablePackagesResult(
            succeeded=True,
            packages_available_in_anaconda=omitted_packages,
            downloaded_packages_details=[
                RequirementWithFiles(requirement=dep.requirement, files=dep.namelist())
                for dep in dependencies_to_be_packed
            ],
        )


def _filter_dependencies_not_available_in_conda(
    dependencies: List[RequirementWithWheelAndDeps],
    available_in_conda: List[Requirement],
) -> List[RequirementWithWheelAndDeps]:
    in_conda = set(package.name for package in available_in_conda)
    return [dep for dep in dependencies if dep.requirement.name not in in_conda]


def detect_and_log_shared_libraries(dependencies: List[RequirementWithFiles]):
    shared_libraries = [
        dependency.requirement.name
        for dependency in dependencies
        if any(file.endswith(".so") for file in dependency.files)
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


def _pip_failed_log_msg(return_code: int) -> str:
    return dedent(
        f"""
        pip failed with return code {return_code}. Most likely reasons:
         * incorrect package name or version
         * package isn't compatible with host architecture (most probably due to .so libraries)
         * pip is not installed correctly
        """
    )
