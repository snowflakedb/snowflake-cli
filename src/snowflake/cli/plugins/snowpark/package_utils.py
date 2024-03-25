from __future__ import annotations

import logging
import os
from typing import List

import click
import requirements
from click import ClickException
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.models import (
    Requirement,
    RequirementWithFilesAndDeps,
    RequirementWithWheelAndDeps,
    SplitRequirements,
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
    reqs: List[Requirement] = []
    if requirements_file.exists():
        with requirements_file.open(
            "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"
        ) as f:
            for req in requirements.parse(f):
                reqs.append(req)
    else:
        log.info("No %s found", requirements_file.path)

    return deduplicate_and_sort_reqs(reqs)


def deduplicate_and_sort_reqs(
    packages: List[Requirement],
) -> List[Requirement]:
    """
    Deduplicates a list of requirements, keeping the first occurrence of each package.
    """
    seen = set()
    deduped: List[RequirementWithFilesAndDeps] = []
    for package in packages:
        if package.name not in seen:
            deduped.append(package)
            seen.add(package.name)
    # sort by package name
    deduped.sort(key=lambda x: x.name)
    return deduped


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


def download_packages(
    anaconda: AnacondaChannel | None,
    packages_dir: SecurePath,
    ignore_anaconda: bool = False,
    requirements: List[Requirement] | None = None,
    index_url: str | None = None,
    allow_shared_libraries: YesNoAsk = YesNoAsk.ASK,
    skip_version_check: bool = False,
) -> tuple[bool, SplitRequirements | None]:
    """
    Downloads all packages described in [requirements] list into [packages_dir] directory.
    If [perform_anaconda_check_for_dependencies] is set to True, dependencies available in Snowflake Anaconda
    channel will be omitted, otherwise all packages will be downloaded using pip.

    Returns a tuple of:
    1) a boolean indicating whether the download was successful
    2) a SplitRequirements object containing any installed dependencies
        which are available on the Snowflake Anaconda channel.
        They will not be downloaded into '.packages' directory.
    """
    if anaconda is None and not ignore_anaconda:
        raise ClickException(
            "Cannot perform anaconda checks if anaconda channel is not specified."
        )

    with Venv() as v, SecurePath.temporary_directory() as downloads_dir:
        # This is a Windows workaround where use TemporaryDirectory instead of NamedTemporaryFile
        requirements_file = SecurePath(v.directory.name) / "requirements.txt"
        _write_requirements_file(requirements_file, requirements)  # type: ignore

        pip_wheel_result = v.pip_wheel(
            requirements_file.path, download_dir=downloads_dir.path, index_url=index_url  # type: ignore
        )
        if pip_wheel_result != 0:
            log.info(pip_failed_log_msg, pip_wheel_result)
            return False, None

        dependencies = v.get_package_dependencies(
            requirements_file, downloads_dir=downloads_dir.path
        )
        dependency_requirements = [d.requirement for d in dependencies]

        log.info(
            "Downloaded packages: %s",
            ", ".join([d.name for d in dependency_requirements]),
        )

        if ignore_anaconda:
            dependencies_to_be_packed = dependencies
            split_dependencies = SplitRequirements([], other=dependency_requirements)
        else:
            log.info("Checking for dependencies available in Anaconda...")
            split_dependencies = anaconda.parse_anaconda_packages(  # type: ignore
                packages=dependency_requirements, skip_version_check=skip_version_check
            )
            _log_dependencies_found_in_conda(split_dependencies.snowflake)
            dependencies_to_be_packed = _filter_dependencies_not_available_in_conda(
                dependencies, split_dependencies.snowflake
            )

        log.info("Checking to see if packages have shared (.so) libraries...")
        if _perform_shared_libraries_check(dependencies_to_be_packed):
            if not _confirm_shared_libraries(allow_shared_libraries):
                return False, split_dependencies

        packages_dir.mkdir(exist_ok=True)
        for package in dependencies_to_be_packed:
            package.extract_files(packages_dir.path)
        return True, split_dependencies


def _filter_dependencies_not_available_in_conda(
    dependencies: List[RequirementWithWheelAndDeps],
    available_in_conda: List[Requirement],
) -> List[RequirementWithWheelAndDeps]:
    in_conda = set(package.name for package in available_in_conda)
    return [dep for dep in dependencies if dep.requirement.name not in in_conda]


def _confirm_shared_libraries(allow_shared_libraries: YesNoAsk) -> bool:
    if allow_shared_libraries == YesNoAsk.ASK:
        return click.confirm("Continue with package installation?", default=False)
    else:
        return allow_shared_libraries == YesNoAsk.YES


def _check_for_shared_libraries(
    dependencies: List[RequirementWithWheelAndDeps],
) -> List[str]:
    return [
        dependency.requirement.name
        for dependency in dependencies
        if any(file.endswith(".so") for file in dependency.namelist())
    ]


def _perform_shared_libraries_check(deps: List[RequirementWithWheelAndDeps]):
    if native_libraries := _check_for_shared_libraries(deps):
        _log_shared_libraries(native_libraries)
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
    # TODO: add "with --allow-shared-libraries" flag when refactoring snowpark build command
    log.error("You may still try to create your package, but it probably won't work")
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


pip_failed_log_msg = (
    "pip failed with return code %d."
    " If pip is installed correctly, this may mean you're trying to install a package"
    " that isn't compatible with the host architecture -"
    " and generally means it has shared (.so) libraries."
)
