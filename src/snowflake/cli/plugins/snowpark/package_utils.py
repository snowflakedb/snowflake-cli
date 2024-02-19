from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import List

import click
import requests
import requirements
import typer
from packaging.version import parse
from snowflake.cli.plugins.snowpark.models import (
    PypiOption,
    Requirement,
    RequirementType,
    RequirementWithFilesAndDeps,
    SplitRequirements,
    pip_failed_msg,
    second_chance_msg,
)
from snowflake.cli.plugins.snowpark.venv import Venv

log = logging.getLogger(__name__)

ANACONDA_CHANNEL_DATA = "https://repo.anaconda.com/pkgs/snowflake/channeldata.json"

PIP_PATH = os.environ.get("SNOWCLI_PIP_PATH", "pip")


def parse_requirements(
    requirements_file: str = "requirements.txt",
) -> List[Requirement]:
    """Reads and parses a python requirements.txt file.

    Args:
        requirements_file (str, optional): The name of the file.
        Defaults to 'requirements.txt'.

    Returns:
        list[str]: A flat list of package names, without versions
    """
    reqs: List[Requirement] = []
    if os.path.exists(requirements_file):
        with open(requirements_file, encoding="utf-8") as f:
            for req in requirements.parse(f):
                reqs.append(req)
    else:
        log.info("No %s found", requirements_file)

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


def parse_anaconda_packages(packages: List[Requirement]) -> SplitRequirements:
    """
    Checks if a list of packages are available in the Snowflake Anaconda channel.
    Returns a dict with two keys: 'snowflake' and 'other'.
    Each key contains a list of Requirement object.

    Parameters:
        packages (List[Requirement]) - list of requirements to be checked

    Returns:
        result (SplitRequirements) - required packages split to those avaiable in conda, and others, that need to be
                                     installed using pip

    """
    result = SplitRequirements([], [])
    channel_data = _get_anaconda_channel_contents()

    for package in packages:
        # pip package names are case-insensitive, while Anaconda package names are lowercase
        if check_if_package_is_avaiable_in_conda(package, channel_data["packages"]):
            result.snowflake.append(package)
        else:
            log.info("'%s' not found in Snowflake anaconda channel...", package.name)
            result.other.append(package)
    return result


def _get_anaconda_channel_contents():
    response = requests.get(ANACONDA_CHANNEL_DATA)
    if response.status_code == 200:
        return response.json()
    else:
        log.error("Error reading Anaconda channel data: %s", response.status_code)
        raise typer.Abort()


def install_packages(
    file_name: str | None,
    perform_anaconda_check: bool = True,
    package_native_libraries: PypiOption = PypiOption.YES,
    package_name: str | None = None,
) -> tuple[bool, SplitRequirements | None]:
    """
    Install packages from a requirements.txt file or a single package name,
    into a local folder named '.packages'.
    If a requirements.txt file is provided, they will be installed using pip.
    If a package name is provided, it will be installed using pip.
    Returns a tuple of:
    1) a boolean indicating whether the installation was successful
    2) a SplitRequirements object containing any installed dependencies
    which are available on the Snowflake anaconda channel. These will have
    been deleted from the local packages folder.
    """
    with Venv() as v:
        if file_name is not None:
            pip_install_result = v.pip_install(file_name, RequirementType.FILE)
            dependencies = v.get_package_dependencies(file_name, RequirementType.FILE)

        if package_name is not None:
            pip_install_result = v.pip_install(package_name, RequirementType.PACKAGE)
            dependencies = v.get_package_dependencies(
                package_name, RequirementType.FILE
            )

        if pip_install_result != 0:
            log.info(pip_failed_msg.format(pip_install_result))
            return False, None

        if perform_anaconda_check:
            log.info("Checking for dependencies available in Anaconda...")
            dependency_requirements = [dep.requirement for dep in dependencies]
            log.info(
                "Downloaded packages: %s",
                ",".join([d.name for d in dependency_requirements]),
            )
            second_chance_results: SplitRequirements = parse_anaconda_packages(
                dependency_requirements
            )

            if len(second_chance_results.snowflake) > 0:
                log.info(second_chance_msg.format(second_chance_results.snowflake))
            else:
                log.info("None of the package dependencies were found on Anaconda")

        dependencies_to_be_packed = _get_dependencies_not_avaiable_in_conda(
            dependencies,
            second_chance_results.snowflake if second_chance_results else None,
        )

        log.info("Checking to see if packages have native libraries...")

        if _perform_native_libraries_check(
            dependencies_to_be_packed, package_native_libraries
        ):
            v.copy_files_to_packages_dir(
                [Path(file) for dep in dependencies_to_be_packed for file in dep.files]
            )
            return True, second_chance_results
        else:
            return False, second_chance_results


def _check_for_native_libraries(dependencies: List[RequirementWithFilesAndDeps]):
    return any(
        [
            file
            for dependency in dependencies
            for file in dependency.files
            if not file.endswith(".so")
        ]
    )


def get_snowflake_packages() -> List[str]:
    if os.path.exists("requirements.snowflake.txt"):
        with open("requirements.snowflake.txt", encoding="utf-8") as f:
            return [req for line in f if (req := line.split("#")[0].strip())]
    else:
        return []


def _get_dependencies_not_avaiable_in_conda(
    dependencies: List[RequirementWithFilesAndDeps],
    avaiable_in_conda: List[Requirement],
) -> List[RequirementWithFilesAndDeps]:
    return [
        dep
        for dep in dependencies
        if dep.requirement.name not in [package.name for package in avaiable_in_conda]
    ]


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


def check_if_package_is_avaiable_in_conda(package: Requirement, packages: dict) -> bool:
    package_name = package.name.lower()
    if package_name not in packages:
        return False
    if package.specs:
        latest_ver = parse(packages[package_name]["version"])
        return all([parse(spec[1]) <= latest_ver for spec in package.specs])
    return True


def _perform_native_libraries_check(
    deps: List[RequirementWithFilesAndDeps], package_native_libraries: PypiOption
):
    if _check_for_native_libraries(deps):
        return (
            click.confirm(
                "\n\nWARNING! Some packages appear to have native libraries!\n"
                "Continue with package installation?",
                default=False,
            )
            if package_native_libraries == PypiOption.ASK
            else True
        )

    else:
        log.info("No non-supported native libraries found in packages (Good news!)...")
        return True


def create_zip_name(name: str) -> str:
    if name.startswith("git+"):
        pattern = r"github\.com\/([^\/]+)\/([^\/.]+)(\.git)?"
        if match := re.search(pattern, name):
            return match.group(2) + ".zip"
        else:
            return "package.zip"

    elif name.endswith(".zip"):
        return name
    else:
        return name + ".zip"
