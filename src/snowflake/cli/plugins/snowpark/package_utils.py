from __future__ import annotations

import glob
import logging
import os
import re
import shutil
from typing import Dict, List

import click
import requests
import requirements
import typer
from packaging.version import parse
from requirements.requirement import Requirement
from snowflake.cli.plugins.snowpark.models import (
    PypiOption,
    RequirementWithFiles,
    SplitRequirements, pip_failed_msg, second_chance_msg, RequirementWithFilesAndDeps,
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


def deduplicate_and_sort_reqs(packages: List[Requirement]) -> List[Requirement]:
    """
    Deduplicates a list of requirements, keeping the first occurrence of each package.
    """
    seen = set()
    deduped: List[Requirement] = []
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
    package_native_libraries: PypiOption = PypiOption.ASK,
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
    second_chance_results = None

    with Venv() as v:
        if file_name is not None:
            pip_install_result = v.pip_install(file_name, "file")
            dependencies = v.get_package_dependencies(file_name, "file")

        if package_name is not None:
            pip_install_result = v.pip_install(package_name, "package")
            dependencies = v.get_package_dependencies(package_name, "package")

        if pip_install_result != 0:
            log.info(pip_failed_msg.format(pip_install_result))
            return False, None

        if perform_anaconda_check:
            log.info("Checking for dependencies available in Anaconda...")
            dependency_requirements = [dep.requirement.name for dep in dependencies]
            log.info("Downloaded packages: %s", ",".join(dependency_requirements))
            second_chance_results = parse_anaconda_packages(dependency_requirements)

            if len(second_chance_results.snowflake) > 0:
                log.info(second_chance_msg.format(second_chance_results.snowflake))
            else:
                log.info("None of the package dependencies were found on Anaconda")

        dependencies_to_be_packed = _get_dependencies_not_avaiable_in_conda(dependencies, second_chance_results.snowflake)

        log.info("Checking to see if packages have native libraries...")

        if _check_for_native_libraries(dependencies_to_be_packed):
            continue_installation = (
                click.confirm(
                    "\n\nWARNING! Some packages appear to have native libraries!\n"
                    "Continue with package installation?",
                    default=False,
                )
                if package_native_libraries == PypiOption.ASK
                else True
            )
            print("hello")
            if not continue_installation:
                shutil.rmtree(".packages")
                return False, second_chance_results
        else:
            log.info("No non-supported native libraries found in packages (Good news!)...")
    return True, second_chance_results


def _check_for_native_libraries():
    if glob.glob(".packages/**/*.so"):
        for path in glob.glob(".packages/**/*.so"):
            log.info("Potential native library: %s", path)
        return True
    return False


def get_snowflake_packages() -> List[str]:
    if os.path.exists("requirements.snowflake.txt"):
        with open("requirements.snowflake.txt", encoding="utf-8") as f:
            return [req for line in f if (req := line.split("#")[0].strip())]
    else:
        return []

def _get_dependencies_not_avaiable_in_conda(dependencies: List[RequirementWithFilesAndDeps], avaiable_in_conda: List[Requirement]):
    return [dep for dep in dependencies if dep.requirement.name not in [package.name for package in avaiable_in_conda]]


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


