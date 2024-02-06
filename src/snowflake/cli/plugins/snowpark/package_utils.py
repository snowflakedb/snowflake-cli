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
    SplitRequirements,
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


def get_downloaded_packages() -> Dict[str, RequirementWithFiles]:
    """Returns a dict of official package names mapped to the files/folders
    that belong to it under the .packages directory.

    Returns:
        dict[str:List[str]]: a dict of package folder names to package name
    """
    metadata_files = glob.glob(".packages/*dist-info/METADATA")
    packages_full_path = os.path.abspath(".packages")
    return_dict: Dict[str, RequirementWithFiles] = {}
    for metadata_file in metadata_files:
        parent_folder = os.path.dirname(metadata_file)
        package = get_package_name_from_metadata(metadata_file)
        if package is not None:
            # since we found a package name, we can now look at the RECORD
            # file (a sibling of METADATA) to determine which files and
            # folders that belong to it
            record_file_path = os.path.join(parent_folder, "RECORD")
            if os.path.exists(record_file_path):
                # the RECORD file contains a list of files included in the
                # package, get the unique root folder names and delete them
                # recursively
                with open(record_file_path, encoding="utf-8") as record_file:
                    # we want the part up until the first '/'.
                    # Sometimes it's a file with a trailing ",sha256=abcd....",
                    # so we trim that off too
                    record_entries = list(
                        {
                            line.split(",")[0].rsplit("/", 1)[0]
                            for line in record_file.readlines()
                        },
                    )
                    included_record_entries = []
                    for record_entry in record_entries:
                        record_entry_full_path = os.path.abspath(
                            os.path.join(".packages", record_entry),
                        )
                        # it's possible for the RECORD file to contain relative
                        # paths to items outside of the packages folder.
                        # We'll ignore those by asserting that the full
                        # packages path exists in the full path of each item.
                        if (
                            os.path.exists(record_entry_full_path)
                            and packages_full_path in record_entry_full_path
                        ):
                            included_record_entries.append(record_entry)
                    return_dict[package.name] = RequirementWithFiles(
                        requirement=package, files=included_record_entries
                    )
    return return_dict


def get_package_name_from_metadata(metadata_file_path: str) -> Requirement | None:
    """Loads a METADATA file from the dist-info directory of an installed
    Python package, finds the name of the package.
    This is found on a line containing "Name: my_package".

    Args:
        metadata_file_path (str): The path to the METADATA file

    Returns:
        str: the name of the package.
    """
    with open(metadata_file_path, encoding="utf-8") as metadata_file:
        contents = metadata_file.read()
        results = re.search("^Name: (.*)$", contents, flags=re.MULTILINE)
        if results is None:
            return None
        requirement_line = results.group(1)
        results = re.search("^Version: (.*)$", contents, flags=re.MULTILINE)
        if results is not None:
            version = results.group(1)
            requirement_line += f"=={version}"
        return Requirement.parse(requirement_line)


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

        if package_name is not None:
            pip_install_result = v.pip_install(package_name, "package")

    if pip_install_result != 0:
        log.info(pip_failed_msg.format(pip_install_result))
        return False, None

    if perform_anaconda_check:
        log.info("Checking for dependencies available in Anaconda...")
        # it's not over just yet. a non-Anaconda package may have brought in
        # a package available on Anaconda.
        # use each folder's METADATA file to determine its real name
        downloaded_packages_dict = get_downloaded_packages()
        log.info("Downloaded packages: %s", downloaded_packages_dict.keys())
        # look for all the downloaded packages on the Anaconda channel
        downloaded_package_requirements = [
            r.requirement for r in downloaded_packages_dict.values()
        ]
        second_chance_results = parse_anaconda_packages(
            downloaded_package_requirements,
        )
        second_chance_snowflake_packages = second_chance_results.snowflake
        if len(second_chance_snowflake_packages) > 0:
            log.info(second_chance_msg.format(second_chance_results))
        else:
            log.info("None of the package dependencies were found on Anaconda")
        second_chance_snowflake_package_names = [
            p.name for p in second_chance_snowflake_packages
        ]
        downloaded_packages_not_needed = {
            k: v
            for k, v in downloaded_packages_dict.items()
            if k in second_chance_snowflake_package_names
        }
        _delete_packages(downloaded_packages_not_needed)

    log.info("Checking to see if packages have native libraries...")
    # use glob to see if any files in packages have a .so extension
    if _check_for_native_libraries():
        continue_installation = (
            click.confirm(
                "\n\nWARNING! Some packages appear to have native libraries!\n"
                "Continue with package installation?",
                default=False,
            )
            if package_native_libraries == PypiOption.ASK
            else True
        )
        if not continue_installation:
            shutil.rmtree(".packages")
            return False, second_chance_results
    else:
        log.info("No non-supported native libraries found in packages (Good news!)...")
    return True, second_chance_results


def _delete_packages(to_be_deleted: Dict) -> None:
    for package, items in to_be_deleted.items():
        log.info("Package %s: deleting %d files", package, len(items.files))
        for item in items.files:
            item_path = os.path.join(".packages", item)
            if os.path.exists(item_path):
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)


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


pip_failed_msg = """pip failed with return code {}.
            If pip is installed correctly, this may mean you`re trying to install a package
            that isn't compatible with the host architecture -
            and generally means it has native libraries."""

second_chance_msg = """Good news! The following package dependencies can be
                imported directly from Anaconda, and will be excluded from
                the zip: {}"""
