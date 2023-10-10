from __future__ import annotations

import importlib
import platform
import threading
from dataclasses import dataclass

import glob
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import warnings
import git

from typing import Dict, List, Literal, Optional, Generic, TypeVar, Callable
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

import click
import requests
import requirements
from requirements.requirement import Requirement
import typer
from jinja2 import Environment, FileSystemLoader
from snowflake.connector.cursor import SnowflakeCursor


warnings.filterwarnings("ignore", category=UserWarning)

YesNoAskOptions = ["yes", "no", "ask"]
YesNoAskOptionsType = Literal["yes", "no", "ask"]

PIP_PATH = os.environ.get("SNOWCLI_PIP_PATH", "pip")

templates_path = os.path.join(Path(__file__).parent, "python_templates")

log = logging.getLogger(__name__)

BUFFER_SIZE = 4096


# TODO: add typing to all functions
def yes_no_ask_callback(value: str):
    """
    A typer callback to handle yes/no/ask parameters
    """
    if value not in YesNoAskOptions:
        raise typer.BadParameter(
            f"Valid values: {YesNoAskOptions}. You provided: {value}",
        )
    return value


# create a temporary directory, copy the file_path to it and rename to app.zip


def prepare_app_zip(file_path, temp_dir) -> str:
    # get filename from file path (e.g. app.zip from /path/to/app.zip)
    # TODO: think if no file exceptions are handled correctly
    file_name = os.path.basename(file_path)
    temp_path = temp_dir + "/" + file_name
    shutil.copy(file_path, temp_path)
    return temp_path


@dataclass
class SplitRequirements:
    """A dataclass to hold the results of parsing requirements files and dividing them into
    snowflake-supported vs other packages.
    """

    snowflake: List[Requirement]
    other: List[Requirement]


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
        log.info(f"No {requirements_file} found")

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


# parse JSON from https://repo.anaconda.com/pkgs/snowflake/channeldata.json and
# return a list of packages that exist in packages with the .packages json
# response from https://repo.anaconda.com/pkgs/snowflake/channeldata.json
def parse_anaconda_packages(packages: List[Requirement]) -> SplitRequirements:
    """
    Checks if a list of packages are available in the Snowflake Anaconda channel.
    Returns a dict with two keys: 'snowflake' and 'other'.
    Each key contains a list of Requirement objects.
    """
    url = "https://repo.anaconda.com/pkgs/snowflake/channeldata.json"
    response = requests.get(url)
    snowflake_packages: List[Requirement] = []
    other_packages: List[Requirement] = []
    if response.status_code == 200:
        channel_data = response.json()
        for package in packages:
            # pip package names are case insensitive,
            # Anaconda package names are lowercased
            if package.name.lower() in channel_data["packages"]:
                snowflake_packages.append(package)
            else:
                log.info(
                    f"'{package.name}' not found in Snowflake anaconda channel...",
                )
                if package.name.lower() == "streamlit":
                    # As at April 2023, streamlit appears unavailable in the Snowflake Anaconda channel
                    # but actually works if specified in the environment
                    log.info(
                        f"'{package.name}' is not available in the Snowflake anaconda channel "
                        "but is supported in the environment.yml file",
                    )
                else:
                    other_packages.append(package)
        return SplitRequirements(snowflake=snowflake_packages, other=other_packages)
    else:
        log.error(f"Error reading Anaconda channel data: {response.status_code}")
        raise typer.Abort()


def generate_streamlit_environment_file(
    excluded_anaconda_deps: Optional[List[str]],
    requirements_file: str = "requirements.snowflake.txt",
) -> Optional[Path]:
    """Creates an environment.yml file for streamlit deployment, if a Snowflake
    requirements file exists.
    The file path is returned if it was generated, otherwise None is returned.
    """
    if os.path.exists(requirements_file):
        snowflake_requirements = parse_requirements(requirements_file)

        # remove explicitly excluded anaconda dependencies
        if excluded_anaconda_deps is not None:
            log.info(f"Excluded dependencies: {','.join(excluded_anaconda_deps)}")
            snowflake_requirements = [
                r
                for r in snowflake_requirements
                if r.name not in excluded_anaconda_deps
            ]
        # remove snowflake-connector-python
        requirement_yaml_lines = [
            # unsure if streamlit supports versioned requirements,
            # following PrPr docs convention for now
            f"- {req.name}"
            for req in snowflake_requirements
            if req.name != "snowflake-connector-python"
        ]
        dependencies_list = "\n".join(requirement_yaml_lines)
        environment = Environment(loader=FileSystemLoader(templates_path))
        template = environment.get_template("environment.yml.jinja")
        with open("environment.yml", "w", encoding="utf-8") as f:
            f.write(template.render(dependencies=dependencies_list))
        return Path("environment.yml")
    return None


def generate_streamlit_package_wrapper(
    stage_name: str, main_module: str, extract_zip: bool
) -> Path:
    """Uses a jinja template to generate a streamlit wrapper.
    The wrapper will add app.zip to the path and import the app module.
    """
    environment = Environment(loader=FileSystemLoader(templates_path))
    template = environment.get_template("streamlit_app_launcher.py.jinja")
    target_file = Path("streamlit_app_launcher.py")
    content = template.render(
        {
            "stage_name": stage_name,
            "main_module": main_module,
            "extract_zip": extract_zip,
        }
    )
    with open(target_file, "w", encoding="utf-8") as output_file:
        output_file.write(content)
    return target_file


@dataclass
class RequirementWithFiles:
    """A dataclass to hold a requirement and the path to the
    downloaded files/folders that belong to it"""

    requirement: Requirement
    files: List[str]


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
                            line.split("/")[0].split(",")[0]
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


def generate_snowpark_coverage_wrapper(
    target_file: str,
    proc_name: str,
    proc_signature: str,
    handler_module: str,
    handler_function: str,
    coverage_reports_stage: str,
    coverage_reports_stage_path: str,
):
    """Using a hardcoded template (python_templates/snowpark_coverage.py.jinja), substitutes variables
    and writes out a file.
    The resulting file can be used as the initial handler for the stored proc, and uses the coverage package
    to measure code coverage of the actual stored proc code.
    Afterwards, the handler persists the coverage report to json by executing a query.

    Args:
        target_file (str): _description_
        proc_name (str): _description_
        proc_signature (str): _description_
        handler_module (str): _description_
        handler_function (str): _description_
    """

    environment = Environment(loader=FileSystemLoader(templates_path))
    template = environment.get_template("snowpark_coverage.py.jinja")
    content = template.render(
        {
            "proc_name": proc_name,
            "proc_signature": proc_signature,
            "handler_module": handler_module,
            "handler_function": handler_function,
            "coverage_reports_stage": coverage_reports_stage,
            "coverage_reports_stage_path": coverage_reports_stage_path,
        }
    )
    with open(target_file, "w", encoding="utf-8") as output_file:
        output_file.write(content)


def add_file_to_existing_zip(zip_file: str, other_file: str):
    """Adds another file to an existing zip file

    Args:
        zip_file (str): The existing zip file
        other_file (str): The new file to add
    """
    with ZipFile(zip_file, mode="a") as myzip:
        myzip.write(other_file, os.path.basename(other_file))


def install_packages(
    file_name: str | None,
    perform_anaconda_check: bool = True,
    package_native_libraries: YesNoAskOptionsType = "ask",
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
    pip_install_result = None
    second_chance_results = None
    if file_name is not None:
        try:
            process = subprocess.Popen(
                [PIP_PATH, "install", "-t", ".packages/", "-r", file_name],
                stdout=subprocess.PIPE,
                universal_newlines=True,
            )
            for line in process.stdout:  # type: ignore
                log.info(line.strip())
            process.wait()
            pip_install_result = process.returncode
        except FileNotFoundError:
            log.error(
                "pip not found. Please install pip and try again. "
                "HINT: you can also set the environment variable 'SNOWCLI_PIP_PATH' to the path of pip.",
            )
            return False, None
    if package_name is not None:
        try:
            process = subprocess.Popen(
                [PIP_PATH, "install", "-t", ".packages/", package_name],
                stdout=subprocess.PIPE,
                universal_newlines=True,
            )
            for line in process.stdout:  # type: ignore
                log.info(line.strip())
            process.wait()
            pip_install_result = process.returncode
        except FileNotFoundError:
            log.error(
                "pip not found. Please install pip and try again. "
                "HINT: you can also set the environment variable 'SNOWCLI_PIP_PATH' to the path of pip.",
            )
            return False, None

    if pip_install_result is not None and pip_install_result != 0:
        log.info(
            f"pip failed with return code {pip_install_result}. "
            "This may happen when attempting to install a package "
            "that isn't compatible with the host architecture - "
            "and generally means it has native libraries."
        )
        return False, None
    if perform_anaconda_check:
        log.info("Checking for dependencies available in Anaconda...")
        # it's not over just yet. a non-Anaconda package may have brought in
        # a package available on Anaconda.
        # use each folder's METADATA file to determine its real name
        downloaded_packages_dict = get_downloaded_packages()
        log.info(f"Downloaded packages: {downloaded_packages_dict.keys()}")
        # look for all the downloaded packages on the Anaconda channel
        downloaded_package_requirements = [
            r.requirement for r in downloaded_packages_dict.values()
        ]
        second_chance_results = parse_anaconda_packages(
            downloaded_package_requirements,
        )
        second_chance_snowflake_packages = second_chance_results.snowflake
        if len(second_chance_snowflake_packages) > 0:
            log.info(
                "Good news! The following package dependencies can be "
                "imported directly from Anaconda, and will be excluded from "
                f"the zip: {second_chance_snowflake_packages}"
            )
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
        for package, items in downloaded_packages_not_needed.items():
            log.info(f"Package {package}: deleting {len(items.files)} files")
            for item in items.files:
                item_path = os.path.join(".packages", item)
                if os.path.exists(item_path):
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)

    log.info("Checking to see if packages have native libraries...")
    # use glob to see if any files in packages have a .so extension
    if glob.glob(".packages/**/*.so"):
        for path in glob.glob(".packages/**/*.so"):
            log.info(f"Potential native library: {path}")
        continue_installation = (
            click.confirm(
                "\n\nWARNING! Some packages appear to have native libraries!\n"
                "Continue with package installation?",
                default=False,
            )
            if package_native_libraries == "ask"
            else package_native_libraries == "yes"
        )
        if continue_installation:
            return True, second_chance_results
        else:
            shutil.rmtree(".packages")
            return False, second_chance_results
    else:
        log.info("No non-supported native libraries found in packages (Good news!)...")
        return True, second_chance_results


def recursive_zip_packages_dir(pack_dir: str, dest_zip: str) -> bool:
    files_to_pack = get_list_of_files_to_pack(pack_dir, True)
    add_files_to_zip(dest_zip, files_to_pack)
    return True


def standard_zip_dir(dest_zip: str) -> bool:
    files_to_pack = get_list_of_files_to_pack(None, False)
    add_files_to_zip(dest_zip, files_to_pack)
    return True


def add_files_to_zip(dest_zip: str, files_to_pack: List[File]) -> None:
    with ZipFile(dest_zip, "w", ZIP_DEFLATED, allowZip64=True) as package_zip:
        for file in files_to_pack:
            package_zip.write(
                file.name, arcname=os.path.relpath(file.name, file.relpath)
            )


def get_list_of_files_to_pack(
    pack_dir: Optional[str], is_recursive: bool
) -> List[File]:

    files: List[File] = []

    def filenames_filter(filepath: Path) -> bool:

        return (
            not filepath.name.startswith(".")
            and not filepath.match("*.pyc")
            and not filepath.match("*__pycache__*")
            and filepath not in [file.name for file in files]
        )

    files += [
        File(filepath.absolute(), None)
        if filenames_filter(filepath)
        else File(Path(), None)
        for filepath in Path(".").glob("**/*")
    ]
    for include_dir in os.getenv("SNOWCLI_INCLUDE_PATHS", "").split(":"):
        files += [
            File(filepath.absolute(), include_dir)
            if (os.path.isdir(include_dir) and filenames_filter(filepath))
            else File(Path(), None)
            for filepath in Path(include_dir).glob("**/*")
        ]

    if is_recursive:
        files += [
            File(filepath.absolute(), pack_dir)
            if filenames_filter(filepath)
            else File(Path(), None)
            for filepath in Path(str(pack_dir)).glob("**/*")
        ]

    return list(filter(lambda x: os.path.isfile(x.name), files))


def get_snowflake_packages() -> List[str]:
    if os.path.exists("requirements.snowflake.txt"):
        with open("requirements.snowflake.txt", encoding="utf-8") as f:
            return [req for line in f if (req := line.split("#")[0].strip())]
    else:
        return []


def get_snowflake_packages_delta(anaconda_packages) -> List[str]:
    updated_package_list = []
    if os.path.exists("requirements.snowflake.txt"):
        with open("requirements.snowflake.txt", encoding="utf-8") as f:
            # for each line, check if it exists in anaconda_packages. If it
            # doesn't, add it to the return string
            for line in f:
                if line.strip() not in anaconda_packages:
                    updated_package_list.append(line.strip())
        return updated_package_list
    else:
        return updated_package_list


def convert_resource_details_to_dict(function_details: SnowflakeCursor) -> dict:
    function_dict = {}
    json_properties = ["packages", "installed_packages"]
    for function in function_details:
        if function[0] in json_properties:
            function_dict[function[0]] = json.loads(
                function[1].replace("'", '"'),
            )
        else:
            function_dict[function[0]] = function[1]
    return function_dict


def generate_deploy_stage_name(name: str, input_parameters: str) -> str:
    return name + input_parameters.replace("(", "",).replace(")", "",).replace(
        " ",
        "_",
    ).replace(
        ",",
        "",
    )


@dataclass()
class File:
    name: Path
    relpath: Optional[str] = None


def create_project_template(template_name: str, project_directory: str | None = None):
    target = project_directory or os.getcwd()
    shutil.copytree(
        Path(importlib.util.find_spec("templates").origin).parent / template_name,  # type: ignore
        target,
        dirs_exist_ok=True,
    )


def sql_to_python_return_type_mapper(resource_return_type: str) -> str:
    """
    Some of the python data types get converted to SQL types, when function/procedure is created.
    So, to properly compare types, we use mapping based on:
    https://docs.snowflake.com/en/developer-guide/udf-stored-procedure-data-type-mapping#sql-python-data-type-mappings

    Mind you, this only applies to cases, in which Snowflake accepts python type as return.
    Ie. if function returns list, it has to be declared as 'array' during creation,
    therefore any conversion is not necessary
    """
    mapping = {
        "number(38,0)": "int",
        "timestamp_ntz(9)": "datetime",
        "timestamp_tz(9)": "datetime",
        "varchar(16777216)": "string",
    }

    return mapping.get(resource_return_type.lower(), resource_return_type.lower())


def path_resolver(path_to_file: str):
    if sys.platform == "win32" and "~1" in path_to_file:
        from ctypes import create_unicode_buffer, windll  # type: ignore

        buffer = create_unicode_buffer(BUFFER_SIZE)
        get_long_path_name = windll.kernel32.GetLongPathNameW
        return_value = get_long_path_name(path_to_file, buffer, BUFFER_SIZE)

        if 0 < return_value <= BUFFER_SIZE:
            return buffer.value
    return path_to_file


T = TypeVar("T")


class ThreadsafeValue(Generic[T]):
    def __init__(self, value: T):
        self._value = value
        self._lock = threading.Lock()

    def set(self, new_value: T) -> T:
        return self.transform(lambda _: new_value)

    def transform(self, f: Callable[[T], T]) -> T:
        with self._lock:
            new_value = f(self._value)
            self._value = new_value
            return new_value

    @property
    def value(self) -> T:
        with self._lock:
            return self._value


class ThreadsafeCounter(ThreadsafeValue[int]):
    def increment(self, d=1) -> int:
        return self.transform(lambda v: v + d)

    def decrement(self, d=1) -> int:
        return self.increment(-d)
