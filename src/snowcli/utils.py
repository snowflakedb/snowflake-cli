from __future__ import annotations

import glob
import json
import os
import pathlib
import re
import shutil
import subprocess
import warnings
import zipfile
from pathlib import Path
from typing import List, Literal, Optional

import click
import requests
import requirements
import typer
from jinja2 import Environment, FileSystemLoader
from rich import box, print
from rich.table import Table
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.config import AppConfig

warnings.filterwarnings("ignore", category=UserWarning)

YesNoAskOptions = ["yes", "no", "ask"]
YesNoAskOptionsType = Literal["yes", "no", "ask"]

PIP_PATH = os.environ.get("SNOWCLI_PIP_PATH", "pip")

templates_path = os.path.join(Path(__file__).parent, "python_templates")


def yes_no_ask_callback(value: str):
    """
    A typer callback to handle yes/no/ask parameters
    """
    if value not in YesNoAskOptions:
        raise typer.BadParameter(
            f"Valid values: {YesNoAskOptions}. You provided: {value}",
        )
    return value


def getDeployNames(database, schema, name) -> dict:
    stage = f"{database}.{schema}.deployments"
    path = f"/{name.lower()}/app.zip"
    directory = f"/{name.lower()}"
    return {
        "stage": stage,
        "path": path,
        "full_path": f"@{stage}{path}",
        "directory": directory,
    }


# create a temporary directory, copy the file_path to it and rename to app.zip


def prepareAppZip(file_path, temp_dir) -> str:
    # get filename from file path (e.g. app.zip from /path/to/app.zip)
    file_name = os.path.basename(file_path)
    temp_path = temp_dir + "/" + file_name
    shutil.copy(file_path, temp_path)
    return temp_path


def parseRequirements(requirements_file: str = "requirements.txt") -> list[str]:
    """Reads and parses a python requirements.txt file.

    Args:
        requirements_file (str, optional): The name of the file.
        Defaults to 'requirements.txt'.

    Returns:
        list[str]: A flat list of package names, without versions
    """
    reqs = []
    if os.path.exists(requirements_file):
        with open(requirements_file, encoding="utf-8") as f:
            for req in requirements.parse(f):
                reqs.append(req.name)
    else:
        click.echo(f"No {requirements_file} found")

    return reqs


# parse JSON from https://repo.anaconda.com/pkgs/snowflake/channeldata.json and
# return a list of packages that exist in packages with the .packages json
# response from https://repo.anaconda.com/pkgs/snowflake/channeldata.json
# CURRENTLY DOES NOT SUPPORT PINNING TO VERSIONS


def parseAnacondaPackages(packages: list[str]) -> dict:
    url = "https://repo.anaconda.com/pkgs/snowflake/channeldata.json"
    response = requests.get(url)
    snowflakePackages = []
    otherPackages = []
    if response.status_code == 200:
        channel_data = response.json()
        for package in packages:
            # pip package names are case insensitive,
            # Anaconda package names are lowercased
            if package.lower() in channel_data["packages"]:
                snowflakePackages.append(
                    f"{package}",
                )
            else:
                click.echo(
                    f'"{package}" not found in Snowflake anaconda channel...',
                )
                otherPackages.append(package)
        # As at April 2023, streamlit appears unavailable in the Snowflake Anaconda channel
        # but actually works if specified in the environment
        if "streamlit" in otherPackages:
            otherPackages.remove("streamlit")
        return {"snowflake": snowflakePackages, "other": otherPackages}
    else:
        click.echo(f"Error: {response.status_code}")
        return {}


def generateStreamlitEnvironmentFile(
    excluded_anaconda_deps: Optional[List[str]],
) -> Optional[Path]:
    """Creates an environment.yml file for streamlit deployment, if a Snowflake
    requirements file exists.
    The file path is returned if it was generated, otherwise None is returned.
    """
    if os.path.exists("requirements.snowflake.txt"):
        # for each line in requirements.snowflake.txt, prepend '- ' to the line and prepare it for interpolation into the template
        with open("requirements.snowflake.txt", "r", encoding="utf-8") as f:
            requirements = f.read().split("\n")
        # remove explicitly excluded anaconda dependencies
        if excluded_anaconda_deps is not None:
            print(f"""Excluded dependencies: {','.join(excluded_anaconda_deps)}""")
            requirements = [
                line for line in requirements if line not in excluded_anaconda_deps
            ]
        # remove duplicates, remove comments, remove snowflake-connector-python
        requirements = [
            f"- {line}"
            for line in sorted(list(set(requirements)))
            if len(line) > 0 and line[0] != "#" and line != "snowflake-connector-python"
        ]
        dependencies_list = "\n".join(requirements)
        environment = Environment(loader=FileSystemLoader(templates_path))
        template = environment.get_template("environment.yml.jinja")
        with open("environment.yml", "w", encoding="utf-8") as f:
            f.write(template.render(dependencies=dependencies_list))
        return Path("environment.yml")
    return None


def generateStreamlitPackageWrapper(
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


def getDownloadedPackageNames() -> dict[str, list[str]]:
    """Returns a dict of official package names mapped to the files/folders
    that belong to it under the .packages directory.

    Returns:
        dict[str:List[str]]: a dict of package folder names to package name
    """
    metadata_files = glob.glob(".packages/*dist-info/METADATA")
    packages_full_path = os.path.abspath(".packages")
    return_dict = {}
    for metadata_file in metadata_files:
        parent_folder = os.path.dirname(metadata_file)
        package_name = getPackageNameFromMetadata(metadata_file)
        if package_name is not None:
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
                    return_dict[package_name] = included_record_entries
    return return_dict


def getPackageNameFromMetadata(metadata_file_path: str) -> str | None:
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
        return results.group(1)


def generateSnowparkCoverageWrapper(
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


def addFileToExistingZip(zip_file: str, other_file: str):
    """Adds another file to an existing zip file

    Args:
        zip_file (str): The existing zip file
        other_file (str): The new file to add
    """
    with zipfile.ZipFile(zip_file, mode="a") as myzip:
        myzip.write(other_file, os.path.basename(other_file))


def installPackages(
    file_name: str | None,
    perform_anaconda_check: bool = True,
    package_native_libraries: YesNoAskOptionsType = "ask",
    package_name: str | None = None,
) -> tuple[bool, dict[str, list[str]] | None]:
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
                click.echo(line.strip())
            process.wait()
            pip_install_result = process.returncode
        except FileNotFoundError:
            click.echo(
                "\n\npip not found. Please install pip and try again.\nHINT: you can also set the environment variable 'SNOWCLI_PIP_PATH' to the path of pip.",
                err=True,
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
                click.echo(line.strip())
            process.wait()
            pip_install_result = process.returncode
        except FileNotFoundError:
            click.echo(
                "\n\npip not found. Please install pip and try again.\nHINT: you can also set the environment variable 'SNOWCLI_PIP_PATH' to the path of pip.",
                err=True,
            )
            return False, None

    if pip_install_result is not None and pip_install_result != 0:
        print(
            f"pip failed with return code {pip_install_result}. \n\nThis may happen when attempting to install a package that isn't compatible with the host architecture - and generally means it has native libraries."
        )
        return False, None
    if perform_anaconda_check:
        click.echo("Checking for dependencies available in Anaconda...")
        # it's not over just yet. a non-Anaconda package may have brought in
        # a package available on Anaconda.
        # use each folder's METADATA file to determine its real name
        downloaded_packages = getDownloadedPackageNames()
        click.echo(f"Downloaded packages: {downloaded_packages.values()}")
        # look for all the downloaded packages on the Anaconda channel
        second_chance_results = parseAnacondaPackages(
            list(downloaded_packages.keys()),
        )
        second_chance_snowflake_packages = second_chance_results["snowflake"]
        if len(second_chance_snowflake_packages) > 0:
            click.echo(
                f"""Good news! The following package dependencies can be
                imported directly from Anaconda, and will be excluded from
                the zip: {second_chance_snowflake_packages}""",
            )
        else:
            click.echo(
                "None of the package dependencies were found on Anaconda",
            )
        downloaded_packages_not_needed = {
            k: v
            for k, v in downloaded_packages.items()
            if k in second_chance_snowflake_packages
        }
        for package, items in downloaded_packages_not_needed.items():
            click.echo(f"Package {package}: deleting {items}")
            for item in items:
                item_path = os.path.join(".packages", item)
                if os.path.exists(item_path):
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)

    click.echo("Checking to see if packages have native libaries...\n")
    # use glob to see if any files in packages have a .so extension
    if glob.glob(".packages/**/*.so"):
        for path in glob.glob(".packages/**/*.so"):
            click.echo(f"Potential native library: {path}")
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
        click.echo(
            "No non-supported native libraries found in packages (Good news!)..."
        )
        return True, second_chance_results


def recursiveZipPackagesDir(pack_dir: str, dest_zip: str) -> bool:
    # create a zip file object
    zipf = zipfile.ZipFile(dest_zip, "w", zipfile.ZIP_DEFLATED, allowZip64=True)

    # for every file in the relative path pack_dir, add it to the zip file
    for file in pathlib.Path(pack_dir).glob("**/*"):
        zipf.write(file, arcname=os.path.relpath(file, pack_dir))

    # zip all files in the current directory except the ones that start with "." or are in the pack_dir
    for file in pathlib.Path(".").glob("**/*"):
        if (
            not str(file).startswith(".")
            and not file.match(f"{pack_dir}/*")
            and not file.match(dest_zip)
        ):
            zipf.write(os.path.relpath(file))

    for dir_path in os.getenv("SNOWCLI_INCLUDE_PATHS", "").split(":"):
        directory = pathlib.Path(dir_path)
        if directory.is_dir():
            for file in pathlib.Path(directory).glob("**/*"):
                if (
                    not str(file).startswith(".")
                    and not file.match("*.pyc")
                    and not file.match("*__pycache__*")
                ):
                    zipf.write(file, arcname=os.path.relpath(file, directory))

    # close the zip file object
    zipf.close()
    return True


def standardZipDir(dest_zip: str) -> bool:
    zipf = zipfile.ZipFile(dest_zip, "w", zipfile.ZIP_DEFLATED, allowZip64=True)
    for file in pathlib.Path(".").glob("*"):
        if not file.match(".*"):
            zipf.write(os.path.relpath(file))

    for dir_path in os.getenv("SNOWCLI_INCLUDE_PATHS", "").split(":"):
        directory = pathlib.Path(dir_path)
        if directory.is_dir():
            for file in pathlib.Path(directory).glob("**/*"):
                if (
                    not str(file).startswith(".")
                    and not file.match("*.pyc")
                    and not file.match("*__pycache__*")
                ):
                    zipf.write(file, arcname=os.path.relpath(file, directory))

    # close the zip file object
    zipf.close()
    return True


def getSnowflakePackages() -> list[str]:
    if os.path.exists("requirements.snowflake.txt"):
        with open("requirements.snowflake.txt", encoding="utf-8") as f:
            return [line.strip() for line in f]
    else:
        return []


def getSnowflakePackagesDelta(anaconda_packages) -> list[str]:
    updatedPackageList = []
    if os.path.exists("requirements.snowflake.txt"):
        with open("requirements.snowflake.txt", encoding="utf-8") as f:
            # for each line, check if it exists in anaconda_packages. If it
            # doesn't, add it to the return string
            for line in f:
                if line.strip() not in anaconda_packages:
                    updatedPackageList.append(line.strip())
        return updatedPackageList
    else:
        return updatedPackageList


def convertResourceDetailsToDict(function_details: list[tuple]) -> dict:
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


def print_db_cursor(
    cursor, only_cols=[], show_header: bool = True, show_border: bool = True
):
    if cursor.description:
        if any(only_cols):
            cols = [
                (index, col[0])
                for (index, col) in enumerate(
                    cursor.description,
                )
                if col[0] in only_cols
            ]
        else:
            cols = [(index, col[0]) for (index, col) in enumerate(cursor.description)]

        box_val = box.HEAVY_HEAD if show_border else None

        table = Table(
            *[col[1] for col in cols],
            show_header=show_header,
            box=box_val,
            border_style=None,
        )
        for row in cursor.fetchall():
            filtered_row = [str(row[col_index]) for (col_index, _) in cols]
            try:
                table.add_row(*filtered_row)
            except Exception as e:
                print(type(e))
                print(e.args)
                print(e)
        print(table)


def print_list_tuples(lt: SnowflakeCursor):
    table = Table("Key", "Value")
    for item in lt:
        if item[0] == "imports":
            table.add_row(item[0], item[1].strip("[]"))
        else:
            table.add_row(item[0], item[1])
    print(table)


def conf_callback(ctx: typer.Context, param: typer.CallbackParam, value: str):
    if value:
        try:
            app_config = AppConfig().config

            # Initialize the default map
            ctx.default_map = ctx.default_map or {}
            # if app_config has key 'default'
            config_section = os.getenv("SNOWCLI_CONFIG_SECTION", "default")
            if config_section in app_config:
                ctx.default_map.update(
                    app_config.get(config_section),
                )  # type: ignore
            if value in app_config:
                # TODO: Merge the config dict into default_map
                # type: ignore
                ctx.default_map.update(app_config.get(value))
        except Exception as ex:
            raise typer.BadParameter(str(ex))
    return value


def generate_deploy_stage_name(name: str, input_parameters: str) -> str:
    return name + input_parameters.replace("(", "",).replace(")", "",).replace(
        " ",
        "_",
    ).replace(
        ",",
        "",
    )
