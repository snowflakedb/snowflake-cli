from __future__ import annotations

import glob
import json
import os
import re
import shutil
from typing import Literal

import click
import requests
import requirements
import typer
from rich import print
from rich.table import Table
from snowcli.config import AppConfig
from snowflake.connector.cursor import SnowflakeCursor

YesNoAskOptions = ["yes", "no", "ask"]
YesNoAskOptionsType = Literal["yes", "no", "ask"]


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
    temp_path = temp_dir + "/app.zip"
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
        return {"snowflake": snowflakePackages, "other": otherPackages}
    else:
        click.echo(f"Error: {response.status_code}")
        return {}


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


def installPackages(
    file_name: str,
    perform_anaconda_check: bool = True,
    package_native_libraries: YesNoAskOptionsType = "ask",
) -> tuple[bool, dict[str, list[str]] | None]:
    os.system(f"pip install -t .packages/ -r {file_name}")
    second_chance_results = None
    if perform_anaconda_check:
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
        click.echo("No native libraries found in packages (Good news!)...")
        return True, second_chance_results


def recursiveZipPackagesDir(pack_dir: str, dest_zip: str) -> bool:
    prevdir = os.getcwd()
    os.chdir(f"./{pack_dir}")
    os.system(f"zip -r ../{dest_zip} .")
    os.chdir(prevdir)
    os.system(f'zip -r -g {dest_zip} . -x ".*" -x "{pack_dir}/*"')
    return True


def standardZipDir(dest_zip: str) -> bool:
    os.system(f'zip -r {dest_zip} . -x ".*"')
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


def print_db_cursor(cursor, only_cols=[]):
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

        table = Table(*[col[1] for col in cols])
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
            if "default" in app_config:
                ctx.default_map.update(
                    app_config.get("default"),
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
