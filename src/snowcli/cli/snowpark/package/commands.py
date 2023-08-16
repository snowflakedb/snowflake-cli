from __future__ import annotations

import logging
from pathlib import Path

import typer

from snowcli.cli.common.decorators import global_options
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.package.manager import PackageManager
from snowcli.cli.snowpark.package.utils import (
    InAnaconda,
    NotInAnaconda,
    RequiresPackages,
    NothingFound,
)
from snowcli.output.decorators import with_output

app = typer.Typer(
    name="package",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage custom Python packages for Snowpark",
)
log = logging.getLogger(__name__)


@app.command("lookup")
@global_options
def package_lookup(
    name: str = typer.Argument(..., help="Name of the package"),
    install_packages: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Install packages that are not available on the Snowflake anaconda channel",
    ),
    **kwargs,
):
    """
    Checks if a package is available on the Snowflake anaconda channel.
    In install_packages flag is set to True, command will check all the dependencies of the packages
    outside snowflake channel.
    """
    lookup_result = PackageManager().lookup(
        name=name, install_packages=install_packages
    )

    if type(lookup_result) == InAnaconda:
        message = f"Package {name} is available on the Snowflake anaconda channel."
    elif type(lookup_result) == RequiresPackages:
        message = f"""The package {name} is supported, but does depend on the
                following Snowflake supported native libraries. You should
                include the following in your packages: {lookup_result.requirements.snowflake}"""
    elif type(lookup_result) == NotInAnaconda:
        message = f"""The package {name} is avaiable through PIP. You can create a zip using:\n
                snow snowpark package create {name} -y"""
    elif type(lookup_result) == NothingFound:
        message = f"Lookup for package {name} resulted in some error. Please check the package name and try again"
    q = log.name
    log.info(message)
    return message


@app.command("upload")
@global_options
def package_upload(
    file: Path = typer.Option(
        ...,
        "--file",
        "-f",
        help="Path to the file to update",
        exists=False,
    ),
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="The stage to upload the file to, NOT including @ symbol",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Overwrite the file if it already exists",
    ),
    **kwargs,
) -> str:
    """
    Upload a python package zip file to a Snowflake stage, so it can be referenced in the imports of a procedure or function.
    """
    return PackageManager().upload(file, stage, overwrite)


@app.command("create")
@global_options
def package_create(
    name: str = typer.Argument(
        ...,
        help="Name of the package",
    ),
    install_packages: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Install packages that are not available on the Snowflake anaconda channel",
    ),
    **kwargs,
):
    """
    Create a python package as a zip file that can be uploaded to a stage and imported for a Snowpark python app.
    """
    PackageManager().lookup(name, install_packages, True)
    PackageManager().create(name)
