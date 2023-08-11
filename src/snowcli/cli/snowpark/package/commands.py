from __future__ import annotations

import logging
from pathlib import Path

import typer

from snowcli.cli.common.decorators import global_options
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.package.manager import PackageManager
from snowcli.cli.snowpark.package.utils import (
    InAnaconda,
    Unsupported,
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
@with_output
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
    result = PackageManager().lookup(name=name, install_packages=install_packages)

    if type(result) == InAnaconda:
        return f"Package {name} is available on the Snowflake anaconda channel."
    elif type(result) == RequiresPackages:
        return f"""The package {name} is supported, but does depend on the
                following Snowflake supported native libraries. You should
                include the following in your packages: {result.requirements.snowflake}"""


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
