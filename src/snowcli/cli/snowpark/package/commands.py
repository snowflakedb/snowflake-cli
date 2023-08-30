from __future__ import annotations

import logging
from pathlib import Path

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.package.manager import (
    lookup,
    create,
    cleanup_after_install,
    upload,
)
from snowcli.cli.snowpark.package.utils import (
    InAnaconda,
    NotInAnaconda,
    RequiresPackages,
    NothingFound,
    CreatedSuccessfully,
)
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData

app = typer.Typer(
    name="package",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage custom Python packages for Snowpark",
)
log = logging.getLogger(__name__)


@app.command("lookup")
@global_options_with_connection
@with_output
def package_lookup(
    name: str = typer.Argument(..., help="Name of the package"),
    install_packages: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Install packages that are not available on the Snowflake anaconda channel",
    ),
    **options,
) -> OutputData:
    """
    Checks if a package is available on the Snowflake anaconda channel.
    In install_packages flag is set to True, command will check all the dependencies of the packages
    outside snowflake channel.
    """
    lookup_result = lookup(name=name, install_packages=install_packages)
    cleanup_after_install()
    return OutputData.from_string(lookup_result.message)


@app.command("upload")
@global_options_with_connection
@with_output
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
    **options,
) -> OutputData:
    """
    Upload a python package zip file to a Snowflake stage, so it can be referenced in the imports of a procedure or function.
    """
    return OutputData.from_string(upload(file=file, stage=stage, overwrite=overwrite))


@app.command("create")
@global_options_with_connection
@with_output
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
    **options,
) -> OutputData:
    """
    Create a python package as a zip file that can be uploaded to a stage and imported for a Snowpark python app.
    """

    if (
        type(lookup_result := lookup(name=name, install_packages=install_packages))
        in [
            NotInAnaconda,
            RequiresPackages,
        ]
        and type(creation_result := create(name)) == CreatedSuccessfully
    ):
        message = creation_result.message
        if type(lookup_result) == RequiresPackages:
            message += "\n" + lookup_result.message
    else:
        message = lookup_result.message

    cleanup_after_install()
    return OutputData.from_string(message)
