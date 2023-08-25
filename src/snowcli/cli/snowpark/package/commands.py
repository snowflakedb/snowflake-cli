from __future__ import annotations

import logging
from pathlib import Path

import typer

from snowcli.cli.common.decorators import global_options, global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.snowpark.package.manager import (
    lookup,
    create,
    cleanup_after_install,
    upload,
)
from snowcli.cli.snowpark.package.utils import (
    NotInAnaconda,
    RequiresPackages,
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
@with_output
@global_options_with_connection
def package_lookup(
    name: str = typer.Argument(..., help="Name of the package"),
    install_packages: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Install packages that are not available on the Snowflake anaconda channel",
    ),
    **options,
):
    """
    Checks if a package is available on the Snowflake anaconda channel.
    In install_packages flag is set to True, command will check all the dependencies of the packages
    outside snowflake channel.
    """
    lookup_result = lookup(name=name, install_packages=install_packages)
    cleanup_after_install()
    return OutputData.from_string(lookup_result.message)


@app.command("upload")
@with_output
@global_options_with_connection
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
@with_output
@global_options_with_connection
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
):
    """
    Create a python package as a zip file that can be uploaded to a stage and imported for a Snowpark python app.
    """

    if type(lookup_result := lookup(name=name, install_packages=install_packages)) in [
        NotInAnaconda,
        RequiresPackages,
    ]:

        if type(creation_result := create(name)) == CreatedSuccessfully:
            message = f"Package {name}.zip created. You can now upload it to a stage (`snow snowpark package upload -f {name}.zip -s packages`) and reference it in your procedure or function."
            if type(lookup_result) == RequiresPackages:
                message += lookup_result.message
        else:
            message = lookup_result.message

        cleanup_after_install()
        return OutputData.from_string(message)
