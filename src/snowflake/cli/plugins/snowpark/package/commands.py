from __future__ import annotations

import logging
from pathlib import Path

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.plugins.snowpark.models import PypiOption
from snowflake.cli.plugins.snowpark.package.manager import (
    cleanup_after_install,
    create,
    lookup,
    upload,
)
from snowflake.cli.plugins.snowpark.package.utils import (
    CreatedSuccessfully,
    NotInAnaconda,
    RequiresPackages,
)
from snowflake.cli.plugins.snowpark.snowpark_shared import PackageNativeLibrariesOption

app = SnowTyper(
    name="package",
    help="Manage custom Python packages for Snowpark",
)
log = logging.getLogger(__name__)

install_option = typer.Option(
    False,
    "--pypi-download",
    help="Installs packages that are not available on the Snowflake anaconda channel.",
)

deprecated_install_option = typer.Option(
    False,
    "--yes",
    "-y",
    hidden=True,
    help="Installs packages that are not available on the Snowflake anaconda channel.",
)


@app.command("lookup", requires_connection=True)
def package_lookup(
    name: str = typer.Argument(..., help="Name of the package."),
    install_packages: bool = install_option,
    deprecated_install_option: bool = deprecated_install_option,
allow_native_libraries: PypiOption = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """
    Checks if a package is available on the Snowflake anaconda channel.
    If the `--pypi-download` flag is provided, this command checks all dependencies of the packages
    outside Snowflake channel.
    """
    if deprecated_install_option:
        install_packages = deprecated_install_option


    lookup_result = lookup(
        name=name,
        install_packages=install_packages,
        allow_native_libraries=allow_native_libraries,
    )
    cleanup_after_install()
    return MessageResult(lookup_result.message)


@app.command("upload", requires_connection=True)
def package_upload(
    file: Path = typer.Option(
        ...,
        "--file",
        "-f",
        help="Path to the file to upload.",
        exists=False,
    ),
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="Name of the stage in which to upload the file, not including the @ symbol.",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Whether to overwrite the file if it already exists.",
    ),
    **options,
) -> CommandResult:
    """
    Uploads a python package zip file to a Snowflake stage so it can be referenced in the imports of a procedure or function.
    """
    return MessageResult(upload(file=file, stage=stage, overwrite=overwrite))


@app.command("create", requires_connection=True)
def package_create(
    name: str = typer.Argument(
        ...,
        help="Name of the package to create.",
    ),
    install_packages: bool = install_option,
    deprecated_install_option: bool = deprecated_install_option,
allow_native_libraries: PypiOption = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """
    Creates a python package as a zip file that can be uploaded to a stage and imported for a Snowpark python app.
    """
    if deprecated_install_option:
        install_packages = deprecated_install_option

    if (
        type(
            lookup_result := lookup(
                name=name,
                install_packages=install_packages,
                allow_native_libraries=allow_native_libraries,
            )
        )
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
    return MessageResult(message)
