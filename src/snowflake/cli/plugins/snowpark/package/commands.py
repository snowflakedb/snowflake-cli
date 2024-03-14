from __future__ import annotations

import logging
from pathlib import Path
from textwrap import dedent

import typer
from click import ClickException
from requests import HTTPError
from snowflake.cli.api.commands.flags import deprecated_flag_callback
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.plugins.snowpark.models import PypiOption, Requirement
from snowflake.cli.plugins.snowpark.package.anaconda import AnacondaChannel
from snowflake.cli.plugins.snowpark.package.manager import (
    cleanup_after_install,
    create_packages_zip,
    lookup,
    upload,
)
from snowflake.cli.plugins.snowpark.package.utils import (
    NotInAnaconda,
    RequiresPackages,
)
from snowflake.cli.plugins.snowpark.snowpark_shared import PackageNativeLibrariesOption

app = SnowTyper(
    name="package",
    help="Manages custom Python packages for Snowpark",
)
log = logging.getLogger(__name__)


lookup_install_option = typer.Option(
    False,
    "--pypi-download",
    hidden=True,
    callback=deprecated_flag_callback(
        "Using --pypi-download is deprecated. Lookup command no longer checks for package in PyPi."
    ),
    help="Installs packages that are not available on the Snowflake Anaconda channel.",
)

lookup_deprecated_install_option = typer.Option(
    False,
    "--yes",
    "-y",
    hidden=True,
    callback=deprecated_flag_callback(
        "Using --yes is deprecated. Lookup command no longer checks for package in PyPi."
    ),
    help="Installs packages that are not available on the Snowflake Anaconda channel.",
)


@app.command("lookup", requires_connection=True)
def package_lookup(
    package_name: str = typer.Argument(
        ..., help="Name of the package.", show_default=False
    ),
    # todo: remove with 3.0
    _: bool = lookup_install_option,
    __: bool = lookup_deprecated_install_option,
    **options,
) -> CommandResult:
    """
    Checks if a package is available on the Snowflake Anaconda channel.
    """
    try:
        anaconda = AnacondaChannel.from_snowflake()
    except HTTPError as err:
        raise ClickException(
            f"Accessing Snowflake Anaconda channel failed. Reason {err}"
        )

    package = Requirement.parse(package_name)
    if anaconda.is_package_available(package=package):
        msg = f"Package `{package_name}` is available in Anaconda."
        if version := anaconda.package_version(package=package):
            msg += f" Latest available version: {version}."
        return MessageResult(msg)

    return MessageResult(
        dedent(
            f"""
        Package `{package_name}` is not available in Anaconda. To prepare Snowpark compatible package run:
        snow snowpark package create {package_name}
        """
        )
    )


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
        help="Overwrites the file if it already exists.",
    ),
    **options,
) -> CommandResult:
    """
    Uploads a Python package zip file to a Snowflake stage so it can be referenced in the imports of a procedure or function.
    """
    return MessageResult(upload(file=file, stage=stage, overwrite=overwrite))


install_option = typer.Option(
    False,
    "--pypi-download",
    help="Installs packages that are not available on the Snowflake Anaconda channel.",
)

deprecated_install_option = typer.Option(
    False,
    "--yes",
    "-y",
    hidden=True,
    help="Installs packages that are not available on the Snowflake Anaconda channel.",
)


@app.command("create", requires_connection=True)
@cleanup_after_install
def package_create(
    name: str = typer.Argument(
        ...,
        help="Name of the package to create.",
    ),
    install_packages: bool = install_option,
    _deprecated_install_option: bool = deprecated_install_option,
    allow_native_libraries: PypiOption = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """
    Creates a Python package as a zip file that can be uploaded to a stage and imported for a Snowpark Python app.
    """
    if _deprecated_install_option:
        install_packages = _deprecated_install_option

    lookup_result = lookup(
        name=name,
        install_packages=install_packages,
        allow_native_libraries=allow_native_libraries,
    )

    if not isinstance(lookup_result, (NotInAnaconda, RequiresPackages)):
        return MessageResult(lookup_result.message)

    # The package is not in anaconda so we have to pack it
    zip_file = create_packages_zip(name)
    message = dedent(
        f"""
    Package {zip_file} created. You can now upload it to a stage using
    snow snowpark package upload -f {zip_file} -s <stage-name>`
    and reference it in your procedure or function.
    """
    )
    if isinstance(lookup_result, RequiresPackages):
        message += "\n" + lookup_result.message

    return MessageResult(message)
