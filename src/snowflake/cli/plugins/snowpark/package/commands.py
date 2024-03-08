from __future__ import annotations

import logging
from pathlib import Path
from textwrap import dedent

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.models import YesNoAsk
from snowflake.cli.plugins.snowpark.package.manager import (
    check_if_package_in_anaconda,
    cleanup_after_install,
    create_package,
    lookup,
    upload,
)
from snowflake.cli.plugins.snowpark.package.utils import (
    get_readable_list_of_requirements,
)
from snowflake.cli.plugins.snowpark.package_utils import check_for_native_libraries
from snowflake.cli.plugins.snowpark.snowpark_shared import (
    PackageNativeLibrariesOption,
    check_if_can_continue_with_native_libs,
)

app = SnowTyper(
    name="package",
    help="Manages custom Python packages for Snowpark",
)
log = logging.getLogger(__name__)

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


@app.command("lookup", requires_connection=True)
@cleanup_after_install
def package_lookup(
    name: str = typer.Argument(..., help="Name of the package."),
    install_packages: bool = install_option,
    _deprecated_install_option: bool = deprecated_install_option,
    **options,
) -> CommandResult:
    """
    Checks if a package is available on the Snowflake Anaconda channel.
    If the `--pypi-download` flag is provided, this command checks all dependencies of the packages
    outside Snowflake channel.
    """
    if _deprecated_install_option:
        install_packages = _deprecated_install_option

    lookup_result = lookup(package_name=name, install_packages=install_packages)
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
        help="Overwrites the file if it already exists.",
    ),
    **options,
) -> CommandResult:
    """
    Uploads a Python package zip file to a Snowflake stage so it can be referenced in the imports of a procedure or function.
    """
    return MessageResult(upload(file=file, stage=stage, overwrite=overwrite))


@app.command("create", requires_connection=True)
@cleanup_after_install
def package_create(
    package_name: str = typer.Argument(
        ...,
        help="Name of the package to create.",
    ),
    install_packages: bool = install_option,
    package_native_libraries: YesNoAsk = PackageNativeLibrariesOption,
    _deprecated_install_option: bool = deprecated_install_option,
    **options,
) -> CommandResult:
    """
    Creates a Python package as a zip file that can be uploaded to a stage and imported for a Snowpark Python app.
    """
    if _deprecated_install_option:
        install_packages = _deprecated_install_option

    with cli_console.phase("Anaconda check"):
        available_in_anaconda, _ = check_if_package_in_anaconda(package_name)

        # If the package is in Anaconda there's nothing to do
        if available_in_anaconda:
            return MessageResult(
                f"Package {package_name} is available on the Snowflake Anaconda channel."
            )
        cli_console.message("Package not available on Anaconda")

    # Now there are some missing package in Anaconda and if we are not
    # allowed to install them, so we can only quit
    if not install_packages:
        return MessageResult(
            f"Package {package_name} is not available on Anaconda. Creating the package "
            f"requires installing additional packages. Enable it by providing --pypi-download."
        )

    with cli_console.phase("Building the package"):
        split_requirements = package_utils.install_packages(
            perform_anaconda_check=True, package_name=package_name, file_name=None
        )

        if check_for_native_libraries():
            check_if_can_continue_with_native_libs(package_native_libraries)

        # If we can do a local installation, then lookup created packages directory that we now zip
        zip_file = create_package(package_name)

    message = dedent(
        f"""
    Package {package_name}.zip created. Upload it to stage using:
    snow snowpark package upload -f {zip_file} -s <stage-name>
    and reference it in your procedure or function imports.
    """
    )

    # Some requirements can be available in Snowflake and not included in the zip.
    # In such case users will have to add them manually, so we add the list to the message
    if split_requirements.snowflake:
        # There are requirements available in snowflake that has to be added to package list
        packages_to_be_added_manually = get_readable_list_of_requirements(
            split_requirements.snowflake
        )
        message += f"\nYou should also include following packages in your function or procedure:\n{packages_to_be_added_manually}"

    return MessageResult(message)
