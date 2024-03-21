from __future__ import annotations

import logging
from pathlib import Path
from textwrap import dedent
from typing import Optional

import typer
from snowflake.cli.api.commands.flags import deprecated_flag_callback
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.plugins.snowpark.models import (
    PypiOption,
    Requirement,
)
from snowflake.cli.plugins.snowpark.package.anaconda import (
    get_anaconda_from_snowflake,
)
from snowflake.cli.plugins.snowpark.package.manager import (
    cleanup_packages_dir,
    create_packages_zip,
    upload,
)
from snowflake.cli.plugins.snowpark.package_utils import download_packages
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
    anaconda = get_anaconda_from_snowflake()

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


deprecated_pypi_download_option = typer.Option(
    False,
    "--pypi-download",
    hidden=True,
    callback=deprecated_flag_callback(
        "Using --pypi-download is deprecated. Create command always checks for package in PyPi."
    ),
    help="Installs packages that are not available on the Snowflake Anaconda channel.",
)

deprecated_install_option = typer.Option(
    False,
    "--yes",
    "-y",
    hidden=True,
    help="Installs packages that are not available on the Snowflake Anaconda channel.",
    callback=deprecated_flag_callback(
        "Using --yes is deprecated. Create command always checks for package in PyPi."
    ),
)

deprecated_allow_native_libraries_option = typer.Option(
    PypiOption.NO.value,
    "--allow-native-libraries",
    help="Allows native libraries, when using packages installed through PIP",
    hidden=True,
)

ignore_anaconda_option = typer.Option(
    False,
    "--ignore-anaconda",
    help="Does not lookup packages on Snowflake Anaconda channel.",
)

index_option = typer.Option(
    None,
    "--index-url",
    help="Base URL of the Python Package Index to use for package lookup. This should point to "
    " a repository compliant with PEP 503 (the simple repository API) or a local directory laid"
    " out in the same format.",
    show_default=False,
)

skip_version_check_option = typer.Option(
    False,
    "--skip-version-check",
    help="Skip comparing versions of dependencies between requirements and Anaconda.",
)

allow_shared_libraries_option = typer.Option(
    False,
    "--allow-shared-libraries",
    help="Allows shared (.so) libraries, when using packages installed through PIP",
)


@app.command("create", requires_connection=True)
@cleanup_packages_dir
def package_create(
    name: str = typer.Argument(
        ...,
        help="Name of the package to create.",
    ),
    ignore_anaconda: bool = ignore_anaconda_option,
    index_url: Optional[str] = index_option,
    skip_version_check: bool = skip_version_check_option,
    allow_shared_libraries: PypiOption = PackageNativeLibrariesOption,
    _allow_native_libraries: PypiOption = deprecated_allow_native_libraries_option,
    _deprecated_install_option: bool = deprecated_install_option,
    _install_packages: bool = deprecated_pypi_download_option,
    **options,
) -> CommandResult:
    """
    Creates a Python package as a zip file that can be uploaded to a stage and imported for a Snowpark Python app.
    """
    if _allow_native_libraries != PypiOption.NO:
        allow_shared_libraries = _allow_native_libraries

    if ignore_anaconda:
        anaconda = None
    else:
        anaconda = get_anaconda_from_snowflake()
        package = Requirement.parse(name)
        if anaconda.is_package_available(
            package, skip_version_check=skip_version_check
        ):
            return MessageResult(
                f"Package {name} is already available in Snowflake Anaconda Channel."
            )

    packages_are_downloaded, dependencies = download_packages(
        anaconda=anaconda,
        perform_anaconda_check_for_dependencies=not ignore_anaconda,
        package_name=name,
        file_name=None,
        index_url=index_url,
        allow_shared_libraries=allow_shared_libraries,
        skip_version_check=skip_version_check,
    )

    if not packages_are_downloaded:
        return MessageResult(
            dedent(
                f"""
                Cannot create package for {name}. Please check the package name
                or try again with --allow-shared-libraries option.
                """
            )
        )

    # The package is not in anaconda, so we have to pack it
    zip_file = create_packages_zip(name)
    message = dedent(
        f"""
        Package {zip_file} created. You can now upload it to a stage using
        snow snowpark package upload -f {zip_file} -s <stage-name>`
        and reference it in your procedure or function.
        Remember to add it to imports in the procedure or function definition.
        """
    )
    if dependencies.snowflake:
        message += dedent(
            f"""
            The package {name} is supported, but does depend on the
            following Snowflake supported libraries. You should include the
            following dependencies in you function or procedure requirements:
            """
        )
        message += "\n".join((req.line for req in dependencies.snowflake))

    return MessageResult(message)
