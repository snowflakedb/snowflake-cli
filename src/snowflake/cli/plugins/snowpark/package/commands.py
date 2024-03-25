from __future__ import annotations

import logging
from pathlib import Path
from textwrap import dedent
from typing import Optional

import typer
from snowflake.cli.api.commands.flags import (
    deprecated_flag_callback,
)
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.models import (
    Requirement,
    YesNoAsk,
)
from snowflake.cli.plugins.snowpark.package.anaconda import (
    AnacondaChannel,
)
from snowflake.cli.plugins.snowpark.package.manager import (
    cleanup_packages_dir,
    create_packages_zip,
    upload,
)
from snowflake.cli.plugins.snowpark.package_utils import download_packages
from snowflake.cli.plugins.snowpark.snowpark_shared import (
    AllowSharedLibrariesOption,
    IgnoreAnacondaOption,
    deprecated_allow_native_libraries_option,
)

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
    anaconda = AnacondaChannel.from_snowflake()

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


@app.command("create", requires_connection=True)
@cleanup_packages_dir
def package_create(
    name: str = typer.Argument(
        ...,
        help="Name of the package to create.",
    ),
    ignore_anaconda: bool = IgnoreAnacondaOption,
    index_url: Optional[str] = index_option,
    skip_version_check: bool = skip_version_check_option,
    allow_shared_libraries: bool = AllowSharedLibrariesOption,
    deprecated_allow_native_libraries: YesNoAsk = deprecated_allow_native_libraries_option(
        "--allow-native-libraries"
    ),
    _deprecated_install_option: bool = deprecated_install_option,
    _deprecated_install_packages: bool = deprecated_pypi_download_option,
    **options,
) -> CommandResult:
    """
    Creates a Python package as a zip file that can be uploaded to a stage and imported for a Snowpark Python app.
    """
    # TODO: yes/no/ask logic should be removed in 3.0
    allow_shared_libraries_yesnoask = {
        True: YesNoAsk.YES,
        False: YesNoAsk.NO,
    }[allow_shared_libraries]
    if deprecated_allow_native_libraries != YesNoAsk.NO:
        allow_shared_libraries_yesnoask = deprecated_allow_native_libraries

    package = Requirement.parse(name)
    if ignore_anaconda:
        anaconda = None
    else:
        anaconda = AnacondaChannel.from_snowflake()
        if anaconda.is_package_available(
            package, skip_version_check=skip_version_check
        ):
            return MessageResult(
                f"Package {name} is already available in Snowflake Anaconda Channel."
            )

    packages_dir = SecurePath(".packages")
    packages_are_downloaded, dependencies = download_packages(
        anaconda=anaconda,
        ignore_anaconda=ignore_anaconda,
        requirements=[package],
        packages_dir=packages_dir,
        index_url=index_url,
        allow_shared_libraries=allow_shared_libraries_yesnoask,
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
            The package {name} is successfully created, but depends on the following
            Anaconda libraries. They need to be included in project requirements,
            as their are not included in .zip.
            """
        )
        message += "\n".join((req.line for req in dependencies.snowflake))

    return MessageResult(message)
