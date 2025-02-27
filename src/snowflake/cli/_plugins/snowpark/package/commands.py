# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
from pathlib import Path
from textwrap import dedent
from typing import Optional

import typer
from click import ClickException
from snowflake.cli._plugins.snowpark.models import (
    Requirement,
)
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AnacondaPackagesManager,
)
from snowflake.cli._plugins.snowpark.package.manager import upload
from snowflake.cli._plugins.snowpark.package_utils import (
    detect_and_log_shared_libraries,
    download_unavailable_packages,
    get_package_name_from_pip_wheel,
)
from snowflake.cli._plugins.snowpark.snowpark_shared import (
    AllowSharedLibrariesOption,
    IgnoreAnacondaOption,
    SkipVersionCheckOption,
)
from snowflake.cli._plugins.snowpark.zipper import zip_dir
from snowflake.cli.api.commands.flags import IndexUrlOption
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.secure_path import SecurePath

app = SnowTyperFactory(
    name="package",
    help="Manages custom Python packages for Snowpark",
)
log = logging.getLogger(__name__)


@app.command("lookup", requires_connection=True)
def package_lookup(
    package_name: str = typer.Argument(
        ..., help="Name of the package.", show_default=False
    ),
    **options,
) -> CommandResult:
    """
    Checks if a package is available on the Snowflake Anaconda channel.
    """
    anaconda_packages_manager = AnacondaPackagesManager()
    anaconda_packages = (
        anaconda_packages_manager.find_packages_available_in_snowflake_anaconda()
    )

    package = Requirement.parse(package_name)
    if anaconda_packages.is_package_available(package=package):
        msg = f"Package `{package_name}` is available in Anaconda"
        if version := anaconda_packages.package_latest_version(package=package):
            msg += f". Latest available version: {version}."
        elif versions := anaconda_packages.package_versions(package=package):
            msg += f" in versions: {', '.join(versions)}."
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
        show_default=False,
    ),
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="Name of the stage in which to upload the file, not including the @ symbol.",
        show_default=False,
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
def package_create(
    name: str = typer.Argument(
        ...,
        help="Name of the package to create.",
        show_default=False,
    ),
    ignore_anaconda: bool = IgnoreAnacondaOption,
    index_url: Optional[str] = IndexUrlOption,
    skip_version_check: bool = SkipVersionCheckOption,
    allow_shared_libraries: bool = AllowSharedLibrariesOption,
    **options,
) -> CommandResult:
    """
    Creates a Python package as a zip file that can be uploaded to a stage and imported for a Snowpark Python app.
    """
    with SecurePath.temporary_directory() as packages_dir:
        package = Requirement.parse(name)
        anaconda_packages_manager = AnacondaPackagesManager()
        download_result = download_unavailable_packages(
            requirements=[package],
            target_dir=packages_dir,
            anaconda_packages=(
                AnacondaPackages.empty()
                if ignore_anaconda
                else anaconda_packages_manager.find_packages_available_in_snowflake_anaconda()
            ),
            skip_version_check=skip_version_check,
            pip_index_url=index_url,
        )

        # check if package was detected as available
        package_available_in_conda = any(
            p.line == package.line for p in download_result.anaconda_packages
        )
        if package_available_in_conda:
            return MessageResult(
                f"Package {name} is already available in Snowflake Anaconda Channel."
            )

        # The package is not in anaconda, so we have to pack it
        log.info("Checking to see if packages have shared (.so/.dll) libraries...")
        if detect_and_log_shared_libraries(download_result.downloaded_packages_details):
            if not allow_shared_libraries:
                raise ClickException(
                    "Some packages contain shared (.so/.dll) libraries. "
                    "Try again with --allow-shared-libraries."
                )

        # The package is not in anaconda, so we have to pack it
        # the package was downloaded once, pip wheel should use cache
        zip_file = f"{get_package_name_from_pip_wheel(name, index_url=index_url)}.zip"
        zip_dir(dest_zip=Path(zip_file), source=packages_dir.path)
        message = dedent(
            f"""
        Package {zip_file} created. You can now upload it to a stage using
        snow snowpark package upload -f {zip_file} -s <stage-name>`
        and reference it in your procedure or function.
        Remember to add it to imports in the procedure or function definition.
        """
        )
        if download_result.anaconda_packages:
            message += dedent(
                f"""
                The package {name} is successfully created, but depends on the following
                Anaconda libraries. They need to be included in project requirements,
                as their are not included in .zip.
                """
            )
            message += "\n".join(
                (req.line for req in download_result.anaconda_packages)
            )

        return MessageResult(message)
