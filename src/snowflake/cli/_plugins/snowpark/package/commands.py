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
    IndexUrlOption,
    SkipVersionCheckOption,
)
from snowflake.cli._plugins.snowpark.zipper import zip_dir
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.secure_path import SecurePath

app = SnowTyperFactory(
    name="package",
    help="Manages custom Python packages for Snowpark",
)
log = logging.getLogger(__name__)

_PACKAGE_COMMANDS = "/developer-guide/snowflake-cli/command-reference/snowpark-commands/package-commands"

_PACKAGE_CREATE_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link("/developer-guide/snowflake-cli/command-reference/overview"),
    link(f"{_PACKAGE_COMMANDS}/overview", "Package command reference"),
    link(f"{_PACKAGE_COMMANDS}/lookup"),
    link(f"{_PACKAGE_COMMANDS}/upload"),
)

_PACKAGE_LOOKUP_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link("/developer-guide/snowflake-cli/command-reference/overview"),
    link(f"{_PACKAGE_COMMANDS}/overview", "Package command reference"),
    link(f"{_PACKAGE_COMMANDS}/create"),
    link(f"{_PACKAGE_COMMANDS}/upload"),
)

_PACKAGE_UPLOAD_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link("/developer-guide/snowflake-cli/command-reference/overview"),
    link(f"{_PACKAGE_COMMANDS}/overview", "Package command reference"),
    link(f"{_PACKAGE_COMMANDS}/create"),
    link(f"{_PACKAGE_COMMANDS}/lookup"),
)

_PACKAGE_LOOKUP_DOCS = CommandDocs(
    related=_PACKAGE_LOOKUP_RELATED,
    usage_notes=(
        plain_text(
            "The ",
            code("snow snowpark lookup"),
            " command checks to see whether a package is available on the Snowflake Anaconda channel.",
        ),
    ),
    examples=(
        Example(
            description=plain_text(
                "The following example illustrates looking up a package that is already available on the Snowflake Anaconda channel:"
            ),
            command="snow snowpark package lookup numpy",
            output="Package `numpy` is available in Anaconda. Latest available version: 1.26.4.",
        ),
        Example(
            description=plain_text(
                "If a package is not available on the Snowflake Anaconda channel, you can get a message similar to the following:"
            ),
            command="snow snowpark package lookup july",
            output=(
                "Package `july` is not available in Anaconda. To prepare Snowpark compatible package run:\n"
                "\n"
                "  snow snowpark package create july"
            ),
        ),
    ),
)


@app.command("lookup", requires_connection=True, docs=_PACKAGE_LOOKUP_DOCS)
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


_PACKAGE_UPLOAD_DOCS = CommandDocs(
    related=_PACKAGE_UPLOAD_RELATED,
    usage_notes=(
        plain_text(
            "If you specify a stage that does not exist, the command creates it automatically."
        ),
    ),
    examples=(
        Example(
            description=plain_text("Upload a package to a stage:"),
            command="snow snowpark package upload -f my_package.zip -s deployments",
            output="Package my_package.zip UPLOADED to Snowflake @deployments/my_package.zip.",
        ),
        Example(
            description=plain_text(
                "Upload a package to a stage that already contains a package with that name:"
            ),
            command="snow snowpark package upload -f my_package.zip -s deployments",
            output=(
                "Package already exists on stage. Consider using --overwrite to overwrite the file."
            ),
        ),
    ),
)


@app.command("upload", requires_connection=True, docs=_PACKAGE_UPLOAD_DOCS)
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


_PACKAGE_CREATE_DOCS = CommandDocs(
    related=_PACKAGE_CREATE_RELATED,
    usage_notes=(
        plain_text(
            "The ", code("snowpark package create"), " command does the following:"
        ),
        bullet_list(
            bullet("Creates an artifact ready to upload to a stage."),
            bullet(
                "Checks for native libraries and asks if you want to continue. If the native libraries are present in the downloaded packages, this command works the same as the ",
                code("snowpark package build"),
                " command.",
            ),
        ),
    ),
    examples=(
        Example(
            description=plain_text(
                'This example creates a Python package as a zip file that can be uploaded to a stage and later imported by a Snowpark Python app. Dependencies for the "july" package are found on the Anaconda channel, so they were excluded from the ',
                code(".zip"),
                " file. The command displays the packages you would need to include in *requirements.txt* of your Snowpark project.",
            ),
            command="snow snowpark package create july==0.1",
            output=(
                "Package july.zip created. You can now upload it to a stage using\n"
                "snow snowpark package upload -f july.zip -s <stage-name>`\n"
                "and reference it in your procedure or function.\n"
                "Remember to add it to imports in the procedure or function definition.\n"
                "\n"
                "The package july is successfully created, but depends on the following\n"
                "Anaconda libraries. They need to be included in project requirements,\n"
                "as their are not included in .zip.\n"
                "matplotlib\n"
                "contourpy >=1.0.1\n"
                "numpy>=1.20\n"
                "bokeh\n"
                "selenium\n"
                "mypy==1.8.0\n"
                "Pillow\n"
                "pytest-xdist\n"
                "wurlitzer\n"
                "cycler >=0.10\n"
                "fonttools >=4.22.0\n"
                "kiwisolver >=1.3.1\n"
                "pyparsing >=2.3.1\n"
                "jinja2\n"
                "python-dateutil >=2.7\n"
                "six >=1.5\n"
                "importlib-resources >=3.2.0"
            ),
        ),
        Example(
            description=plain_text(
                "This example creates the ",
                code("july.zip"),
                " package that you can use in your Snowpark project without needing to add any dependencies to the ",
                code("requirements.txt"),
                " file. The error messages indicate that some packages contain shared libraries, which might not work, such as when creating a package using Windows.",
            ),
            command=(
                "snow snowpark package create july==0.1 "
                "--ignore-anaconda --allow-shared-libraries"
            ),
            output=(
                "2024-04-11 16:24:56 ERROR Following dependencies utilise shared libraries, not supported by Conda:\n"
                "2024-04-11 16:24:56 ERROR numpy\n"
                "contourpy\n"
                "fonttools\n"
                "kiwisolver\n"
                "matplotlib\n"
                "pillow\n"
                "2024-04-11 16:24:56 ERROR You may still try to create your package with --allow-shared-libraries, but the might not work.\n"
                "2024-04-11 16:24:56 ERROR You may also request adding the package to Snowflake Conda channel\n"
                "2024-04-11 16:24:56 ERROR at https://support.anaconda.com/\n"
                "\n"
                "Package july.zip created. You can now upload it to a stage using\n"
                "snow snowpark package upload -f july.zip -s <stage-name>`\n"
                "and reference it in your procedure or function.\n"
                "Remember to add it to imports in the procedure or function definition."
            ),
        ),
        Example(
            description=plain_text(
                "This example fails to create the package because it already exists. You can still forcibly create the package by using the ",
                code("--ignore-anaconda"),
                " option.",
            ),
            command="snow snowpark package create matplotlib",
            output="Package matplotlib is already available in Snowflake Anaconda Channel.",
        ),
    ),
)


@app.command("create", requires_connection=True, docs=_PACKAGE_CREATE_DOCS)
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
                as they are not included in the .zip.
                """
            )
            message += "\n".join(
                (req.line for req in download_result.anaconda_packages)
            )

        return MessageResult(message)
