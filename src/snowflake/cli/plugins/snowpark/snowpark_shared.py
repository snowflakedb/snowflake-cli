from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import click
import typer
from click import UsageError
from requirements.requirement import Requirement
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.models import YesNoAsk
from snowflake.cli.plugins.snowpark.zipper import zip_dir

PyPiDownloadOption: YesNoAsk = typer.Option(
    YesNoAsk.ASK.value, help="Whether to download non-Anaconda packages from PyPi."
)

PackageNativeLibrariesOption: YesNoAsk = typer.Option(
    YesNoAsk.NO.value,
    help="Allows native libraries, when using packages installed through PIP",
)

CheckAnacondaForPyPiDependencies: bool = typer.Option(
    True,
    "--check-anaconda-for-pypi-deps/--no-check-anaconda-for-pypi-deps",
    "-a",
    help="""Checks if any of missing Anaconda packages dependencies can be imported directly from Anaconda. Valid values include: `true`, `false`, Default: `true`.""",
)

ReturnsOption = typer.Option(
    ...,
    "--returns",
    "-r",
    help="Data type for the procedure to return.",
)

OverwriteOption = typer.Option(
    False,
    "--overwrite",
    "-o",
    help="Replaces an existing procedure with this one.",
)

log = logging.getLogger(__name__)

REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"
REQUIREMENTS_OTHER = "requirements.other.txt"


def snowpark_package(
    source: Path,
    artifact_file: Path,
    pypi_download: YesNoAsk,
    check_anaconda_for_pypi_deps: bool,
    package_native_libraries: YesNoAsk,
):
    log.info("Resolving any requirements from requirements.txt...")
    requirements = package_utils.parse_requirements()
    if requirements:
        log.info("Comparing provided packages from Snowflake Anaconda...")
        split_requirements = package_utils.parse_anaconda_packages(requirements)
        if not split_requirements.other:
            log.info("No packages to manually resolve")
        else:
            _write_requirements_file(REQUIREMENTS_OTHER, split_requirements.other)
            do_download = (
                click.confirm(
                    "Do you want to try to download non-Anaconda packages?",
                    default=True,
                )
                if pypi_download == YesNoAsk.ASK
                else pypi_download == YesNoAsk.YES
            )
            if do_download:
                log.info("Installing non-Anaconda packages...")
                (
                    requires_native_libs,
                    second_chance_results,
                ) = package_utils.install_packages(
                    REQUIREMENTS_OTHER,
                    check_anaconda_for_pypi_deps,
                )
                if requires_native_libs:
                    check_if_can_continue_with_native_libs(package_native_libraries)
                # add the Anaconda packages discovered as dependencies
                if requires_native_libs and second_chance_results:
                    split_requirements.snowflake = (
                        split_requirements.snowflake + second_chance_results.snowflake
                    )

        # write requirements.snowflake.txt file
        if split_requirements.snowflake:
            _write_requirements_file(
                REQUIREMENTS_SNOWFLAKE,
                package_utils.deduplicate_and_sort_reqs(split_requirements.snowflake),
            )

    zip_dir(source=source, dest_zip=artifact_file)

    if Path(".packages").exists():
        zip_dir(source=Path(".packages"), dest_zip=artifact_file, mode="a")
    log.info("Deployment package now ready: %s", artifact_file)


def check_if_can_continue_with_native_libs(package_native_libraries: YesNoAsk):
    base_warning = "One or many packages may include native libraries. Such libraries may not work when uploaded to Snowpark."
    if package_native_libraries == YesNoAsk.ASK:
        continue_installation = typer.confirm(
            f"{base_warning} Do you want continue anyway?"
        )
    else:
        continue_installation = package_native_libraries == YesNoAsk.YES
    if continue_installation:
        cli_console.warning(base_warning)
        return
    raise UsageError(
        "Requested packages require native libraries. Consider enabling them using flag."
    )


def _write_requirements_file(file_name: str, requirements: List[Requirement]):
    log.info("Writing %s file", file_name)
    with SecurePath(file_name).open("w", encoding="utf-8") as f:
        for req in requirements:
            f.write(f"{req.line}\n")
