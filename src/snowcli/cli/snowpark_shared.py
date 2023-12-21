from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List

import click
import typer
from requirements.requirement import Requirement
from snowcli import utils
from snowcli.utils import (
    YesNoAskOptionsType,
    yes_no_ask_callback,
)
from snowcli.zipper import zip_dir

PyPiDownloadOption = typer.Option(
    "ask",
    help="Whether to download non-Anaconda packages from PyPi. Valid values include: `yes`, `no`, `ask`. Default: `no`.",
    callback=yes_no_ask_callback,
)

PackageNativeLibrariesOption = typer.Option(
    "ask",
    help="When using packages from PyPi, whether to allow native libraries. Valid values include: `yes`, `no`, `ask`. Default: `no`.",
    callback=yes_no_ask_callback,
)

CheckAnacondaForPyPiDependencies: bool = typer.Option(
    True,
    "--check-anaconda-for-pypi-deps/--no-check-anaconda-for-pypi-deps",
    "-a",
    help="""Whether to check if any of missing Anaconda packages dependencies can be imported directly from Anaconda. Valid values include: `true`, `false`, Default: `true`.""",
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
    help="Whether to replace an existing procedure with this one.",
)

log = logging.getLogger(__name__)

REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"
REQUIREMENTS_OTHER = "requirements.other.txt"


def snowpark_package(
    source: Path,
    artefact_file: Path,
    pypi_download: YesNoAskOptionsType,
    check_anaconda_for_pypi_deps: bool,
    package_native_libraries: YesNoAskOptionsType,
):
    log.info("Resolving any requirements from requirements.txt...")
    requirements = utils.parse_requirements()
    if requirements:
        log.info("Comparing provided packages from Snowflake Anaconda...")
        split_requirements = utils.parse_anaconda_packages(requirements)
        if not split_requirements.other:
            log.info("No packages to manually resolve")
        else:
            _write_requirements_file(REQUIREMENTS_OTHER, split_requirements.other)
            do_download = (
                click.confirm(
                    "Do you want to try to download non-Anaconda packages?",
                    default=True,
                )
                if pypi_download == "ask"
                else pypi_download == "yes"
            )
            if do_download:
                log.info("Installing non-Anaconda packages...")
                should_continue, second_chance_results = utils.install_packages(
                    REQUIREMENTS_OTHER,
                    check_anaconda_for_pypi_deps,
                    package_native_libraries,
                )
                # add the Anaconda packages discovered as dependencies
                if should_continue and second_chance_results:
                    split_requirements.snowflake = (
                        split_requirements.snowflake + second_chance_results.snowflake
                    )

        # write requirements.snowflake.txt file
        if split_requirements.snowflake:
            _write_requirements_file(
                REQUIREMENTS_SNOWFLAKE,
                utils.deduplicate_and_sort_reqs(split_requirements.snowflake),
            )

    zip_dir(source=source, dest_zip=artefact_file)

    if Path(".packages").exists():
        zip_dir(source=Path(".packages"), dest_zip=artefact_file, mode="a")
    log.info(f"Deployment package now ready: %s", artefact_file)


def _write_requirements_file(file_name: str, requirements: List[Requirement]):
    log.info(f"Writing %s file", file_name)
    with open(file_name, "w", encoding="utf-8") as f:
        for req in requirements:
            f.write(f"{req.line}\n")
