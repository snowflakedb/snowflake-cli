from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import click
import typer
from requirements.requirement import Requirement
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.models import PypiOption
from snowflake.cli.plugins.snowpark.zipper import zip_dir

PyPiDownloadOption: PypiOption = typer.Option(
    PypiOption.ASK.value, help="Whether to download non-Anaconda packages from PyPi."
)

PackageNativeLibrariesOption: PypiOption = typer.Option(
    PypiOption.NO.value,
    help="When using packages from PyPi, whether to allow native libraries.",
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
    artifact_file: Path,
    pypi_download: PypiOption,
    check_anaconda_for_pypi_deps: bool,
    package_native_libraries: PypiOption,
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
                if pypi_download == PypiOption.ASK
                else pypi_download == PypiOption.YES
            )
            if do_download:
                log.info("Installing non-Anaconda packages...")
                should_continue, second_chance_results = package_utils.install_packages(
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
                package_utils.deduplicate_and_sort_reqs(split_requirements.snowflake),
            )

    zip_dir(source=source, dest_zip=artifact_file)

    if Path(".packages").exists():
        zip_dir(source=Path(".packages"), dest_zip=artifact_file, mode="a")
    log.info("Deployment package now ready: %s", artifact_file)


def _write_requirements_file(file_name: str, requirements: List[Requirement]):
    log.info("Writing %s file", file_name)
    with open(file_name, "w", encoding="utf-8") as f:
        for req in requirements:
            f.write(f"{req.line}\n")
