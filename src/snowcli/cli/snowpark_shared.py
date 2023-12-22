from __future__ import annotations

import logging
import os
from pathlib import Path

import click
import snowcli.utils.package_utils
import typer
from snowcli.utils import utils
from snowcli.utils.package_utils import PypiOption
from snowcli.utils.zipper import zip_dir

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


def snowpark_package(
    source: Path,
    artefact_file: Path,
    pypi_download: PypiOption,
    check_anaconda_for_pypi_deps: bool,
    package_native_libraries: PypiOption,
):
    log.info("Resolving any requirements from requirements.txt...")
    requirements = snowcli.utils.package_utils.parse_requirements()
    if requirements:
        log.info("Comparing provided packages from Snowflake Anaconda...")
        split_requirements = snowcli.utils.package_utils.parse_anaconda_packages(
            requirements
        )
        if not split_requirements.other:
            log.info("No packages to manually resolve")
        if split_requirements.other:
            log.info("Writing requirements.other.txt...")
            with open("requirements.other.txt", "w", encoding="utf-8") as f:
                for package in split_requirements.other:
                    f.write(package.line + "\n")
        # if requirements.other.txt exists
        if os.path.isfile("requirements.other.txt"):
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
                (
                    should_pack,
                    second_chance_results,
                ) = snowcli.utils.package_utils.install_packages(
                    "requirements.other.txt",
                    check_anaconda_for_pypi_deps,
                    package_native_libraries,
                )
                if should_pack:
                    # add the Anaconda packages discovered as dependencies
                    if second_chance_results is not None:
                        split_requirements.snowflake = (
                            split_requirements.snowflake
                            + second_chance_results.snowflake
                        )

        # write requirements.snowflake.txt file
        if split_requirements.snowflake:
            log.info("Writing requirements.snowflake.txt file...")
            with open(
                "requirements.snowflake.txt",
                "w",
                encoding="utf-8",
            ) as f:
                for package in snowcli.utils.package_utils.deduplicate_and_sort_reqs(
                    split_requirements.snowflake
                ):
                    f.write(package.line + "\n")

    zip_dir(source=source, dest_zip=artefact_file)
    log.info("Deployment package now ready: app.zip")
