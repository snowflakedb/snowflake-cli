from __future__ import annotations

import os

import click
import logging
import typer

from snowcli import utils
from snowcli.utils import (
    YesNoAskOptionsType,
    yes_no_ask_callback,
)

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
CheckAnacondaForPyPiDependancies: bool = typer.Option(
    True,
    "--check-anaconda-for-pypi-deps/--no-check-anaconda-for-pypi-deps",
    "-a",
    help="""Whether to check if any of missing Anaconda packages dependencies can be imported directly from Anaconda. Valid values include: `true`, `false`, Default: `true`.""",
)
log = logging.getLogger(__name__)


def snowpark_package(
    pypi_download: YesNoAskOptionsType,
    check_anaconda_for_pypi_deps: bool,
    package_native_libraries: YesNoAskOptionsType,
):
    log.info("Resolving any requirements from requirements.txt...")
    requirements = utils.parse_requirements()
    pack_dir: str = None  # type: ignore
    if requirements:
        log.info("Comparing provided packages from Snowflake Anaconda...")
        split_requirements = utils.parse_anaconda_packages(requirements)
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
                if pypi_download == "ask"
                else pypi_download == "yes"
            )
            if do_download:
                log.info("Installing non-Anaconda packages...")
                should_pack, second_chance_results = utils.install_packages(
                    "requirements.other.txt",
                    check_anaconda_for_pypi_deps,
                    package_native_libraries,
                )
                if should_pack:
                    pack_dir = ".packages"
                    # add the Anaconda packages discovered as dependancies
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
                for package in utils.deduplicate_and_sort_reqs(
                    split_requirements.snowflake
                ):
                    f.write(package.line + "\n")
        if pack_dir:
            utils.recursive_zip_packages_dir(pack_dir, "app.zip")
        else:
            utils.standard_zip_dir("app.zip")
    else:
        utils.standard_zip_dir("app.zip")
    log.info("Deployment package now ready: app.zip")
