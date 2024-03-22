from __future__ import annotations

import logging
from typing import List

import click
import typer
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.models import PypiOption, Requirement
from snowflake.cli.plugins.snowpark.package.anaconda import AnacondaChannel
from snowflake.cli.plugins.snowpark.snowpark_package_paths import SnowparkPackagePaths
from snowflake.cli.plugins.snowpark.zipper import zip_dir

PyPiDownloadOption: PypiOption = typer.Option(
    PypiOption.ASK.value, help="Whether to download non-Anaconda packages from PyPi."
)

PackageNativeLibrariesOption: PypiOption = typer.Option(
    PypiOption.NO.value,
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


def snowpark_package(
    paths: SnowparkPackagePaths,
    pypi_download: PypiOption,
    check_anaconda_for_pypi_deps: bool,
    package_native_libraries: PypiOption,
):
    log.info("Resolving any requirements from requirements.txt...")
    requirements = package_utils.parse_requirements(
        requirements_file=paths.defined_requirements_file
    )
    if requirements:
        anaconda = AnacondaChannel.from_snowflake()
        log.info("Comparing provided packages from Snowflake Anaconda...")
        split_requirements = anaconda.parse_anaconda_packages(packages=requirements)
        if not split_requirements.other:
            log.info("No packages to manually resolve")
        else:
            _write_requirements_file(
                paths.other_requirements_file, split_requirements.other
            )
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
                    anaconda=anaconda,
                    requirements_file=paths.other_requirements_file,
                    packages_dir=paths.downloaded_packages_dir,
                    perform_anaconda_check=check_anaconda_for_pypi_deps,
                    allow_native_libraries=package_native_libraries,
                )
                # add the Anaconda packages discovered as dependencies
                if should_continue and second_chance_results:
                    split_requirements.snowflake = (
                        split_requirements.snowflake + second_chance_results.snowflake
                    )

        # write requirements.snowflake.txt file
        if split_requirements.snowflake:
            _write_requirements_file(
                paths.snowflake_requirements_file,
                package_utils.deduplicate_and_sort_reqs(split_requirements.snowflake),
            )

    zip_dir(source=paths.source.path, dest_zip=paths.artifact_file.path)

    if paths.downloaded_packages_dir.exists():
        zip_dir(
            source=paths.downloaded_packages_dir.path,
            dest_zip=paths.artifact_file.path,
            mode="a",
        )
    log.info("Deployment package now ready: %s", paths.artifact_file.path)


def _write_requirements_file(file_path: SecurePath, requirements: List[Requirement]):
    log.info("Writing %s file", file_path.path)
    with file_path.open("w", encoding="utf-8") as f:
        for req in requirements:
            f.write(f"{req.line}\n")
