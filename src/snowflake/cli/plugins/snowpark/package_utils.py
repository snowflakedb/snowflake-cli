from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List

import click
import requirements
from click import ClickException
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.models import (
    PypiOption,
    Requirement,
    RequirementWithFilesAndDeps,
    SplitRequirements,
    pip_failed_msg,
    second_chance_msg,
)
from snowflake.cli.plugins.snowpark.package.anaconda import AnacondaChannel
from snowflake.cli.plugins.snowpark.venv import Venv

log = logging.getLogger(__name__)

PIP_PATH = os.environ.get("SNOWCLI_PIP_PATH", "pip")


def parse_requirements(
    requirements_file: str = "requirements.txt",
) -> List[Requirement]:
    """Reads and parses a Python requirements.txt file.

    Args:
        requirements_file (str, optional): The name of the file.
        Defaults to 'requirements.txt'.

    Returns:
        list[str]: A flat list of package names, without versions
    """
    reqs: List[Requirement] = []
    requirements_file_spath = SecurePath(requirements_file)
    if requirements_file_spath.exists():
        with requirements_file_spath.open(
            "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"
        ) as f:
            for req in requirements.parse(f):
                reqs.append(req)
    else:
        log.info("No %s found", requirements_file)

    return deduplicate_and_sort_reqs(reqs)


def deduplicate_and_sort_reqs(
    packages: List[Requirement],
) -> List[Requirement]:
    """
    Deduplicates a list of requirements, keeping the first occurrence of each package.
    """
    seen = set()
    deduped: List[RequirementWithFilesAndDeps] = []
    for package in packages:
        if package.name not in seen:
            deduped.append(package)
            seen.add(package.name)
    # sort by package name
    deduped.sort(key=lambda x: x.name)
    return deduped


def install_packages(
    anaconda: AnacondaChannel,
    file_name: str | None,
    perform_anaconda_check: bool = True,
    package_name: str | None = None,
    allow_native_libraries: PypiOption = PypiOption.ASK,
) -> tuple[bool, SplitRequirements | None]:
    """
    Install packages from a requirements.txt file or a single package name,
    into a local folder named '.packages'.
    If a requirements.txt file is provided, they will be installed using pip.
    If a package name is provided, it will be installed using pip.
    Returns a tuple of:
    1) a boolean indicating whether the installation was successful
    2) a SplitRequirements object containing any installed dependencies
    which are available on the Snowflake Anaconda channel. These will have
    been deleted from the local packages folder.
    """
    if file_name and package_name:
        raise ClickException(
            "Could not use package name and requirements file simultaneously"
        )

    if file_name and not Path(file_name).exists():
        raise ClickException(f"File {file_name} does not exists.")

    with Venv() as v:
        if package_name:
            # This is a Windows workaround where use TemporaryDirectory instead of NamedTemporaryFile
            tmp_requirements = Path(v.directory.name) / "requirements.txt"
            tmp_requirements.write_text(str(package_name))
            file_name = str(tmp_requirements)

        pip_install_result = v.pip_install(file_name)
        dependencies = v.get_package_dependencies(file_name)

        if pip_install_result != 0:
            log.info(pip_failed_msg.format(pip_install_result))
            return False, None

        if perform_anaconda_check:
            log.info("Checking for dependencies available in Anaconda...")
            dependency_requirements = [dep.requirement for dep in dependencies]
            log.info(
                "Downloaded packages: %s",
                ",".join([d.name for d in dependency_requirements]),
            )
            second_chance_results: SplitRequirements = anaconda.parse_anaconda_packages(
                packages=dependency_requirements
            )

            if len(second_chance_results.snowflake) > 0:
                log.info(second_chance_msg.format(second_chance_results.snowflake))
            else:
                log.info("None of the package dependencies were found on Anaconda")

        dependencies_to_be_packed = _get_dependencies_not_avaiable_in_conda(
            dependencies,
            second_chance_results.snowflake if second_chance_results else None,
        )

        log.info("Checking to see if packages have native libraries...")

        if _perform_native_libraries_check(
            dependencies_to_be_packed
        ) and not _confirm_native_libraries(allow_native_libraries):
            return False, second_chance_results
        else:
            v.copy_files_to_packages_dir(
                [Path(file) for dep in dependencies_to_be_packed for file in dep.files]
            )
            return True, second_chance_results


def _check_for_native_libraries(
    dependencies: List[RequirementWithFilesAndDeps],
) -> List[str]:
    return [
        dependency.requirement.name
        for dependency in dependencies
        for file in dependency.files
        if file.endswith(".so")
    ]


def get_snowflake_packages() -> List[str]:
    requirements_file = SecurePath("requirements.snowflake.txt")
    if requirements_file.exists():
        with requirements_file.open(
            "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"
        ) as f:
            return [req for line in f if (req := line.split("#")[0].strip())]
    else:
        return []


def _get_dependencies_not_avaiable_in_conda(
    dependencies: List[RequirementWithFilesAndDeps],
    avaiable_in_conda: List[Requirement],
) -> List[RequirementWithFilesAndDeps]:
    return [
        dep
        for dep in dependencies
        if dep.requirement.name not in [package.name for package in avaiable_in_conda]
    ]


def generate_deploy_stage_name(identifier: str) -> str:
    return (
        identifier.replace("()", "")
        .replace(
            "(",
            "_",
        )
        .replace(
            ")",
            "",
        )
        .replace(
            " ",
            "_",
        )
        .replace(
            ",",
            "",
        )
    )


def _perform_native_libraries_check(deps: List[RequirementWithFilesAndDeps]):
    if native_libraries := _check_for_native_libraries(deps):
        _log_native_libraries(native_libraries)
        return True
    else:
        log.info("Unsupported native libraries not found in packages (Good news!)...")
        return False


def _log_native_libraries(
    native_libraries: List[str],
) -> None:
    log.error(
        "Following dependencies utilise native libraries, not supported by Conda:"
    )
    log.error("\n".join(set(native_libraries)))
    log.error("You may still try to create your package, but it probably won`t work")
    log.error("You may also request adding the package to Snowflake Conda channel")
    log.error("at https://support.anaconda.com/")


def _confirm_native_libraries(allow_native_libraries: PypiOption) -> bool:
    if allow_native_libraries == PypiOption.ASK:
        return click.confirm("Continue with package installation?", default=False)
    else:
        return allow_native_libraries == PypiOption.YES
