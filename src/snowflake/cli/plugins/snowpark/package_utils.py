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
    RequirementWithWheelAndDeps,
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


def download_packages(
    anaconda: AnacondaChannel | None,
    file_name: str | None,
    perform_anaconda_check: bool = True,
    package_name: str | None = None,
    index_url: str | None = None,
    allow_native_libraries: PypiOption = PypiOption.ASK,
    skip_version_check: bool = False,
) -> tuple[bool, SplitRequirements | None]:
    """
    Downloads packages from a requirements.txt file or a single package name,
    into a local folder named '.packages'.
    If perform_anaconda_check is set to True, packages available in Snowflake Anaconda
    channel will be omitted, otherwise all packages will be downloaded using pip.

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

    if perform_anaconda_check and not anaconda:
        raise ClickException(
            "Cannot perform anaconda checks if anaconda channel is not specified."
        )

    with Venv() as v, SecurePath.temporary_directory() as downloads_dir:
        if package_name:
            # This is a Windows workaround where use TemporaryDirectory instead of NamedTemporaryFile
            tmp_requirements = Path(v.directory.name) / "requirements.txt"
            tmp_requirements.write_text(str(package_name))
            file_name = str(tmp_requirements)

        pip_wheel_result = v.pip_wheel(
            file_name, download_dir=downloads_dir.path, index_url=index_url
        )
        if pip_wheel_result != 0:
            log.info(pip_failed_msg.format(pip_wheel_result))
            return False, None

        dependencies = v.get_package_dependencies(
            file_name, downloads_dir=downloads_dir.path
        )
        dependency_requirements = [d.requirement for d in dependencies]

        log.info(
            "Downloaded packages: %s",
            ",".join([d.name for d in dependency_requirements]),
        )

        if not perform_anaconda_check:
            split_requirements = SplitRequirements([], other=dependency_requirements)
        else:
            log.info("Checking for dependencies available in Anaconda...")
            assert anaconda is not None
            split_requirements = anaconda.parse_anaconda_packages(
                packages=dependency_requirements, skip_version_check=skip_version_check
            )
            if len(split_requirements.snowflake) > 0:
                log.info(second_chance_msg.format(split_requirements.snowflake))
            else:
                log.info("None of the package dependencies were found on Anaconda")
        dependencies_to_be_packed = _filter_dependencies_not_available_in_conda(
            dependencies, split_requirements.snowflake
        )

        log.info("Checking to see if packages have native libraries...")
        if _perform_native_libraries_check(
            dependencies_to_be_packed
        ) and not _confirm_native_libraries(allow_native_libraries):
            return False, split_requirements
        else:
            packages_dest = SecurePath(".packages")
            packages_dest.mkdir(exist_ok=True)
            for package in dependencies_to_be_packed:
                package.extract_files(packages_dest.path)

            return True, split_requirements


def _filter_dependencies_not_available_in_conda(
    dependencies: List[RequirementWithWheelAndDeps],
    available_in_conda: List[Requirement],
) -> List[RequirementWithWheelAndDeps]:
    in_conda = set(package.name for package in available_in_conda)
    return [dep for dep in dependencies if dep.requirement.name not in in_conda]


def get_snowflake_packages() -> List[str]:
    requirements_file = SecurePath("requirements.snowflake.txt")
    if requirements_file.exists():
        with requirements_file.open(
            "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding="utf-8"
        ) as f:
            return [req for line in f if (req := line.split("#")[0].strip())]
    else:
        return []


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


def _check_for_native_libraries(
    dependencies: List[RequirementWithWheelAndDeps],
) -> List[str]:
    return [
        dependency.requirement.name
        for dependency in dependencies
        if any(file.endswith(".so") for file in dependency.namelist())
    ]


def _perform_native_libraries_check(deps: List[RequirementWithWheelAndDeps]):
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
        "Following dependencies utilise shared libraries, not supported by Conda:"
    )
    log.error("\n".join(set(native_libraries)))
    log.error("You may still try to create your package, but it probably won't work")
    log.error("You may also request adding the package to Snowflake Conda channel")
    log.error("at https://support.anaconda.com/")


def _confirm_native_libraries(allow_native_libraries: PypiOption) -> bool:
    if allow_native_libraries == PypiOption.ASK:
        return click.confirm("Continue with package installation?", default=False)
    else:
        return allow_native_libraries == PypiOption.YES
