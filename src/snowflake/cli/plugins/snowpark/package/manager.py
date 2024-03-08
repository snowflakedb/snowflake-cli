from __future__ import annotations

import logging
from functools import wraps
from pathlib import Path

from requirements.requirement import Requirement
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import PACKAGES_DIR
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.package.utils import (
    InAnaconda,
    LookupResult,
    NotInAnaconda,
    NotInAnacondaButRequiresNativePackages,
    prepare_app_zip,
)
from snowflake.cli.plugins.snowpark.package_utils import check_for_native_libraries
from snowflake.cli.plugins.snowpark.zipper import zip_dir

log = logging.getLogger(__name__)


def check_if_package_in_anaconda(package_name):
    cli_console.step("Checking package availability in Snowflake Anaconda")
    split_requirements = package_utils.parse_anaconda_packages(
        [Requirement.parse(package_name)]
    )
    available_in_anaconda = (
        split_requirements.snowflake and not split_requirements.other
    )
    return available_in_anaconda, split_requirements


def lookup(package_name: str, install_packages: bool) -> LookupResult:
    available_in_anaconda, split_requirements = check_if_package_in_anaconda(
        package_name
    )
    # If all packages are in snowflake then there's no need for user action
    if available_in_anaconda:
        return InAnaconda(requirements=split_requirements, name=package_name)

    # There are missing packages, but we are not allowed to install them
    if split_requirements.other and not install_packages:
        return NotInAnaconda(requirements=split_requirements, name=package_name)

    split_requirements = package_utils.install_packages(
        perform_anaconda_check=True, package_name=package_name, file_name=None
    )
    if check_for_native_libraries():
        return NotInAnacondaButRequiresNativePackages(split_requirements, package_name)

    return NotInAnaconda(split_requirements, package_name)


def upload(file: Path, stage: str, overwrite: bool):
    log.info("Uploading %s to Snowflake @%s/%s...", file, stage, file)
    with SecurePath.temporary_directory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(SecurePath(file), temp_dir)
        sm = StageManager()

        sm.create(sm.get_stage_name_from_path(stage))
        put_response = sm.put(
            temp_app_zip_path.path, stage, overwrite=overwrite
        ).fetchone()

    if put_response[6] == "SKIPPED":
        return "Package already exists on stage. Consider using --overwrite to overwrite the file."

    return f"Package {file} {put_response[6]} to Snowflake. Add '@{stage}/{file}' to imports of your function or procedure."


def create_package(zip_name: str):
    file_name = zip_name if zip_name.endswith(".zip") else f"{zip_name}.zip"
    file_path = Path(file_name)
    cli_console.step(f"Creating {file_path}")
    zip_dir(dest_zip=file_path, source=Path.cwd() / ".packages")
    return file_path


def cleanup_after_install(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        finally:
            if PACKAGES_DIR.exists():
                SecurePath(PACKAGES_DIR).rmdir(recursive=True)

    return wrapper
