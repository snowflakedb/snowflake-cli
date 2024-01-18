from __future__ import annotations

import logging
import os.path
import tempfile
from pathlib import Path
from shutil import rmtree

from requirements.requirement import Requirement
from snowflake.cli.api.constants import PACKAGES_DIR
from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.models import SplitRequirements
from snowflake.cli.plugins.snowpark.package.utils import (
    CreatedSuccessfully,
    InAnaconda,
    LookupResult,
    NothingFound,
    NotInAnaconda,
    RequiresPackages,
    prepare_app_zip,
)
from snowflake.cli.plugins.snowpark.zipper import zip_dir

log = logging.getLogger(__name__)


def lookup(name: str, install_packages: bool) -> LookupResult:

    package_response = package_utils.parse_anaconda_packages([Requirement.parse(name)])

    if package_response.snowflake and not package_response.other:
        return InAnaconda(package_response, name)
    elif install_packages:
        status, result = package_utils.install_packages(
            perform_anaconda_check=True, package_name=name, file_name=None
        )

        if status:
            if result.snowflake:
                return RequiresPackages(result, name)
            else:
                return NotInAnaconda(result, name)

    return NothingFound(SplitRequirements([], []), name)


def upload(file: Path, stage: str, overwrite: bool):
    log.info("Uploading %s to Snowflake @%s/%s...", file, stage, file)
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(file, temp_dir)
        sm = StageManager()
        sm.create(stage)
        put_response = sm.put(temp_app_zip_path, stage, overwrite=overwrite).fetchone()

    message = f"Package {file} {put_response[6]} to Snowflake @{stage}/{file}."

    if put_response[6] == "SKIPPED":
        message = "Package already exists on stage. Consider using --overwrite to overwrite the file."

    return message


def create(zip_name: str):
    file_name = zip_name if zip_name.endswith(".zip") else f"{zip_name}.zip"
    zip_dir(dest_zip=Path(file_name), source=Path.cwd() / ".packages")

    if os.path.exists(file_name):
        return CreatedSuccessfully(zip_name, Path(file_name))


def cleanup_after_install():
    if PACKAGES_DIR.exists():
        rmtree(PACKAGES_DIR)
