from __future__ import annotations

import logging
import os.path
import tempfile
from pathlib import Path
from shutil import rmtree

from requirements.requirement import Requirement
from snowcli import utils
from snowcli.cli.constants import PACKAGES_DIR
from snowcli.cli.object.stage.manager import StageManager
from snowcli.cli.snowpark.package.utils import (
    CreatedSuccessfully,
    InAnaconda,
    LookupResult,
    NothingFound,
    NotInAnaconda,
    RequiresPackages,
)
from snowcli.utils import SplitRequirements
from snowcli.zipper import zip_dir

log = logging.getLogger(__name__)


def lookup(name: str, install_packages: bool) -> LookupResult:

    package_response = utils.parse_anaconda_packages([Requirement.parse(name)])

    if package_response.snowflake and not package_response.other:
        return InAnaconda(package_response, name)
    elif install_packages:
        status, result = utils.install_packages(
            perform_anaconda_check=True, package_name=name, file_name=None
        )

        if status:
            if result.snowflake:
                return RequiresPackages(result, name)
            else:
                return NotInAnaconda(result, name)

    return NothingFound(SplitRequirements([], []), name)


def upload(file: Path, stage: str, overwrite: bool):
    log.info(f"Uploading {file} to Snowflake @{stage}/{file}...")
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_app_zip_path = utils.prepare_app_zip(file, temp_dir)
        sm = StageManager()
        sm.create(stage)
        put_response = sm.put(temp_app_zip_path, stage, overwrite=overwrite).fetchone()

    message = f"Package {file} {put_response[6]} to Snowflake @{stage}/{file}."

    if put_response[6] == "SKIPPED":
        message = "Package already exists on stage. Consider using --overwrite to overwrite the file."

    return message


def create(zip_name: str):
    file_name = zip_name if zip_name.endswith(".zip") else f"{zip_name}.zip"
    zip_dir(dest_zip=Path(file_name), source=Path.cwd())

    if os.path.exists(file_name):
        return CreatedSuccessfully(zip_name, Path(file_name))


def cleanup_after_install():
    if PACKAGES_DIR.exists():
        rmtree(PACKAGES_DIR)
