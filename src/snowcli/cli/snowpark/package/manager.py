from __future__ import annotations
import logging
import os.path
import tempfile
from pathlib import Path


from requirements.requirement import Requirement
from shutil import rmtree

from snowcli import utils
from snowcli.cli.snowpark.package.utils import (
    LookupResult,
    InAnaconda,
    RequiresPackages,
    NotInAnaconda,
    NothingFound,
    CreatedSuccessfully,
    CreationError,
)
from snowcli.cli.stage.manager import StageManager
from snowcli.utils import SplitRequirements

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


def create(name: str):
    file_name = name + ".zip"
    if os.path.exists(".packages"):
        utils.recursive_zip_packages_dir(pack_dir=".packages", dest_zip=file_name)

    if os.path.exists(file_name):
        return CreatedSuccessfully(name, Path(file_name))
    else:
        return CreationError(name)


def cleanup_after_install():
    if os.path.exists(".packages"):
        rmtree(".packages")
