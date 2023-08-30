from __future__ import annotations
import logging
import os.path
import tempfile
from pathlib import Path


from requirements.requirement import Requirement
from shutil import rmtree

from snowcli import utils
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.cli.snowpark.package.utils import (
    LookupResult,
    InAnaconda,
    RequiresPackages,
    NotInAnaconda,
    NothingFound,
    CreatedSuccessfully,
    CreationError,
)
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
    conn = snow_cli_global_context_manager.get_connection()

    log.info(f"Uploading {file} to Snowflake @{stage}/{file}...")
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_app_zip_path = utils.prepare_app_zip(file, temp_dir)
        deploy_response = conn.upload_file_to_stage(
            file_path=temp_app_zip_path,
            destination_stage=stage,
            path="/",
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            overwrite=overwrite,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
        )

    message = (
        f"Package {file} {deploy_response.description[6]} to Snowflake @{stage}/{file}."
    )

    if deploy_response.description[6] == "SKIPPED":
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
