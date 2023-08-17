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


class PackageManager:
    @staticmethod
    def lookup(name: str, install_packages: bool) -> LookupResult:

        package_response = utils.parse_anaconda_packages([Requirement.parse(name)])

        if package_response.snowflake and not package_response.other:
            return InAnaconda(package_response)
        else:
            if install_packages:
                status, result = utils.install_packages(
                    perform_anaconda_check=True, package_name=name, file_name=None
                )

                if status:
                    if result.snowflake:
                        return RequiresPackages(result)
                    else:
                        return NotInAnaconda(result)

        return NothingFound(SplitRequirements([], []))

    def upload(self, file: Path, stage: str, overwrite: bool):
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
        log.info(
            f"Package {file} {deploy_response.description[6]} to Snowflake @{stage}/{file}."
        )
        if deploy_response.description[6] == "SKIPPED":
            log.info(
                "Package already exists on stage. Consider using --overwrite to overwrite the file."
            )

    def create(self, name: str):
        file_name = name + ".zip"
        if os.path.exists(".packages"):
            utils.recursive_zip_packages_dir(pack_dir=".packages", dest_zip=file_name)

        if os.path.exists(file_name):
            return CreatedSuccessfully(Path(file_name))
        else:
            return CreationError()

    @staticmethod
    def cleanup_after_install():
        if os.path.exists(".packages"):
            rmtree(".packages")

    @staticmethod
    def create_lookup_message(lookup_result: LookupResult, name: str):

        if type(lookup_result) == InAnaconda:
            return f"Package {name} is available on the Snowflake anaconda channel."
        elif type(lookup_result) == RequiresPackages:
            return f"""The package {name} is supported, but does depend on the
                    following Snowflake supported native libraries. You should
                    include the following in your packages: {lookup_result.requirements.snowflake}"""
        elif type(lookup_result) == NotInAnaconda:
            return f"""The package {name} is avaiable through PIP. You can create a zip using:\n
                    snow snowpark package create {name} -y"""
        else:
            return f"Lookup for package {name} resulted in some error. Please check the package name and try again"
