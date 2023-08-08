from __future__ import annotations
import logging
import os.path
import tempfile
from pathlib import Path


from requirements.requirement import Requirement
from shutil import rmtree

from snowcli import utils
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.utils import SplitRequirements

log = logging.getLogger(__name__)


class PackageManager:
    def lookup(
        self, name: str, install_packages: bool, _run_nested: bool = False
    ) -> str:

        package_response = utils.parse_anaconda_packages([Requirement.parse(name)])

        if package_response.snowflake:
            return f"Package {name} is available on the Snowflake Anaconda channel. You can just include it in your 'packages' declaration"
        else:
            if install_packages:
                status, result = utils.install_packages(
                    perform_anaconda_check=True, package_name=name, file_name=None
                )
                self._cleanup_after_install(_run_nested)
                return self._determine_lookup_result(status, result, name)
        return ""

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

    def create(self, name: str, install_packages):
        if os.path.exists(".packages"):

            utils.recursive_zip_packages_dir(".packages", name + ".zip")
            self._cleanup_after_install(False)
        return f"Package {name}.zip created. You can now upload it to a stage (`snow package upload -f {name}.zip -s packages`) and reference it in your procedure or function."

    @staticmethod
    def _determine_lookup_result(
        status: bool, reqs: SplitRequirements, name: str
    ) -> str:
        if status and reqs and reqs.snowflake:
            return f"""
                The package {name} is supported, but does depend on the "
                following Snowflake supported native libraries. You should "
                include the following in your packages: {reqs.snowflake}"
                """
        return ""

    @staticmethod
    def _cleanup_after_install(run_nested: bool):
        if not run_nested and os.path.exists(".packages"):
            rmtree(".packages")
