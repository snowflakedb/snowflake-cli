import logging
import os
import tempfile
from enum import Enum
from pathlib import Path

import coverage
import snowflake
import typer
from snowflake.connector.cursor import SnowflakeCursor

from snowcli import utils
from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.stage.manager import StageManager
from snowcli.snow_connector import generate_signature_from_params
from snowcli.utils import generate_deploy_stage_name

log = logging.getLogger(__name__)


class ReportOutputOptions(str, Enum):
    html = "html"
    json = "json"
    lcov = "lcov"


class ProcedureCoverageManager(SqlExecutionMixin):
    def report(
        self,
        name: str,
        input_parameters: str,
        output_format: ReportOutputOptions,
        store_as_comment: bool,
    ) -> None:
        conn = snow_cli_global_context_manager.get_connection()
        deploy_dict = utils.get_deploy_names(
            conn.ctx.database,
            conn.ctx.schema,
            generate_deploy_stage_name(
                name,
                input_parameters,
            ),
        )
        coverage_file = ".coverage"
        # the coverage databases will include paths like "/home/udf/132735964617982/app.zip/app.py", where they ran on Snowflake
        # we need to strip out the prefix up to and including "app.zip/" so that it reads them from the local folder
        # tried to do this by modifying the report data, but couldn't seem to figure it out
        # instead we'll monkey-patch the source code reader and strip it out
        orig_get_python_source = coverage.python.get_python_source

        def new_get_python_source(filename: str):
            new_filename = filename[filename.index("app.zip/") + len("app.zip/") :]
            return orig_get_python_source(new_filename)

        coverage.python.get_python_source = new_get_python_source

        combined_coverage = coverage.Coverage(data_file=coverage_file)
        with tempfile.TemporaryDirectory() as temp_dir:
            stage_name = deploy_dict["stage"]
            stage_path = deploy_dict["directory"]
            report_files = f"{stage_name}{stage_path}/coverage/"
            try:
                results = (
                    StageManager()
                    .get(stage_name=report_files, dest_path=Path(temp_dir))
                    .fetchall()
                )
            except snowflake.connector.errors.DatabaseError as database_error:
                if database_error.errno == 253006:
                    results = []
            if len(results) == 0:
                log.error(
                    "No code coverage reports were found on the stage. "
                    "Please ensure that you've invoked the procedure at least once "
                    "and that you provided the correct inputs"
                )
                raise typer.Abort()
            else:
                log.info(f"Combining data from {len(results)} reports")
            combined_coverage.combine(
                # the tuple contains the columns: | file ┃ size ┃ status ┃ message |
                data_paths=[
                    os.path.join(temp_dir, os.path.basename(result[0]))
                    for result in results
                ]
            )
            if output_format == ReportOutputOptions.html:
                coverage_percentage = combined_coverage.html_report()
                log.info(
                    "Your HTML code coverage report is now available under the 'htmlcov' folder (htmlcov/index.html)."
                )
            elif output_format == ReportOutputOptions.json:
                coverage_percentage = combined_coverage.json_report()
                log.info(
                    "Your JSON code coverage report is now available in 'coverage.json'."
                )
            elif output_format == ReportOutputOptions.lcov:
                coverage_percentage = combined_coverage.lcov_report()
                log.info(
                    "Your lcov code coverage report is now available in 'coverage.lcov'."
                )
            else:
                log.error(f"Unknown output format '{output_format}'")

            if store_as_comment:
                log.info(
                    f"Storing total coverage value of {str(coverage_percentage)} as a procedure comment."
                )
                signature = name + generate_signature_from_params(input_parameters)
                self._execute_query(
                    f"ALTER PROCEDURE {signature} SET COMMENT = $${str(coverage_percentage)}$$"
                )

    def clear(self, name: str, input_parameters: str) -> SnowflakeCursor:
        conn = snow_cli_global_context_manager.get_connection()
        deploy_dict = utils.get_deploy_names(
            conn.ctx.database,
            conn.ctx.schema,
            generate_deploy_stage_name(
                name,
                input_parameters,
            ),
        )
        coverage_path = f"""{deploy_dict["directory"]}/coverage"""
        cursor = StageManager().remove(
            stage_name=deploy_dict["stage"], path=coverage_path
        )
        log.info("Deleted the following coverage results from the stage:")
        return cursor
