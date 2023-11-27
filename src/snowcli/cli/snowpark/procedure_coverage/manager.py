import logging
import os
import tempfile
from enum import Enum
from pathlib import Path

import coverage
import snowflake
import typer
from click import ClickException
from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.object.stage.manager import StageManager
from snowcli.utils import generate_deploy_stage_name
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


class ReportOutputOptions(str, Enum):
    html = "html"
    json = "json"
    lcov = "lcov"


class UnknownOutputFormatError(ClickException):
    def __init__(self, output_format: ReportOutputOptions):
        super().__init__(f"Unknown output format '{output_format}'")


class ProcedureCoverageManager(SqlExecutionMixin):
    def report(
        self,
        identifier: str,
        output_format: ReportOutputOptions,
        store_as_comment: bool,
        artefact_name: str,
        app_stage_path: str,
    ) -> str:
        coverage_file = ".coverage"
        orig_get_python_source = coverage.python.get_python_source

        def new_get_python_source(filename: str):
            file_path = Path(filename)
            parts = file_path.parts
            new_path = Path(artefact_name) / "/".join(
                parts[parts.index(artefact_name) + 1 :]
            )
            return orig_get_python_source(str(new_path))

        coverage.python.get_python_source = new_get_python_source

        combined_coverage = coverage.Coverage(data_file=coverage_file)
        report_files = f"{app_stage_path}/coverage/"

        with tempfile.TemporaryDirectory() as temp_dir:
            results = []
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
            log.info(f"Combining data from {len(results)} reports")
            combined_coverage.combine(
                # the tuple contains the columns: (file, size, status, message)
                data_paths=[
                    os.path.join(temp_dir, os.path.basename(result[0]))
                    for result in results
                ]
            )

            coverage_reports = {
                ReportOutputOptions.html: (
                    combined_coverage.html_report,
                    "Your HTML code coverage report is now available in 'htmlcov/index.html'.",
                ),
                ReportOutputOptions.json: (
                    combined_coverage.json_report,
                    "Your JSON code coverage report is now available in 'coverage.json'.",
                ),
                ReportOutputOptions.lcov: (
                    combined_coverage.lcov_report,
                    "Your lcov code coverage report is now available in 'coverage.lcov'.",
                ),
            }
            report_function, message = coverage_reports.get(output_format, (None, None))
            if not (report_function and message):
                raise UnknownOutputFormatError(output_format)
            coverage_percentage = report_function()

            if store_as_comment:
                log.info(
                    f"Storing total coverage value of {str(coverage_percentage)} as a procedure comment."
                )
                self._execute_query(
                    f"ALTER PROCEDURE {identifier} SET COMMENT = $${str(coverage_percentage)}$$"
                )
            return message

    def clear(self, app_stage_path: str) -> SnowflakeCursor:
        cursor = StageManager().remove(stage_name=f"{app_stage_path}/coverage", path="")
        return cursor
