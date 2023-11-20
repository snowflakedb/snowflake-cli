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
    ) -> str:
        conn = self._conn
        deploy_dict = get_deploy_names(
            conn.database,
            conn.schema,
            generate_deploy_stage_name(identifier),
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
        stage_name = deploy_dict["stage"]
        stage_path = deploy_dict["directory"]
        report_files = f"{stage_name}{stage_path}/coverage/"

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
                # the tuple contains the columns: | file ┃ size ┃ status ┃ message |
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

    def clear(self, identifier: str) -> SnowflakeCursor:
        conn = self._conn
        deploy_dict = get_deploy_names(
            conn.database,
            conn.schema,
            generate_deploy_stage_name(identifier),
        )
        coverage_path = f"""{deploy_dict["directory"]}/coverage"""
        cursor = StageManager().remove(
            stage_name=deploy_dict["stage"], path=coverage_path
        )
        log.info("Deleted the following coverage results from the stage:")
        return cursor


def get_deploy_names(database, schema, name) -> dict:
    stage = f"{database}.{schema}.deployments"
    path = f"/{name.lower()}/app.zip"
    directory = f"/{name.lower()}"
    return {
        "stage": stage,
        "path": path,
        "full_path": f"@{stage}{path}",
        "directory": directory,
    }
