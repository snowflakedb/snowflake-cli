import os
import tempfile
from enum import Enum

import coverage
import snowflake.connector
import typer

from snowcli import config, utils
from snowcli.config import AppConfig
from snowcli.utils import conf_callback, generate_deploy_stage_name, print_db_cursor

from . import app

EnvironmentOption = typer.Option(
    "dev",
    help="Environment name",
    callback=conf_callback,
    is_eager=True,
)


class ReportOutputOptions(str, Enum):
    html = "html"
    json = "json"
    lcov = "lcov"


@app.command(
    "report",
    help="Generate a code coverage report by downloading and combining reports from the stage",
)
def procedure_coverage_report(
    environment: str = EnvironmentOption,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Name of the procedure",
    ),
    input_parameters: str = typer.Option(
        ...,
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int). Must exactly match those provided when creating the procedure.",
    ),
    output_format: ReportOutputOptions = typer.Option(
        ReportOutputOptions.html,
        "--output-format",
        case_sensitive=False,
        help="The format to use when saving the coverage report locally",
    ),
    store_as_comment: bool = typer.Option(
        False,
        "--store-as-comment",
        help="In addition to the local report, saves the percentage coverage (a decimal value) as a comment on the stored procedure so that a coverage threshold can be easily checked for a number of procedures.",
    ),
):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            f"The {environment} environment is not configured in app.toml "
            "yet, please run `snow configure -e dev` first before continuing.",
        )
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        deploy_dict = utils.getDeployNames(
            env_conf["database"],
            env_conf["schema"],
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
                results = config.snowflake_connection.getStage(
                    database=env_conf.get("database"),
                    schema=env_conf.get("schema"),
                    role=env_conf.get("role"),
                    warehouse=env_conf.get("warehouse"),
                    name=report_files,
                    path=str(temp_dir),
                ).fetchall()
            except snowflake.connector.errors.DatabaseError as database_error:
                if database_error.errno == 253006:
                    results = []
            if len(results) == 0:
                print(
                    "No code coverage reports were found on the stage. Please ensure that you've invoked the procedure at least once and that you provided the correct inputs"
                )
                raise typer.Abort()
            else:
                print(f"Combining data from {len(results)} reports")
            combined_coverage.combine(
                # the tuple contains the columns: | file ┃ size ┃ status ┃ message |
                data_paths=[
                    os.path.join(temp_dir, os.path.basename(result[0]))
                    for result in results
                ]
            )
        if output_format == ReportOutputOptions.html:
            coverage_percentage = combined_coverage.html_report()
            print(
                "Your HTML code coverage report is now available under the 'htmlcov' folder (htmlcov/index.html)."
            )
        elif output_format == ReportOutputOptions.json:
            coverage_percentage = combined_coverage.json_report()
            print("Your JSON code coverage report is now available in 'coverage.json'.")
        elif output_format == ReportOutputOptions.lcov:
            coverage_percentage = combined_coverage.lcov_report()
            print("Your lcov code coverage report is now available in 'coverage.lcov'.")
        else:
            print(f"Unknown output format '{output_format}'")
        if store_as_comment:
            print(
                f"Storing total coverage value of {str(coverage_percentage)} as a procedure comment."
            )
            config.snowflake_connection.setProcedureComment(
                env_conf["database"],
                env_conf["schema"],
                env_conf["role"],
                env_conf["warehouse"],
                name=name,
                inputParameters=input_parameters,
                show_exceptions=True,
                comment=str(coverage_percentage),
            )
