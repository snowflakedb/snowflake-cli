import os
import tempfile
from typing import List

import coverage
import typer
from snowflake.snowpark import GetResult

from snowcli import config, utils
from snowcli.config import AppConfig
from snowcli.utils import conf_callback, generate_deploy_stage_name

from . import app

EnvironmentOption = typer.Option(
    "dev",
    help="Environment name",
    callback=conf_callback,
    is_eager=True,
)


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
):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            f"The {environment} environment is not configured in app.toml "
            "yet, please run `snow configure dev` first before continuing.",
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
            results: List[
                GetResult
            ] = config.snowflake_connection.fetchProcedureCoverageReports(
                stage_name=deploy_dict["stage"],
                stage_path=deploy_dict["directory"],
                target_directory=temp_dir,
            )
            if len(results) == 0:
                print(
                    "No code coverage reports were found on the stage. Please ensure that you've invoked the procedure at least once and that you provided the correct inputs"
                )
                raise typer.Abort()
            else:
                print(f"Combining data from {len(results)} reports")
            combined_coverage.combine(
                data_paths=[os.path.join(temp_dir, result.file) for result in results]
            )
        combined_coverage.html_report()
