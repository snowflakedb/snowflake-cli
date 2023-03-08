from __future__ import annotations

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


@app.command(
    "clear",
    help="Delete the code coverage reports from the stage, to start the measuring process over",
)
def procedure_coverage_clear(
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
        coverage_path = f"""{deploy_dict["directory"]}/coverage"""
        results = config.snowflake_connection.removeFromStage(
            database=env_conf["database"],
            schema=env_conf["schema"],
            role=env_conf["role"],
            warehouse=env_conf["warehouse"],
            name=deploy_dict["stage"],
            path=coverage_path,
        )
        print("Deleted the following coverage results from the stage:")
        print_db_cursor(results)
