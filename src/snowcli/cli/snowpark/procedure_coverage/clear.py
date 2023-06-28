from __future__ import annotations

import logging
import typer

from snowcli import config, utils
from snowcli.config import connect_to_snowflake
from snowcli.utils import generate_deploy_stage_name
from snowcli.output.printing import print_db_cursor

from snowcli.cli.snowpark.procedure_coverage import app
from snowcli.cli.common.flags import ConnectionOption

log = logging.getLogger(__name__)


@app.command(
    "clear",
    help="Delete the code coverage reports from the stage, to start the measuring process over",
)
def procedure_coverage_clear(
    environment: str = ConnectionOption,
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
    conn = connect_to_snowflake(connection_name=environment)
    if config.is_auth():
        deploy_dict = utils.get_deploy_names(
            conn.ctx.database,
            conn.ctx.schema,
            generate_deploy_stage_name(
                name,
                input_parameters,
            ),
        )
        coverage_path = f"""{deploy_dict["directory"]}/coverage"""
        results = conn.remove_from_stage(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=deploy_dict["stage"],
            path=coverage_path,
        )
        log.info("Deleted the following coverage results from the stage:")
        print_db_cursor(results)
