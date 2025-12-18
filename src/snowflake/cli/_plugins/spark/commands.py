# Copyright (c) 2025 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.spark.manager import SparkManager, SubmitQueryBuilder
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult

app = SnowTyperFactory(
    name="spark",
    help="Manages Spark Applications.",
)


@app.command(requires_connection=True)
def submit(
    entrypoint_file: str = typer.Argument(
        None,
        metavar="FILE_PATH",
        help="The path to the entrypoint file to execute.",
        show_default=False,
    ),
    application_arguments: Optional[List[str]] = typer.Argument(
        None,
        metavar="APPLICATION_ARGUMENTS",
        help="Application arguments.",
        show_default=False,
    ),
    class_name: Optional[str] = typer.Option(
        None,
        "--class",
        help="The name of the main class to execute. Used and required by Java/Scala applications only.",
    ),
    scls_file_stage: Optional[str] = typer.Option(
        None,
        f"--snow-file-stage",
        help="The stage to upload the entrypoint file to.",
        show_default=False,
    ),
    status: Optional[str] = typer.Option(
        None,
        "--status",
        help=f"Check the status of the Spark application by its ID. (e.g. snow spark submit --status [id])",
        show_default=False,
    ),
    jars: Optional[str] = typer.Option(
        None,
        "--jars",
        help="Comma-separated list of JAR files to include in the classpath.",
        show_default=False,
    ),
    **options,
):
    """
    Submit Spark Job to Snowflake.
    """
    if status:
        return MessageResult(SparkManager().check_status(status))
    else:
        # validate required arguments
        if not entrypoint_file:
            raise ClickException("Entrypoint file path is required")
        if not scls_file_stage:
            raise ClickException(f"--snow-file-stage is required")

        file_name = SparkManager().upload_file_to_stage(
            entrypoint_file, scls_file_stage
        )
        # e.g. Spark Application submitted successfully. Spark Application ID: <id>
        result_message = SparkManager().submit(
            SubmitQueryBuilder(file_name, scls_file_stage)
            .with_application_arguments(application_arguments)
            .with_class_name(class_name)
            .build()
        )
        return MessageResult(result_message)
