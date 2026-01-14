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
    image: Optional[str] = typer.Option(
        None,
        "--image",
        help="The docker image to use for the Spark application. (for development only)",
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
    py_files: Optional[str] = typer.Option(
        None,
        "--py-files",
        help="Comma-separated list of .zip, .egg, or .py files to include in the PYTHONPATH for Python applications.",
        show_default=False,
    ),
    conf: Optional[List[str]] = typer.Option(
        None,
        "--conf",
        help="Spark configuration properties in the format of key=value.",
        show_default=False,
    ),
    name: Optional[str] = typer.Option(
        None,
        "--name",
        help="The name of the Spark application.",
        show_default=False,
    ),
    files: Optional[str] = typer.Option(
        None,
        "--files",
        help="Comma-separated list of files to include in the Spark application. File paths can be accessed via SparkFiles.get(file_name).",
        show_default=False,
    ),
    properties_file: Optional[str] = typer.Option(
        None,
        "--properties-file",
        help="The path to the properties file to include in the Spark application.  The configuration loaded from the file will override the configuration passed in via --conf.",
        show_default=False,
    ),
    **options,
):
    """
    Submit Spark Job to Snowflake.
    """
    manager = SparkManager()
    if status:
        return MessageResult(manager.check_status(status))
    else:
        # validate required arguments
        if not entrypoint_file:
            raise ClickException("Entrypoint file path is required")
        if not scls_file_stage:
            raise ClickException(f"--snow-file-stage is required")

        file_name = manager.upload_file_to_stage(entrypoint_file, scls_file_stage)

        query_builder = (
            SubmitQueryBuilder(file_name, scls_file_stage)
            .with_application_arguments(application_arguments)
            .with_class_name(class_name)
        )

        if properties_file:
            conf_dict = _read_properties_file(properties_file)
            query_builder.with_conf(conf_dict)

        if jars:
            jar_paths = jars.split(",")
            uploaded_jars = [
                manager.upload_file_to_stage(jar_path, scls_file_stage)
                for jar_path in jar_paths
            ]
            query_builder.with_jars(uploaded_jars)

        if py_files:
            py_file_paths = py_files.split(",")
            uploaded_py_files = [
                manager.upload_file_to_stage(py_file_path, scls_file_stage)
                for py_file_path in py_file_paths
            ]
            query_builder.with_py_files(uploaded_py_files)

        if conf:
            query_builder.with_conf(conf)

        if name:
            query_builder.with_name(name)

        if files:
            file_paths = files.split(",")
            uploaded_files = [
                manager.upload_file_to_stage(file_path, scls_file_stage)
                for file_path in file_paths
            ]
            query_builder.with_files(uploaded_files)

        # e.g. Spark Application submitted successfully. Spark Application ID: <id>
        result_message = manager.submit(query_builder.build(), image)
        return MessageResult(result_message)


def _read_properties_file(file_path: str) -> dict:
    """
    Read a spark-submit properties file and return the content as a dict.
    The file format is expected to be key followed by spaces and then value, one per line.
    Lines starting with # are treated as comments and ignored.
    """
    conf_dict = {}
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue
            # Parse key-value pairs separated by whitespace
            parts = line.split(None, 1)  # Split on whitespace, max 2 parts
            if len(parts) == 2:
                key, value = parts
                conf_dict[key] = value.rstrip()
    return conf_dict
