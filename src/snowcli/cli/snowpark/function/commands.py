from __future__ import annotations

import logging
from typing import List

import typer
from pathlib import Path
from tempfile import TemporaryDirectory

from click import ClickException
from snowflake.connector import ProgrammingError

from snowcli.cli.common.decorators import global_options_with_connection, global_options
from snowcli.cli.common.flags import (
    DEFAULT_CONTEXT_SETTINGS,
    identifier_argument,
    execution_identifier_argument,
)
from snowcli.cli.common.project_initialisation import add_init_command
from snowcli.cli.constants import DEPLOYMENT_STAGE, ObjectType
from snowcli.cli.snowpark.common import (
    remove_parameter_names,
    check_if_replace_is_required,
)
from snowcli.cli.project.definition_manager import DefinitionManager
from snowcli.cli.snowpark.function.manager import FunctionManager
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependencies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
)
from snowcli.cli.object.stage.manager import StageManager
from snowcli.exception import ObjectAlreadyExistsError
from snowcli.output.decorators import with_output
from snowcli.output.types import (
    MessageResult,
    SingleQueryResult,
    QueryResult,
    CommandResult,
    MultipleResults,
)
from snowcli.utils import (
    prepare_app_zip,
    get_snowflake_packages,
)

log = logging.getLogger(__name__)

app = typer.Typer(
    name="function",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages user defined functions.",
)

FileOption = typer.Option(
    "app.zip",
    "--file",
    "-f",
    help="Path to the file or folder to containing the function code. If you specify a directory, the procedure deploys the function in the default `app.zip` file.",
    exists=False,
)

HandlerOption = typer.Option(
    ...,
    "--handler",
    "-h",
    help="Path to the file containing the handler code for the stored function.",
)

LikeOption = typer.Option(
    "%%",
    "--like",
    "-l",
    help='Regular expression for filtering the functions by name. For example, `list --like "my%"` lists all functions in the **dev** (default) environment that begin with “my”.',
)

ReplaceOption = typer.Option(
    False,
    "--replace",
    help="Replace function, even if no detected changes to metadata",
)


add_init_command(app, project_type="functions", template="default_function")


@app.command("deploy")
@with_output
@global_options_with_connection
def function_deploy(
    function_names: List[str] = typer.Argument(
        None, help="Functions names. Multiple can be provided."
    ),
    replace: bool = ReplaceOption,
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """Deploy a python UDF or UDTF using a local artifact."""
    dm = DefinitionManager()
    functions = dm.project_definition.get("functions")
    if not functions:
        raise ClickException("No functions were specified in project definition.")

    defined_functions_names = [f["name"] for f in functions]
    for name in function_names:
        if name not in defined_functions_names:
            raise ClickException(f"Function '{name}' is not defined in the project.")

    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )

    sm = StageManager()
    fm = FunctionManager()
    function_exists = True
    replace_function = False
    results = MultipleResults()

    if function_names:
        functions = [f for f in functions if f["name"] in function_names]

    sm.create(stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli")

    for function in functions:
        arguments = ", ".join(
            (f"{arg['name']} {arg['type']}" for arg in function["signature"])
        )
        identifier = f"{function['name']}({arguments})"

        try:
            current_state = fm.describe(remove_parameter_names(identifier))
        except ProgrammingError as ex:
            if ex.msg.__contains__("does not exist or not authorized"):
                function_exists = False
            else:
                raise ex

        if function_exists and not replace:
            raise ObjectAlreadyExistsError(ObjectType.FUNCTION, identifier)

        if function_exists:
            replace_function = check_if_replace_is_required(
                ObjectType.FUNCTION,
                current_state,
                None,
                function["handler"],
                function["returns"],
            )

        artifact_file = _upload_snowpark_artifact(
            function_manager=fm,
            stage_manager=sm,
            function_identifier=identifier,
            file=Path("app.zip"),
        )

        if not function_exists or replace_function:
            packages = get_snowflake_packages()
            cursor = fm.create_or_replace(
                identifier=identifier,
                handler=function["handler"],
                return_type=function["returns"],
                artifact_file=str(artifact_file),
                packages=packages,
            )
            results.add(SingleQueryResult(cursor))

        results.add(MessageResult("No packages to update. Deployment complete!"))
    return results


@app.command("build")
@global_options
@with_output
def function_package(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """Packages function code into zip file."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    return MessageResult("Done")


@app.command("execute")
@with_output
@global_options_with_connection
def function_execute(
    execution_identifier: str = execution_identifier_argument(
        "function", "hello(1, 'world')"
    ),
    **options,
) -> CommandResult:
    """Executes a function in a Snowflake environment."""
    cursor = FunctionManager().execute(execution_identifier=execution_identifier)
    return SingleQueryResult(cursor)


@app.command("describe")
@with_output
@global_options_with_connection
def function_describe(
    identifier: str = identifier_argument("function", "hello(int, string)"),
    **options,
) -> CommandResult:
    """Describes a Snowflake function."""
    cursor = FunctionManager().describe(identifier=identifier)
    return QueryResult(cursor)


@app.command("list")
@with_output
@global_options_with_connection
def function_list(
    like: str = LikeOption,
    **options,
) -> CommandResult:
    """Displays the functions available in a specified environment, with the option to filter the results."""
    cursor = FunctionManager().show(like=like)
    return QueryResult(cursor)


@app.command("drop")
@with_output
@global_options_with_connection
def function_drop(
    identifier: str = identifier_argument("function", "hello(int, string)"),
    **options,
) -> CommandResult:
    """Deletes a function from a specified environment."""
    cursor = FunctionManager().drop(identifier=identifier)
    return SingleQueryResult(cursor)


def _upload_snowpark_artifact(
    function_manager: FunctionManager,
    stage_manager: StageManager,
    function_identifier: str,
    file: Path,
):
    artifact_location = f"{DEPLOYMENT_STAGE}/{function_manager.artifact_stage_path(function_identifier)}"
    artifact_file = f"{artifact_location}/app.zip"
    with TemporaryDirectory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(file, temp_dir)
        stage_manager.put(
            local_path=temp_app_zip_path,
            stage_path=artifact_location,
            overwrite=True,
        )
    log.info(f"{file.name} uploaded to stage {artifact_file}")
    return artifact_file
