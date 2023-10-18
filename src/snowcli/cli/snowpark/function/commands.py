from __future__ import annotations

import logging
from typing import List

import typer
from pathlib import Path
from tempfile import TemporaryDirectory

from snowcli.cli.common.decorators import global_options_with_connection, global_options
from snowcli.cli.common.flags import (
    DEFAULT_CONTEXT_SETTINGS,
    identifier_argument,
    execution_identifier_argument,
)
from snowcli.cli.constants import DEPLOYMENT_STAGE
from snowcli.cli.project.definition_manager import DefinitionManager
from snowcli.cli.snowpark.function.manager import FunctionManager
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependencies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
    ReturnsOption,
)
from snowcli.cli.stage.manager import StageManager
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
    create_project_template,
    convert_resource_details_to_dict,
    get_snowflake_packages_delta,
    sql_to_python_return_type_mapper,
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


@app.command("init")
@global_options
@with_output
def function_init(**options):
    """
    Initializes this directory with a sample set of files for creating a function.
    """
    create_project_template("default_function")
    return MessageResult("Done")


@app.command("deploy")
@with_output
@global_options_with_connection
def function_deploy(
    function_names: List[str] = typer.Option(
        None, help="Functions names. Multiple can be provided."
    ),
    replace: bool = ReplaceOption,
    pypi_download: str = PyPiDownloadOption,
    package_native_libraries: str = PackageNativeLibrariesOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    **options,
) -> CommandResult:
    """Creates a python UDF or UDTF using a local artifact."""
    dm = DefinitionManager()
    functions = dm.project_definition.get("functions")

    if not functions:
        return MessageResult("No functions were specified in project definition.")

    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )

    sm = StageManager()
    fm = FunctionManager()

    result = MultipleResults()
    if function_names:
        functions = [f for f in functions if f["name"] in function_names]

    if not functions:
        return MessageResult("No functions defined in this project.")

    sm.create(stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli")

    for function in functions:
        arguments = ", ".join(
            (f"{arg['name']} {arg['type']}" for arg in function["signature"])
        )
        identifier = f"{function['name']}({arguments})"

        # TODO: we can probably be smarter than that, but we
        #  would need to upload project artifacts into single directory
        artifact_file = upload_snowpark_artifact(
            function_manager=fm,
            stage_manager=sm,
            function_identifier=identifier,
            file=Path("app.zip"),
            overwrite=replace,
        )

        packages = get_snowflake_packages()
        cursor = fm.create(
            identifier=identifier,
            handler=function["handler"],
            return_type=function["returns"],
            artifact_file=str(artifact_file),
            packages=packages,
            overwrite=replace,
        )
        result.add(SingleQueryResult(cursor))
    return result


def upload_snowpark_artifact(
    function_manager, stage_manager, function_identifier, file, overwrite
):
    artifact_location = f"{DEPLOYMENT_STAGE}/{function_manager.artifact_stage_path(function_identifier)}"
    artifact_file = f"{artifact_location}/app.zip"
    with TemporaryDirectory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(file, temp_dir)
        stage_manager.put(
            local_path=temp_app_zip_path,
            stage_path=artifact_location,
            overwrite=overwrite,
        )
    log.info(f"{file.name} uploaded to stage {artifact_file}")
    return artifact_file


@app.command("update", hidden=True)
@with_output
@global_options_with_connection
def function_update(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    identifier: str = identifier_argument("function", "hello(number int, name string)"),
    file: Path = FileOption,
    handler: str = HandlerOption,
    return_type: str = ReturnsOption,
    replace: bool = ReplaceOption,
    **options,
) -> CommandResult:
    """Updates an existing python UDF or UDTF using a local artifact."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )

    fm = FunctionManager()
    sm = StageManager()

    try:
        current_state = fm.describe(identifier)
    except:
        log.info(f"Function does not exists. Creating it from scratch.")
        replace = True
    else:
        resource_json = convert_resource_details_to_dict(current_state)
        anaconda_packages = resource_json["packages"]
        log.info(
            f"Found {len(anaconda_packages)} defined Anaconda "
            f"packages in deployed function..."
        )
        log.info("Checking if app configuration has changed...")
        updated_package_list = get_snowflake_packages_delta(
            anaconda_packages,
        )
        if updated_package_list:
            diff = len(updated_package_list) - len(anaconda_packages)
            log.info(f"Found difference of {diff} packages. Replacing the function.")
            replace = True
        elif (
            resource_json["handler"].lower() != handler.lower()
            or sql_to_python_return_type_mapper(resource_json["returns"]).lower()
            != return_type.lower()
        ):
            log.info(
                "Return type or handler types do not match. Replacing the function."
            )
            replace = True

    sm.create(stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli")
    artifact_file = upload_snowpark_artifact(
        function_manager=fm,
        stage_manager=sm,
        function_identifier=identifier,
        file=file,
        overwrite=True,
    )

    if replace:
        packages = get_snowflake_packages()
        cursor = fm.create(
            identifier=identifier,
            handler=handler,
            return_type=return_type,
            artifact_file=str(artifact_file),
            packages=packages,
            overwrite=True,
        )
        return SingleQueryResult(cursor)

    return MessageResult("No packages to update. Deployment complete!")


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
