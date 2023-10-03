from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import typer

from snowcli.cli.common.decorators import global_options_with_connection, global_options
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.cli.constants import DEPLOYMENT_STAGE
from snowcli.cli.snowpark.function.manager import FunctionManager
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependancies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
)
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output
from snowcli.output.types import (
    MessageResult,
    SingleQueryResult,
    QueryResult,
    CommandResult,
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

HandlerOption = typer.Option(
    ...,
    "--handler",
    "-h",
    help="Path to the file containing the handler code for the stored procedure.",
)

InputParametersOption = typer.Option(
    ...,
    "--input-parameters",
    "-i",
    help="Input parameters for this function as a comma-separated string, such as (`message string`, `count int`).",
)

OptionalInputParametersOption = typer.Option(
    None,
    "--input-parameters",
    "-i",
    help="Input parameters for this function as a comma-separated string, such as (message string, count int)",
)

ReturnTypeOption = typer.Option(
    ...,
    "--return-type",
    "-r",
    help="Data type for the function to return.",
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


@app.command("create")
@with_output
@global_options_with_connection
def function_create(
    pypi_download: str = PyPiDownloadOption,
    package_native_libraries: str = PackageNativeLibrariesOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Name of the function.",
    ),
    file: Path = typer.Option(
        "app.zip",
        "--file",
        "-f",
        help="Path to the file or folder to deploy.",
        exists=False,
    ),
    handler: str = HandlerOption,
    input_parameters: str = InputParametersOption,
    return_type: str = ReturnTypeOption,
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Whether to replace an existing function with this one.",
    ),
    **options,
) -> CommandResult:
    """Creates a python UDF or UDTF using a local artifact."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )

    sm = StageManager()
    fm = FunctionManager()

    function_identifier = fm.identifier(name=name, signature=input_parameters)
    artifact_file = upload_snowpark_artifact(
        function_manager=fm,
        stage_manager=sm,
        function_identifier=function_identifier,
        file=file,
        overwrite=overwrite,
    )

    packages = get_snowflake_packages()
    cursor = fm.create(
        identifier=function_identifier,
        handler=handler,
        return_type=return_type,
        artifact_file=str(artifact_file),
        packages=packages,
        overwrite=overwrite,
    )
    return SingleQueryResult(cursor)


def upload_snowpark_artifact(
    function_manager, stage_manager, function_identifier, file, overwrite
):
    stage_manager.create(
        stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli"
    )
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


@app.command("update")
@with_output
@global_options_with_connection
def function_update(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    name: str = typer.Option(..., "--name", "-n", help="Name of the function."),
    file: Path = typer.Option(
        "app.zip",
        "--file",
        "-f",
        help="Path to the file or folder with the updated function definition.",
        exists=False,
    ),
    handler: str = HandlerOption,
    input_parameters: str = InputParametersOption,
    return_type: str = ReturnTypeOption,
    replace: bool = typer.Option(
        False,
        "--replace-always",
        help="Whether to replace the function even in no changes to the metadata are detected.",
    ),
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
    identifier = fm.identifier(name=name, signature=input_parameters)

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


@app.command("package")
@global_options
@with_output
def function_package(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
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
    function: str = typer.Option(
        ...,
        "--function",
        "-f",
        help="String containing the function signature with its parameters, such as 'hello(int, string)'.",
    ),
    **options,
) -> CommandResult:
    """Executes a function in a Snowflake environment."""
    cursor = FunctionManager().execute(expression=function)
    return SingleQueryResult(cursor)


@app.command("describe")
@with_output
@global_options_with_connection
def function_describe(
    name: str = typer.Option("", "--name", "-n", help="Name of the function."),
    input_parameters: str = OptionalInputParametersOption,
    function: str = typer.Option(
        "",
        "--function",
        "-f",
        help="String containing the function signature with its parameters, such as 'hello(int, string)'.",
    ),
    **options,
) -> CommandResult:
    """Describes a Snowflake function."""
    cursor = FunctionManager().describe(
        identifier=FunctionManager.identifier(
            name=name, signature=input_parameters, name_and_signature=function
        )
    )
    return QueryResult(cursor)


@app.command("list")
@with_output
@global_options_with_connection
def function_list(
    like: str = typer.Option(
        "%%",
        "--like",
        "-l",
        help='Regular expression for filtering the functions by name. For example, `list --file "my%"` lists all functions in the **dev** (default) environment that begin with “my”.',
    ),
    **options,
) -> CommandResult:
    """Displays the functions available in a specified environment, with the option to filter the results."""
    cursor = FunctionManager().show(like=like)
    return QueryResult(cursor)


@app.command("drop")
@with_output
@global_options_with_connection
def function_drop(
    name: str = typer.Option("", "--name", "-n", help="Name of the function."),
    input_parameters: str = OptionalInputParametersOption,
    signature: str = typer.Option(
        "",
        "--function",
        "-f",
        help="String containing the function signature with its parameters, such as 'hello(int, string)'.",
    ),
    **options,
) -> CommandResult:
    """Deletes a function from a specified environment."""
    cursor = FunctionManager().drop(
        identifier=FunctionManager.identifier(
            name=name, signature=input_parameters, name_and_signature=signature
        )
    )
    return SingleQueryResult(cursor)
