from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import typer

from snowcli.cli.common.decorators import global_options
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.cli.constants import DEPLOYMENT_STAGE
from snowcli.cli.snowpark.function.manager import FunctionManager
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependancies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
    snowpark_update,
)
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData
from snowcli.utils import (
    prepare_app_zip,
    get_snowflake_packages,
    create_project_template,
)

app = typer.Typer(
    name="function",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage user defined functions",
)

HandlerOption = typer.Option(
    ...,
    "--handler",
    "-h",
    help="Handler",
)

InputParametersOption = typer.Option(
    ...,
    "--input-parameters",
    "-i",
    help="Input parameters - such as (message string, count int)",
)

OptionalInputParametersOption = typer.Option(
    None,
    "--input-parameters",
    "-i",
    help="Input parameters - such as (message string, count int)",
)

ReturnTypeOption = typer.Option(
    ...,
    "--return-type",
    "-r",
    help="Return type",
)


@app.command("init")
def function_init():
    """
    Initialize this directory with a sample set of files to create a function.
    """
    create_project_template("default_function")


@app.command("create")
@with_output
@global_options
def function_create(
    pypi_download: str = PyPiDownloadOption,
    package_native_libraries: str = PackageNativeLibrariesOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Name of the function",
    ),
    file: Path = typer.Option(
        "app.zip",
        "--file",
        "-f",
        help="Path to the file or folder to deploy",
        exists=False,
    ),
    handler: str = HandlerOption,
    input_parameters: str = InputParametersOption,
    return_type: str = ReturnTypeOption,
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Replace if existing function",
    ),
    **options,
) -> OutputData:
    """Creates a python UDF/UDTF using local artifact."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )

    sm = StageManager()
    fm = FunctionManager()

    function_identifier = fm.identifier(name=name, signature=input_parameters)
    sm.create(stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli")

    artifact_location = Path(DEPLOYMENT_STAGE) / fm.artifact_stage_path(
        function_identifier
    )
    artifact_file = artifact_location / "app.zip"

    with TemporaryDirectory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(file, temp_dir)
        sm.put(
            local_path=temp_app_zip_path,
            stage_path=str(artifact_location),
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
    return OutputData.from_cursor(cursor)


@app.command("update")
def function_update(
    environment: str = ConnectionOption,
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    name: str = typer.Option(..., "--name", "-n", help="Name of the function"),
    file: Path = typer.Option(
        "app.zip",
        "--file",
        "-f",
        help="Path to the file to update",
        exists=False,
    ),
    handler: str = HandlerOption,
    input_parameters: str = InputParametersOption,
    return_type: str = ReturnTypeOption,
    replace: bool = typer.Option(
        False,
        "--replace-always",
        "-a",
        help="Replace function, even if no detected changes to metadata",
    ),
):
    """Updates an existing python UDF/UDTF using local artifact."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    snowpark_update(
        type="function",
        environment=environment,
        name=name,
        file=file,
        handler=handler,
        input_parameters=input_parameters,
        return_type=return_type,
        replace=replace,
    )


@app.command("package")
def function_package(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
) -> None:
    """Packages function code into zip file."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )


@app.command("execute")
@with_output
@global_options
def function_execute(
    function: str = typer.Option(
        ...,
        "--function",
        "-f",
        help="Function with inputs. E.g. 'hello(int, string)'",
    ),
    **options,
) -> OutputData:
    """Executes a Snowflake function."""
    cursor = FunctionManager().execute(expression=function)
    return OutputData.from_cursor(cursor)


@app.command("describe")
@with_output
@global_options
def function_describe(
    name: str = typer.Option("", "--name", "-n", help="Name of the function"),
    input_parameters: str = OptionalInputParametersOption,
    function: str = typer.Option(
        "",
        "--function",
        "-f",
        help="Function signature with inputs. E.g. 'hello(int, string)'",
    ),
    **options,
) -> OutputData:
    """Describes a Snowflake function."""
    cursor = FunctionManager().describe(
        identifier=FunctionManager.identifier(
            name=name, signature=input_parameters, name_and_signature=function
        )
    )
    return OutputData.from_cursor(cursor)


@app.command("list")
@with_output
@global_options
def function_list(
    like: str = typer.Option(
        "%%",
        "--like",
        "-l",
        help='Filter functions by name - e.g. "hello%"',
    ),
    **options,
) -> OutputData:
    """Lists Snowflake functions."""
    cursor = FunctionManager().show(like=like)
    return OutputData.from_cursor(cursor)


@app.command("drop")
@with_output
@global_options
def function_drop(
    name: str = typer.Option("", "--name", "-n", help="Name of the function"),
    input_parameters: str = OptionalInputParametersOption,
    signature: str = typer.Option(
        "",
        "--function",
        "-f",
        help="Function signature with inputs. E.g. 'hello(int, string)'",
    ),
    **options,
) -> OutputData:
    """Drops a Snowflake function."""
    cursor = FunctionManager().drop(
        identifier=FunctionManager.identifier(
            name=name, signature=input_parameters, name_and_signature=signature
        )
    )
    return OutputData.from_cursor(cursor)
