from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import typer

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.cli.constants import DEPLOYMENT_STAGE
from snowcli.cli.snowpark.procedure.manager import ProcedureManager
from snowcli.cli.snowpark.procedure_coverage import app as procedure_coverage_app
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependancies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
    snowpark_update,
    replace_handler_in_zip,
)
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData
from snowcli.utils import (
    create_project_template,
    prepare_app_zip,
    get_snowflake_packages,
)

app = typer.Typer(
    name="procedure",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage stored procedures",
)
app.add_typer(procedure_coverage_app)


@app.command("init")
def procedure_init() -> None:
    """
    Initialize this directory with a sample set of files to create a procedure.
    """
    create_project_template("default_procedure")


@app.command("create")
@with_output
@global_options_with_connection
def procedure_create(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Name of the procedure",
    ),
    file: Path = typer.Option(
        "app.zip",
        "--file",
        "-f",
        help="Path to the file or folder to deploy",
        exists=False,
    ),
    handler: str = typer.Option(
        ...,
        "--handler",
        "-h",
        help="Handler",
    ),
    input_parameters: str = typer.Option(
        ...,
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    return_type: str = typer.Option(
        ...,
        "--return-type",
        "-r",
        help="Return type",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Replace if existing procedure",
    ),
    execute_as_caller: bool = typer.Option(
        False,
        "--execute-as-caller",
        help="Execute as caller",
    ),
    install_coverage_wrapper: bool = typer.Option(
        False,
        "--install-coverage-wrapper",
        help="Wraps the procedure with a code coverage measurement tool, so that a coverage report can be later retrieved.",
    ),
    **options,
) -> OutputData:
    """Creates a python procedure using local artifact."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    sm = StageManager()
    pm = ProcedureManager()

    procedure_identifier = pm.identifier(name=name, signature=input_parameters)
    sm.create(stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli")

    artifact_stage_path = pm.artifact_stage_path(procedure_identifier)
    artifact_location = Path(DEPLOYMENT_STAGE) / artifact_stage_path
    artifact_file = artifact_location / "app.zip"

    with TemporaryDirectory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(file, temp_dir)
        if install_coverage_wrapper:
            handler = replace_handler_in_zip(
                proc_name=name,
                proc_signature=input_parameters,
                handler=handler,
                coverage_reports_stage=DEPLOYMENT_STAGE,
                coverage_reports_stage_path=f"/{artifact_stage_path}/coverage",
                temp_dir=temp_dir,
                zip_file_path=temp_app_zip_path,
            )
        sm.put(
            local_path=temp_app_zip_path,
            stage_path=str(artifact_location),
            overwrite=overwrite,
        )

    packages = get_snowflake_packages()

    cursor = pm.create(
        identifier=procedure_identifier,
        handler=handler,
        return_type=return_type,
        artifact_file=str(artifact_file),
        packages=packages,
        overwrite=overwrite,
        execute_as_caller=execute_as_caller,
    )
    return OutputData.from_cursor(cursor)


@app.command("update")
def procedure_update(
    environment: str = ConnectionOption,
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Name of the procedure",
    ),
    file: Path = typer.Option(
        "app.zip",
        "--file",
        "-f",
        help="Path to the file to update",
        exists=False,
    ),
    handler: str = typer.Option(
        ...,
        "--handler",
        "-h",
        help="Handler",
    ),
    input_parameters: str = typer.Option(
        ...,
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    return_type: str = typer.Option(
        ...,
        "--return-type",
        "-r",
        help="Return type",
    ),
    replace: bool = typer.Option(
        False,
        "--replace-always",
        help="Replace procedure, even if no detected changes to metadata",
    ),
    execute_as_caller: bool = typer.Option(
        False,
        "--execute-as-caller",
        help="Execute as caller",
    ),
    install_coverage_wrapper: bool = typer.Option(
        False,
        "--install-coverage-wrapper",
        help="Wraps the procedure with a code coverage measurement tool, so that a coverage report can be later retrieved.",
    ),
):
    """Updates an existing python procedure using local artifact."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    snowpark_update(
        "procedure",
        environment,
        name,
        file,
        handler,
        input_parameters,
        return_type,
        replace,
        execute_as_caller,
        install_coverage_wrapper,
    )


@app.command("package")
def procedure_package(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
):
    """Packages procedure code into zip file."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )


@app.command("execute")
@with_output
@global_options_with_connection
def procedure_execute(
    signature: str = typer.Option(
        ...,
        "--procedure",
        "-p",
        help="Procedure with inputs. E.g. 'hello(int, string)'. Must exactly match those provided when creating the procedure.",
    ),
    **options,
) -> OutputData:
    """Executes a Snowflake procedure."""
    cursor = ProcedureManager().execute(expression=signature)
    return OutputData.from_cursor(cursor)


@app.command("describe")
@with_output
@global_options_with_connection
def procedure_describe(
    name: str = typer.Option("", "--name", "-n", help="Name of the procedure"),
    input_parameters: str = typer.Option(
        "",
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    signature: str = typer.Option(
        "",
        "--procedure",
        "-p",
        help="Procedure signature with inputs. E.g. 'hello(int, string)'",
    ),
    **options,
) -> OutputData:
    """Describes a Snowflake procedure."""
    cursor = ProcedureManager().describe(
        ProcedureManager.identifier(
            name=name,
            signature=input_parameters,
            name_and_signature=signature,
        )
    )
    return OutputData.from_cursor(cursor)


@app.command("list")
@with_output
@global_options_with_connection
def procedure_list(
    like: str = typer.Option(
        "%%",
        "--like",
        "-l",
        help='Filter procedures by name - e.g. "hello%"',
    ),
    **options,
) -> OutputData:
    """Lists Snowflake procedures."""
    cursor = ProcedureManager().show(like=like)
    return OutputData.from_cursor(cursor)


@app.command("drop")
@with_output
@global_options_with_connection
def procedure_drop(
    name: str = typer.Option("", "--name", "-n", help="Name of the procedure"),
    input_parameters: str = typer.Option(
        "",
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    signature: str = typer.Option(
        "",
        "--procedure",
        "-p",
        help="Procedure signature with inputs. E.g. 'hello(int, string)'",
    ),
    **options,
) -> OutputData:
    """Drops a Snowflake procedure."""
    cursor = ProcedureManager().drop(
        ProcedureManager.identifier(
            name=name,
            signature=input_parameters,
            name_and_signature=signature,
        )
    )
    return OutputData.from_cursor(cursor)
