from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import typer

from snowcli import utils
from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.constants import DEPLOYMENT_STAGE
from snowcli.cli.snowpark.procedure.manager import ProcedureManager
from snowcli.cli.snowpark.procedure_coverage.commands import (
    app as procedure_coverage_app,
)
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependancies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
)
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output
from snowcli.output.printing import OutputData
from snowcli.utils import (
    create_project_template,
    prepare_app_zip,
    get_snowflake_packages,
    convert_resource_details_to_dict,
    get_snowflake_packages_delta,
)


log = logging.getLogger(__name__)

app = typer.Typer(
    name="procedure",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage stored procedures",
)
app.add_typer(procedure_coverage_app)


@app.command("init")
@with_output
def procedure_init() -> OutputData:
    """
    Initialize this directory with a sample set of files to create a procedure.
    """
    create_project_template("default_procedure")
    return OutputData.from_string("Done")


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
    artifact_file, new_handler = _upload_procedure_artifact(
        stage_manager=sm,
        procedure_manager=pm,
        file=file,
        handler=handler,
        input_parameters=input_parameters,
        install_coverage_wrapper=install_coverage_wrapper,
        name=name,
        overwrite=overwrite,
        procedure_identifier=procedure_identifier,
    )

    packages = get_snowflake_packages()

    cursor = pm.create(
        identifier=procedure_identifier,
        handler=new_handler,
        return_type=return_type,
        artifact_file=str(artifact_file),
        packages=packages,
        overwrite=overwrite,
        execute_as_caller=execute_as_caller,
    )
    return OutputData.from_cursor(cursor)


def _upload_procedure_artifact(
    stage_manager,
    procedure_manager,
    file,
    handler,
    input_parameters,
    install_coverage_wrapper,
    name,
    overwrite,
    procedure_identifier,
):
    stage_manager.create(
        stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli"
    )
    artifact_stage_path = procedure_manager.artifact_stage_path(procedure_identifier)
    artifact_location = Path(DEPLOYMENT_STAGE) / artifact_stage_path
    artifact_file = artifact_location / "app.zip"
    with TemporaryDirectory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(file, temp_dir)
        if install_coverage_wrapper:
            handler = _replace_handler_in_zip(
                proc_name=name,
                proc_signature=input_parameters,
                handler=handler,
                coverage_reports_stage=DEPLOYMENT_STAGE,
                coverage_reports_stage_path=f"/{artifact_stage_path}/coverage",
                temp_dir=temp_dir,
                zip_file_path=temp_app_zip_path,
            )
        stage_manager.put(
            local_path=temp_app_zip_path,
            stage_path=str(artifact_location),
            overwrite=overwrite,
        )
    return artifact_file, handler


@app.command("update")
@with_output
@global_options_with_connection
def procedure_update(
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
    **options,
) -> OutputData:
    """Updates an existing python procedure using local artifact."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )

    pm = ProcedureManager()
    sm = StageManager()
    identifier = pm.identifier(name=name, signature=input_parameters)

    try:
        current_state = pm.describe(identifier)
    except:
        log.info(f"Procedure does not exists. Creating it from scratch.")
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

        coverage_package = "coverage"
        if install_coverage_wrapper and coverage_package not in [
            *anaconda_packages,
            *updated_package_list,
        ]:
            updated_package_list.append(coverage_package)

        if updated_package_list:
            diff = len(updated_package_list) - len(anaconda_packages)
            log.info(f"Found difference of {diff} packages. Replacing the procedure.")
            replace = True
        elif (
            resource_json["handler"].lower() != handler.lower()
            or resource_json["returns"].lower() != return_type.lower()
        ):
            log.info(
                "Return type or handler types do not match. Replacing the procedure."
            )
            replace = True

    artifact_file, new_handler = _upload_procedure_artifact(
        stage_manager=sm,
        procedure_manager=pm,
        file=file,
        handler=handler,
        input_parameters=input_parameters,
        install_coverage_wrapper=install_coverage_wrapper,
        name=name,
        overwrite=True,
        procedure_identifier=identifier,
    )

    if replace:
        packages = get_snowflake_packages()
        cursor = pm.create(
            identifier=identifier,
            handler=new_handler,
            return_type=return_type,
            artifact_file=str(artifact_file),
            packages=packages,
            overwrite=True,
            execute_as_caller=execute_as_caller,
        )
        return OutputData.from_cursor(cursor)

    return OutputData.from_string("No packages to update. Deployment complete!")


@app.command("package")
@with_output
def procedure_package(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
) -> OutputData:
    """Packages procedure code into zip file."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    return OutputData.from_string("Done")


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


def _replace_handler_in_zip(
    proc_name: str,
    proc_signature: str,
    handler: str,
    temp_dir: str,
    zip_file_path: str,
    coverage_reports_stage: str,
    coverage_reports_stage_path: str,
) -> str:
    """
    Given an existing zipped stored proc artifact, this function inserts a file containing a code coverage
    wrapper, then returns the name of the new handler that the proc should use
    """
    handler_parts = handler.split(".")
    if len(handler_parts) != 2:
        log.error(
            "To install a code coverage wrapper, your handler must be in the format <module>.<function>"
        )
        raise typer.Abort()
    wrapper_file = os.path.join(temp_dir, "snowpark_coverage.py")
    utils.generate_snowpark_coverage_wrapper(
        target_file=wrapper_file,
        proc_name=proc_name,
        proc_signature=proc_signature,
        coverage_reports_stage=coverage_reports_stage,
        coverage_reports_stage_path=coverage_reports_stage_path,
        handler_module=handler_parts[0],
        handler_function=handler_parts[1],
    )
    utils.add_file_to_existing_zip(zip_file=zip_file_path, other_file=wrapper_file)
    return "snowpark_coverage.measure_coverage"
