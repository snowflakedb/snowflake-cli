from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List


import typer

from snowcli import utils
from snowcli.cli.common.decorators import global_options_with_connection, global_options
from snowcli.cli.common.flags import (
    DEFAULT_CONTEXT_SETTINGS,
    identifier_argument,
    execution_identifier_argument,
)
from snowcli.cli.constants import DEPLOYMENT_STAGE
from snowcli.cli.snowpark.procedure.manager import ProcedureManager
from snowcli.cli.snowpark.procedure_coverage.commands import (
    app as procedure_coverage_app,
)
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependencies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
    OverwriteOption,
    ReturnsOption,
)
from snowcli.cli.stage.manager import StageManager
from snowcli.output.decorators import with_output
from snowcli.output.types import (
    MessageResult,
    CommandResult,
    SingleQueryResult,
    QueryResult,
)
from snowcli.utils import (
    create_project_template,
    prepare_app_zip,
    get_snowflake_packages,
    convert_resource_details_to_dict,
    get_snowflake_packages_delta,
    sql_to_python_return_type_mapper,
)


log = logging.getLogger(__name__)

app = typer.Typer(
    name="procedure",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manages stored procedures.",
)
app.add_typer(procedure_coverage_app)

FileOption = typer.Option(
    "app.zip",
    "--file",
    "-f",
    help="Path to the file or folder to containing the procedure code. If you specify a directory, the procedure deploys the procedure in the default `app.zip` file.",
    exists=False,
)

HandlerOption = typer.Option(
    ...,
    "--handler",
    "-h",
    help="Path to the file containing the handler code for the stored procedure.",
)

LikeOption = typer.Option(
    "%%",
    "--like",
    "-l",
    help='Regular expression for filtering the procedure by name. For example, `list --like "my%"` lists all procedures in the **dev** (default) environment that begin with “my”.',
)

ReplaceOption = typer.Option(
    False,
    "--replace-always",
    "-a",
    help="Replace procedure, even if no detected changes to metadata",
)

ExecuteAsCaller = typer.Option(
    False,
    "--execute-as-caller",
    help="Execute as caller.",
)

InstallCoverageWrapper = typer.Option(
    False,
    "--install-coverage-wrapper",
    help="Whether to wrap the procedure with a code coverage measurement tool, so a coverage report can be later retrieved.",
)


@app.command("init")
@global_options
@with_output
def procedure_init(**options) -> CommandResult:
    """
    Initialize this directory with a sample set of files to create a procedure.
    """
    create_project_template("default_procedure")
    return MessageResult("Done")


@app.command("create")
@with_output
@global_options_with_connection
def procedure_create(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    identifier: str = identifier_argument(
        "procedure", "hello(number int, name string)"
    ),
    file: Path = FileOption,
    handler: str = HandlerOption,
    return_type: str = ReturnsOption,
    overwrite: bool = OverwriteOption,
    execute_as_caller: bool = ExecuteAsCaller,
    install_coverage_wrapper: bool = InstallCoverageWrapper,
    **options,
) -> CommandResult:
    """Creates a stored python procedure using a local artifact."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    sm = StageManager()
    pm = ProcedureManager()

    artifact_file, new_handler = _upload_procedure_artifact(
        stage_manager=sm,
        procedure_manager=pm,
        file=file,
        handler=handler,
        install_coverage_wrapper=install_coverage_wrapper,
        overwrite=overwrite,
        identifier=identifier,
    )

    packages = get_snowflake_packages()

    cursor = pm.create(
        identifier=identifier,
        handler=new_handler,
        return_type=return_type,
        artifact_file=str(artifact_file),
        packages=packages,
        overwrite=overwrite,
        execute_as_caller=execute_as_caller,
    )
    return SingleQueryResult(cursor)


def _upload_procedure_artifact(
    stage_manager,
    procedure_manager,
    file,
    handler,
    install_coverage_wrapper,
    overwrite,
    identifier,
):
    stage_manager.create(
        stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli"
    )
    artifact_stage_path = procedure_manager.artifact_stage_path(identifier)
    artifact_location = f"{DEPLOYMENT_STAGE}/{artifact_stage_path}"
    artifact_file = f"{artifact_location}/app.zip"
    with TemporaryDirectory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(file, temp_dir)
        if install_coverage_wrapper:
            signature_start_index = identifier.index("(")
            name = identifier[0:signature_start_index]
            signature = identifier[signature_start_index:]
            handler = _replace_handler_in_zip(
                proc_name=name,
                proc_signature=signature,
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
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    identifier: str = identifier_argument(
        "procedure", "hello(number int, name string)"
    ),
    file: Path = FileOption,
    handler: str = HandlerOption,
    return_type: str = ReturnsOption,
    replace: bool = ReplaceOption,
    execute_as_caller: bool = ExecuteAsCaller,
    install_coverage_wrapper: bool = InstallCoverageWrapper,
    **options,
) -> CommandResult:
    """Updates a procedure in a specified environment."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )

    pm = ProcedureManager()
    sm = StageManager()

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
            or sql_to_python_return_type_mapper(resource_json["returns"]).lower()
            != return_type.lower()
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
        install_coverage_wrapper=install_coverage_wrapper,
        overwrite=True,
        identifier=identifier,
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
        return SingleQueryResult(cursor)

    return MessageResult("No packages to update. Deployment complete!")


@app.command("package")
@global_options
@with_output
def procedure_package(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """Packages procedure code into a `.zip` file."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    return MessageResult("Done")


@app.command("execute")
@with_output
@global_options_with_connection
def procedure_execute(
    execution_identifier: str = execution_identifier_argument(
        "procedure", "hello(1, 'world')"
    ),
    **options,
) -> CommandResult:
    """Executes a procedure in a specified environment."""
    cursor = ProcedureManager().execute(execution_identifier=execution_identifier)
    return SingleQueryResult(cursor)


@app.command("describe")
@with_output
@global_options_with_connection
def procedure_describe(
    identifier: str = identifier_argument("procedure", "hello(int, string)"),
    **options,
) -> CommandResult:
    """Describes the specified stored procedure, including the stored procedure signature (i.e. arguments), return value, language, and body (i.e. definition)."""
    cursor = ProcedureManager().describe(identifier=identifier)
    return QueryResult(cursor)


@app.command("list")
@with_output
@global_options_with_connection
def procedure_list(
    like: str = LikeOption,
    **options,
) -> CommandResult:
    """Lists available procedures."""
    cursor = ProcedureManager().show(like=like)
    return QueryResult(cursor)


@app.command("drop")
@with_output
@global_options_with_connection
def procedure_drop(
    identifier: str = identifier_argument("procedure", "hello(int, string)"),
    **options,
) -> CommandResult:
    """Drops a Snowflake procedure."""
    cursor = ProcedureManager().drop(identifier=identifier)
    return SingleQueryResult(cursor)


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
