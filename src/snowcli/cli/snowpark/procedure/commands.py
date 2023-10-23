from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import typer
from snowflake.connector import ProgrammingError

from snowcli import utils
from snowcli.cli.common.decorators import global_options_with_connection, global_options
from snowcli.cli.common.flags import (
    DEFAULT_CONTEXT_SETTINGS,
    identifier_argument,
    execution_identifier_argument,
)
from snowcli.cli.constants import DEPLOYMENT_STAGE, ObjectType
from snowcli.cli.snowpark.common import (
    remove_parameter_names,
    check_if_replace_is_required,
)
from snowcli.cli.snowpark.procedure.manager import ProcedureManager
from snowcli.cli.snowpark.procedure_coverage.commands import (
    app as procedure_coverage_app,
)
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependencies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
    ReturnsOption,
)
from snowcli.cli.stage.manager import StageManager
from snowcli.exception import ObjectAlreadyExistsError
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
    "--replace",
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


def _upload_procedure_artifact(
    procedure_manager,
    file,
    handler,
    install_coverage_wrapper,
    identifier,
):
    stage_manager = StageManager()
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
            overwrite=True,
        )
    return artifact_file, handler


@app.command("deploy")
@with_output
@global_options_with_connection
def procedure_deploy(
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
    """Deploy a procedure in a specified environment."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )

    pm = ProcedureManager()
    procedure_exists = True
    replace_procedure = False
    try:
        current_state = pm.describe(remove_parameter_names(identifier))
    except ProgrammingError as ex:
        if ex.msg.__contains__("does not exist or not authorized"):
            procedure_exists = False
        else:
            raise ex

    if procedure_exists and not replace:
        raise ObjectAlreadyExistsError(ObjectType.PROCEDURE, identifier)

    if procedure_exists:
        replace_procedure = check_if_replace_is_required(
            ObjectType.FUNCTION,
            current_state,
            install_coverage_wrapper,
            handler,
            return_type,
        )

    artifact_file, new_handler = _upload_procedure_artifact(
        procedure_manager=pm,
        file=file,
        handler=handler,
        install_coverage_wrapper=install_coverage_wrapper,
        identifier=identifier,
    )

    if not procedure_exists or replace_procedure:
        packages = get_snowflake_packages()
        cursor = pm.create_or_replace(
            identifier=identifier,
            handler=new_handler,
            return_type=return_type,
            artifact_file=str(artifact_file),
            packages=packages,
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
