from __future__ import annotations

import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Dict

import typer
from click import ClickException
from snowflake.connector import ProgrammingError

from snowcli import utils
from snowcli.cli.common.decorators import global_options_with_connection, global_options
from snowcli.cli.common.flags import (
    DEFAULT_CONTEXT_SETTINGS,
    identifier_argument,
    execution_identifier_argument,
    LikeOption,
)
from snowcli.cli.common.project_initialisation import add_init_command
from snowcli.cli.constants import DEPLOYMENT_STAGE, SnowparkObjectType
from snowcli.cli.project.definition_manager import DefinitionManager
from snowcli.cli.snowpark.common import (
    remove_parameter_names,
    check_if_replace_is_required,
    build_udf_sproc_identifier,
)
from snowcli.cli.snowpark.manager import ProcedureManager, FunctionManager

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
    CommandResult,
    SingleQueryResult,
    QueryResult,
    CollectionResult,
)
from snowcli.utils import (
    get_snowflake_packages,
)


log = logging.getLogger(__name__)

app = typer.Typer(
    name="snowpark",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage procedures and functions.",
)

ReplaceOption = typer.Option(
    False,
    "--replace",
    help="Replace procedure or function, even if no detected changes to metadata",
)

InstallCoverageWrapper = typer.Option(
    False,
    "--install-coverage-wrapper",
    help="Whether to wrap the procedure with a code coverage measurement tool, "
    "so a coverage report can be later retrieved.",
)

ObjectTypeArgument = typer.Argument(
    help="Type of snowpark object",
    case_sensitive=False,
)

add_init_command(app, project_type="snowpark", template="default_snowpark")


def _alter_procedure_artifact(
    artifact_stage_path: str,
    artifact_path: Path,
    handler: str,
    identifier: str,
):
    signature_start_index = identifier.index("(")
    name = identifier[0:signature_start_index]
    signature = identifier[signature_start_index:]
    handler = _replace_handler_in_zip(
        proc_name=name,
        proc_signature=signature,
        handler=handler,
        coverage_reports_stage=DEPLOYMENT_STAGE,
        coverage_reports_stage_path=f"/{artifact_stage_path}/coverage",
        zip_file_path=str(artifact_path),
    )

    return handler


@app.command("deploy")
@with_output
@global_options_with_connection
def deploy(
    install_coverage_wrapper: bool = InstallCoverageWrapper,
    replace: bool = ReplaceOption,
    **options,
) -> CommandResult:
    """Deploy procedures and functions defined in project."""
    dm = DefinitionManager()
    procedures = dm.project_definition.get("procedures", [])
    functions = dm.project_definition.get("functions", [])

    if not procedures and not functions:
        raise ClickException(
            "No procedures or functions were specified in project definition."
        )

    if (len(procedures) > 1 or functions) and install_coverage_wrapper:
        raise ClickException(
            "Using coverage wrapper is currently limited to project with single procedure."
        )

    build_artifact_path = Path("app.zip")
    # TODO: this should be configurable
    if not build_artifact_path.exists():
        raise ClickException(
            "Artifact required for deploying the project does not exist in this directory. "
            "Please use build command to create it."
        )

    stage_manager = StageManager()
    stage_manager.create(
        stage_name=DEPLOYMENT_STAGE, comment="deployments managed by snowcli"
    )

    packages = get_snowflake_packages()
    deploy_status = []

    # TODO: Check if any object already exists before we start updates
    # TODO: Deploy the artifact only once

    # Procedures
    pm = ProcedureManager()
    for procedure in procedures:
        operation_result = _deploy_single_object(
            manager=pm,
            object_type=SnowparkObjectType.PROCEDURE,
            object_definition=procedure,
            replace=replace,
            packages=packages,
            install_coverage_wrapper=install_coverage_wrapper,
            stage_manager=stage_manager,
            build_artifact_path=build_artifact_path,
        )
        deploy_status.append(operation_result)

    # Functions
    fm = FunctionManager()
    for function in functions:
        operation_result = _deploy_single_object(
            manager=fm,
            object_type=SnowparkObjectType.FUNCTION,
            object_definition=function,
            replace=replace,
            packages=packages,
            stage_manager=stage_manager,
            build_artifact_path=build_artifact_path,
        )
        deploy_status.append(operation_result)

    return CollectionResult(deploy_status)


def _deploy_single_object(
    manager: FunctionManager | ProcedureManager,
    object_type: SnowparkObjectType,
    object_definition: Dict,
    replace: bool,
    packages: List[str],
    stage_manager: StageManager,
    build_artifact_path: Path,
    install_coverage_wrapper: bool = False,
):
    identifier = build_udf_sproc_identifier(object_definition)
    log.info(f"Deploying {object_type.value}: {identifier}")
    handler = object_definition["handler"]
    returns = object_definition["returns"]
    object_exists = True
    replace_object = False
    current_state = None
    artifact_stage_path = manager.artifact_stage_path(identifier)
    artifact_stage_target = f"{DEPLOYMENT_STAGE}/{artifact_stage_path}"
    artifact_path_on_stage = f"{artifact_stage_target}/{build_artifact_path.name}"
    try:
        current_state = manager.describe(remove_parameter_names(identifier))
    except ProgrammingError as ex:
        if ex.msg.__contains__("does not exist or not authorized"):
            object_exists = False
            log.debug(f"{object_type.value.capitalize()} does not exists.")
        else:
            raise ex
    if object_exists and not replace:
        raise ObjectAlreadyExistsError(object_type, identifier, replace_available=True)
    if object_type == SnowparkObjectType.PROCEDURE and install_coverage_wrapper:
        # This changes existing artifact
        handler = _alter_procedure_artifact(
            artifact_path=build_artifact_path,
            handler=handler,
            identifier=identifier,
            artifact_stage_path=artifact_stage_path,
        )
        packages.append("coverage")
    if object_exists:
        replace_object = check_if_replace_is_required(
            object_type,
            current_state,
            handler,
            returns,
        )
    stage_manager.put(
        local_path=build_artifact_path,
        stage_path=artifact_stage_target,
        overwrite=True,
    )
    if not object_exists or replace_object:
        create_or_replace_kwargs = {
            "identifier": identifier,
            "handler": handler,
            "return_type": returns,
            "artifact_file": artifact_path_on_stage,
            "packages": packages,
        }
        if object_type == SnowparkObjectType.PROCEDURE:
            create_or_replace_kwargs["execute_as_caller"] = object_definition.get(
                "execute_as_caller"
            )

        manager.create_or_replace(**create_or_replace_kwargs)

        status = "created" if not object_exists else "definition updated"
        return {"object": identifier, "type": object_type.value, "status": status}
    else:
        return {
            "object": identifier,
            "type": object_type.value,
            "status": "packages updated",
        }


@app.command("build")
@global_options
@with_output
def build(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """Build the current project as a `.zip` file."""
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    return MessageResult("Done")


def _execute_object_method(
    method_name: str,
    object_type: SnowparkObjectType,
    **kwargs,
):
    if object_type == SnowparkObjectType.PROCEDURE:
        manager = ProcedureManager()
    elif object_type == SnowparkObjectType.FUNCTION:
        manager = FunctionManager()
    else:
        raise ClickException(f"Unknown object type: {object_type}")

    return getattr(manager, method_name)(**kwargs)


@app.command("execute")
@with_output
@global_options_with_connection
def execute(
    object_type: SnowparkObjectType = ObjectTypeArgument,
    execution_identifier: str = execution_identifier_argument(
        "procedure/function", "hello(1, 'world')"
    ),
    **options,
) -> CommandResult:
    """Executes a procedure or function in a specified environment."""
    cursor = _execute_object_method(
        "execute", object_type=object_type, execution_identifier=execution_identifier
    )
    return SingleQueryResult(cursor)


@app.command("describe")
@with_output
@global_options_with_connection
def describe(
    object_type: SnowparkObjectType = ObjectTypeArgument,
    identifier: str = identifier_argument(
        "procedure or function", "hello(int, string)"
    ),
    **options,
) -> CommandResult:
    """
    Describes the specified object, including the signature (i.e. arguments),
    return value, language, and body (i.e. definition).
    """
    cursor = _execute_object_method(
        "describe", object_type=object_type, identifier=identifier
    )
    return QueryResult(cursor)


@app.command("list")
@with_output
@global_options_with_connection
def list(
    object_type: SnowparkObjectType = ObjectTypeArgument,
    like: str = LikeOption,
    **options,
) -> CommandResult:
    """Lists all available procedures or functions."""
    cursor = _execute_object_method("show", object_type=object_type, like=like)
    return QueryResult(cursor)


@app.command("drop")
@with_output
@global_options_with_connection
def drop(
    object_type: SnowparkObjectType = ObjectTypeArgument,
    identifier: str = identifier_argument(
        "procedure or function", "hello(int, string)"
    ),
    **options,
) -> CommandResult:
    """Drops a Snowflake procedure or function."""
    cursor = _execute_object_method(
        "drop", object_type=object_type, identifier=identifier
    )
    return SingleQueryResult(cursor)


def _replace_handler_in_zip(
    proc_name: str,
    proc_signature: str,
    handler: str,
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
    with TemporaryDirectory() as temp_dir:
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
