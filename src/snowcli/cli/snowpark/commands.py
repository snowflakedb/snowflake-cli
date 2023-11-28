from __future__ import annotations

import logging
import os
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List

import typer
from click import ClickException
from snowcli import utils
from snowcli.cli.common.decorators import global_options, global_options_with_connection
from snowcli.cli.common.flags import (
    DEFAULT_CONTEXT_SETTINGS,
    execution_identifier_argument,
)
from snowcli.cli.common.project_initialisation import add_init_command
from snowcli.cli.constants import DEPLOYMENT_STAGE, ObjectType
from snowcli.cli.object.manager import ObjectManager
from snowcli.cli.object.stage.manager import StageManager
from snowcli.cli.project.definition_manager import DefinitionManager
from snowcli.cli.snowpark.common import (
    build_udf_sproc_identifier,
    check_if_replace_is_required,
    remove_parameter_names,
)
from snowcli.cli.snowpark.manager import FunctionManager, ProcedureManager
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependencies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
)
from snowcli.exception import (
    NoProjectDefinitionError,
    ObjectAlreadyExistsError,
    SecretsWithoutExternalAccessIntegrationError,
)
from snowcli.output.decorators import with_output
from snowcli.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowcli.utils import (
    get_snowflake_packages,
)
from snowcli.zipper import add_file_to_existing_zip
from snowflake.connector import ProgrammingError

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


def _alter_procedure_artifact_with_coverage_wrapper(
    artifact_stage_path: str,
    artifact_path: Path,
    handler: str,
    identifier: str,
    stage_name: str,
):
    signature_start_index = identifier.index("(")
    name = identifier[0:signature_start_index]
    signature = identifier[signature_start_index:]
    stage_directory = artifact_stage_path.rpartition("/")[0]
    handler = _replace_handler_in_zip(
        proc_name=name,
        proc_signature=signature,
        handler=handler,
        coverage_reports_stage=stage_name,
        coverage_reports_stage_path=f"{stage_directory}/coverage",
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
    """
    Deploys procedures and functions defined in project. Deploying the project alters all object defined in it.
    By default, if any of the objects exist already the commands will fail unless `--replace` flag is provided.
    All deployed object use the same artefact which is deployed only once.
    """
    snowpark = get_snowpark_project_definition()

    procedures = snowpark.get("procedures", [])
    functions = snowpark.get("functions", [])

    if not procedures and not functions:
        raise ClickException(
            "No procedures or functions were specified in project definition."
        )

    if (len(procedures) > 1 or functions) and install_coverage_wrapper:
        raise ClickException(
            "Using coverage wrapper is currently limited to project with single procedure."
        )

    build_artifact_path = _get_snowpark_artefact_path(snowpark)

    if not build_artifact_path.exists():
        raise ClickException(
            "Artifact required for deploying the project does not exist in this directory. "
            "Please use build command to create it."
        )

    stage_name = snowpark.get("stage_name", DEPLOYMENT_STAGE)
    stage_manager = StageManager()
    stage_manager.create(
        stage_name=stage_name, comment="deployments managed by snowcli"
    )

    packages = get_snowflake_packages()

    artifact_stage_directory = get_app_stage_path(snowpark)
    artifact_stage_target = f"{artifact_stage_directory}/{build_artifact_path.name}"

    pm = ProcedureManager()

    # Coverage case
    if install_coverage_wrapper:
        return _deploy_procedure_with_coverage(
            artifact_stage_directory=artifact_stage_directory,
            artifact_stage_target=artifact_stage_target,
            build_artifact_path=build_artifact_path,
            packages=packages,
            pm=pm,
            procedure=procedures[0],
            replace=replace,
            stage_manager=stage_manager,
            stage_name=stage_name,
        )

    # TODO: Check if any object already exists before we start updates
    stage_manager.put(
        local_path=build_artifact_path,
        stage_path=artifact_stage_directory,
        overwrite=True,
    )

    deploy_status = []
    # Procedures
    for procedure in procedures:
        operation_result = _deploy_single_object(
            manager=pm,
            object_type=ObjectType.PROCEDURE,
            object_definition=procedure,
            replace=replace,
            packages=packages,
            stage_artifact_path=artifact_stage_target,
        )
        deploy_status.append(operation_result)

    # Functions
    fm = FunctionManager()
    for function in functions:
        operation_result = _deploy_single_object(
            manager=fm,
            object_type=ObjectType.FUNCTION,
            object_definition=function,
            replace=replace,
            packages=packages,
            stage_artifact_path=artifact_stage_target,
        )
        deploy_status.append(operation_result)

    return CollectionResult(deploy_status)


def _deploy_procedure_with_coverage(
    artifact_stage_directory: str,
    artifact_stage_target: str,
    build_artifact_path: Path,
    packages: List,
    pm: ProcedureManager,
    procedure: Dict,
    replace: bool,
    stage_manager: StageManager,
    stage_name: str,
):
    # This changes existing artifact so we need to generate wrapper and only then deploy
    handler = _alter_procedure_artifact_with_coverage_wrapper(
        artifact_path=build_artifact_path,
        handler=procedure["handler"],
        identifier=build_udf_sproc_identifier(procedure),
        artifact_stage_path=artifact_stage_target,
        stage_name=stage_name,
    )
    stage_manager.put(
        local_path=build_artifact_path,
        stage_path=artifact_stage_directory,
        overwrite=True,
    )
    procedure["handler"] = handler
    packages.append("coverage")
    operation_result = _deploy_single_object(
        manager=pm,
        object_type=ObjectType.PROCEDURE,
        object_definition=procedure,
        replace=replace,
        packages=packages,
        stage_artifact_path=artifact_stage_target,
    )
    return CollectionResult([operation_result])


def get_snowpark_project_definition():
    dm = DefinitionManager()
    snowpark = dm.project_definition.get("snowpark")
    if not snowpark:
        raise NoProjectDefinitionError(dm.BASE_DEFINITION_FILENAME)
    return snowpark


def get_app_stage_path(snowpark):
    artifact_stage_directory = (
        f"@{snowpark.get('stage_name', DEPLOYMENT_STAGE)}/{snowpark['project_name']}"
    )
    return artifact_stage_directory


def _deploy_single_object(
    manager: FunctionManager | ProcedureManager,
    object_type: ObjectType,
    object_definition: Dict,
    replace: bool,
    packages: List[str],
    stage_artifact_path: str,
):
    identifier = build_udf_sproc_identifier(object_definition)
    log.info(f"Deploying {object_type}: {identifier}")
    handler = object_definition["handler"]
    returns = object_definition["returns"]
    object_exists = True
    replace_object = False
    current_state = None
    try:
        current_state = ObjectManager().describe(
            object_type=str(object_type), name=remove_parameter_names(identifier)
        )
    except ProgrammingError as ex:
        if ex.msg.__contains__("does not exist or not authorized"):
            object_exists = False
            log.debug(f"{str(object_type).capitalize()} does not exists.")
        else:
            raise ex
    if object_exists and not replace:
        raise ObjectAlreadyExistsError(object_type, identifier, replace_available=True)

    external_access_integrations = object_definition.get("external_access_integrations")
    secrets = object_definition.get("secrets")
    if not external_access_integrations and secrets:
        raise SecretsWithoutExternalAccessIntegrationError()

    if object_exists:
        replace_object = check_if_replace_is_required(
            object_type,
            current_state,
            handler,
            returns,
        )

    if object_exists and not replace_object:
        return {
            "object": identifier,
            "type": str(object_type),
            "status": "packages updated",
        }

    create_or_replace_kwargs = {
        "identifier": identifier,
        "handler": handler,
        "return_type": returns,
        "artifact_file": stage_artifact_path,
        "packages": packages,
        "external_access_integrations": external_access_integrations,
        "secrets": secrets,
    }
    if object_type == ObjectType.PROCEDURE:
        create_or_replace_kwargs["execute_as_caller"] = object_definition.get(
            "execute_as_caller"
        )

    manager.create_or_replace(**create_or_replace_kwargs)

    status = "created" if not object_exists else "definition updated"
    return {"object": identifier, "type": str(object_type), "status": status}


def _get_snowpark_artefact_path(snowpark_definition: Dict):
    source = Path(snowpark_definition["src"])
    artefact_file = Path.cwd() / (source.name + ".zip")
    return artefact_file


@app.command("build")
@global_options
@with_output
def build(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """
    Builds the Snowpark project as a `.zip` archive that can be used by `deploy` command.
    The archive is built using only `src` directory specified in project file.
    """
    snowpark = get_snowpark_project_definition()
    source = Path(snowpark.get("src"))
    artefact_file = _get_snowpark_artefact_path(snowpark)
    log.info("Building package using sources from: %s", source.resolve())

    snowpark_package(
        source=source,
        artefact_file=artefact_file,
        pypi_download=pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps=check_anaconda_for_pypi_deps,
        package_native_libraries=package_native_libraries,  # type: ignore[arg-type]
    )
    return MessageResult(f"Build done. Artefact path: {artefact_file}")


class _SnowparkObject(Enum):
    """This clas is used only for snowpark execute where choice is limited."""

    PROCEDURE = str(ObjectType.PROCEDURE)
    FUNCTION = str(ObjectType.FUNCTION)


def _execute_object_method(
    method_name: str,
    object_type: _SnowparkObject,
    **kwargs,
):
    if object_type == _SnowparkObject.PROCEDURE:
        manager = ProcedureManager()
    elif object_type == _SnowparkObject.FUNCTION:
        manager = FunctionManager()
    else:
        raise ClickException(f"Unknown object type: {object_type}")

    return getattr(manager, method_name)(**kwargs)


@app.command("execute")
@with_output
@global_options_with_connection
def execute(
    object_type: _SnowparkObject = ObjectTypeArgument,
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
    handler_module, _, handler_function = handler.rpartition(".")
    with TemporaryDirectory() as temp_dir:
        wrapper_file = os.path.join(temp_dir, "snowpark_coverage.py")
        utils.generate_snowpark_coverage_wrapper(
            target_file=wrapper_file,
            proc_name=proc_name,
            proc_signature=proc_signature,
            coverage_reports_stage_path=coverage_reports_stage_path,
            handler_module=handler_module,
            handler_function=handler_function,
        )
        add_file_to_existing_zip(zip_file=zip_file_path, file=wrapper_file)
    return "snowpark_coverage.measure_coverage"
