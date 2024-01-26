from __future__ import annotations

import logging
from enum import Enum
from pathlib import Path
from typing import Dict, List, Set

import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    global_options,
    global_options_with_connection,
    with_output,
    with_project_definition,
)
from snowflake.cli.api.commands.flags import (
    DEFAULT_CONTEXT_SETTINGS,
    execution_identifier_argument,
)
from snowflake.cli.api.commands.project_initialisation import add_init_command
from snowflake.cli.api.constants import DEPLOYMENT_STAGE, ObjectType
from snowflake.cli.api.exceptions import (
    SecretsWithoutExternalAccessIntegrationError,
)
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.plugins.object.manager import ObjectManager
from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.cli.plugins.snowpark.common import (
    build_udf_sproc_identifier,
    check_if_replace_is_required,
    remove_parameter_names,
)
from snowflake.cli.plugins.snowpark.manager import FunctionManager, ProcedureManager
from snowflake.cli.plugins.snowpark.models import PypiOption
from snowflake.cli.plugins.snowpark.package_utils import get_snowflake_packages
from snowflake.cli.plugins.snowpark.snowpark_shared import (
    CheckAnacondaForPyPiDependencies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
)
from snowflake.connector import DictCursor, ProgrammingError

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

ObjectTypeArgument = typer.Argument(
    help="Type of snowpark object",
    case_sensitive=False,
)

add_init_command(app, project_type="snowpark", template="default_snowpark")


@app.command("deploy")
@with_output
@with_project_definition("snowpark")
@global_options_with_connection
def deploy(
    replace: bool = ReplaceOption,
    **options,
) -> CommandResult:
    """
    Deploys procedures and functions defined in project. Deploying the project alters all objects defined in it.
    By default, if any of the objects exist already the commands will fail unless `--replace` flag is provided.
    All deployed objects use the same artifact which is deployed only once.
    """
    snowpark = cli_context.project_definition

    procedures = snowpark.get("procedures", [])
    functions = snowpark.get("functions", [])

    if not procedures and not functions:
        raise ClickException(
            "No procedures or functions were specified in the project definition."
        )

    build_artifact_path = _get_snowpark_artifact_path(snowpark)

    if not build_artifact_path.exists():
        raise ClickException(
            "Artifact required for deploying the project does not exist in this directory. "
            "Please use build command to create it."
        )

    pm = ProcedureManager()
    fm = FunctionManager()
    om = ObjectManager()

    _check_if_all_defined_integrations_exists(om, functions, procedures)

    existing_functions = _find_existing_objects(ObjectType.FUNCTION, functions, om)
    existing_procedures = _find_existing_objects(ObjectType.PROCEDURE, procedures, om)

    if (existing_functions or existing_procedures) and not replace:
        msg = "Following objects already exists. Consider using --replace.\n"
        msg += "\n".join(f"function: {n}" for n in existing_functions)
        msg += "\n" if existing_functions and existing_procedures else ""
        msg += "\n".join(f"procedure: {n}" for n in existing_procedures)
        raise ClickException(msg)

    # Create stage
    stage_name = snowpark.get("stage_name", DEPLOYMENT_STAGE)
    stage_manager = StageManager()
    stage_manager.create(
        stage_name=stage_name, comment="deployments managed by snowcli"
    )

    packages = get_snowflake_packages()

    artifact_stage_directory = get_app_stage_path(snowpark)
    artifact_stage_target = f"{artifact_stage_directory}/{build_artifact_path.name}"

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
            existing_objects=existing_procedures,
            packages=packages,
            stage_artifact_path=artifact_stage_target,
        )
        deploy_status.append(operation_result)

    # Functions
    for function in functions:
        operation_result = _deploy_single_object(
            manager=fm,
            object_type=ObjectType.FUNCTION,
            object_definition=function,
            existing_objects=existing_functions,
            packages=packages,
            stage_artifact_path=artifact_stage_target,
        )
        deploy_status.append(operation_result)

    return CollectionResult(deploy_status)


def _find_existing_objects(
    object_type: ObjectType, objects: List[Dict], om: ObjectManager
):
    existing_objects = {}
    for object_definition in objects:
        identifier = build_udf_sproc_identifier(object_definition)
        try:
            current_state = om.describe(
                object_type=object_type.value.sf_name,
                name=remove_parameter_names(identifier),
            )
            existing_objects[identifier] = current_state
        except ProgrammingError:
            pass
    return existing_objects


def _check_if_all_defined_integrations_exists(
    om: ObjectManager, functions: List[Dict], procedures: List[Dict]
):
    existing_integrations = {
        i["name"].lower()
        for i in om.show(object_type="integration", cursor_class=DictCursor, like=None)
        if i["type"] == "EXTERNAL_ACCESS"
    }
    declared_integration: Set[str] = set()
    for object_definition in [*functions, *procedures]:
        external_access_integrations = {
            s.lower() for s in object_definition.get("external_access_integrations", [])
        }
        secrets = [s.lower() for s in object_definition.get("secrets", [])]

        if not external_access_integrations and secrets:
            raise SecretsWithoutExternalAccessIntegrationError(
                object_definition["name"]
            )

        declared_integration = declared_integration | external_access_integrations

    missing = declared_integration - existing_integrations
    if missing:
        raise ClickException(
            f"Following external access integration does not exists in Snowflake: {', '.join(missing)}"
        )


def get_app_stage_path(snowpark):
    artifact_stage_directory = (
        f"@{snowpark.get('stage_name', DEPLOYMENT_STAGE)}/{snowpark['project_name']}"
    )
    return artifact_stage_directory


def _deploy_single_object(
    manager: FunctionManager | ProcedureManager,
    object_type: ObjectType,
    object_definition: Dict,
    existing_objects: Dict[str, Dict],
    packages: List[str],
    stage_artifact_path: str,
):
    identifier = build_udf_sproc_identifier(object_definition)
    log.info("Deploying %s: %s", object_type, identifier)

    handler = object_definition["handler"]
    returns = object_definition["returns"]
    replace_object = False

    object_exists = identifier in existing_objects
    if object_exists:
        replace_object = check_if_replace_is_required(
            object_type,
            existing_objects[identifier],
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
        "runtime": object_definition.get("runtime"),
        "external_access_integrations": object_definition.get(
            "external_access_integrations"
        ),
        "secrets": object_definition.get("secrets"),
        "imports": object_definition.get("imports", []),
    }
    if object_type == ObjectType.PROCEDURE:
        create_or_replace_kwargs["execute_as_caller"] = object_definition.get(
            "execute_as_caller"
        )

    manager.create_or_replace(**create_or_replace_kwargs)

    status = "created" if not object_exists else "definition updated"
    return {"object": identifier, "type": str(object_type), "status": status}


def _get_snowpark_artifact_path(snowpark_definition: Dict):
    source = Path(snowpark_definition["src"])
    artifact_file = Path.cwd() / (source.name + ".zip")
    return artifact_file


@app.command("build")
@global_options
@with_output
@with_project_definition("snowpark")
def build(
    pypi_download: PypiOption = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependencies,
    package_native_libraries: PypiOption = PackageNativeLibrariesOption,
    **options,
) -> CommandResult:
    """
    Builds the Snowpark project as a `.zip` archive that can be used by `deploy` command.
    The archive is built using only the `src` directory specified in the project file.
    """
    snowpark = cli_context.project_definition
    source = Path(snowpark.get("src"))
    artifact_file = _get_snowpark_artifact_path(snowpark)
    log.info("Building package using sources from: %s", source.resolve())

    snowpark_package(
        source=source,
        artifact_file=artifact_file,
        pypi_download=pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps=check_anaconda_for_pypi_deps,
        package_native_libraries=package_native_libraries,  # type: ignore[arg-type]
    )
    return MessageResult(f"Build done. Artifact path: {artifact_file}")


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
