from __future__ import annotations

import logging
from enum import Enum
from typing import Dict, List, Optional, Set

import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.flags import (
    ReplaceOption,
    deprecated_flag_callback_enum,
    execution_identifier_argument,
)
from snowflake.cli.api.commands.project_initialisation import add_init_command
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.constants import (
    DEFAULT_SIZE_LIMIT_MB,
    DEPLOYMENT_STAGE,
    ObjectType,
)
from snowflake.cli.api.exceptions import (
    SecretsWithoutExternalAccessIntegrationError,
)
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.schemas.snowpark.callable import (
    Callable,
    FunctionSchema,
    ProcedureSchema,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.object.manager import ObjectManager
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.common import (
    build_udf_sproc_identifier,
    check_if_replace_is_required,
)
from snowflake.cli.plugins.snowpark.manager import FunctionManager, ProcedureManager
from snowflake.cli.plugins.snowpark.models import YesNoAsk
from snowflake.cli.plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AnacondaPackagesManager,
)
from snowflake.cli.plugins.snowpark.snowpark_package_paths import SnowparkPackagePaths
from snowflake.cli.plugins.snowpark.snowpark_shared import (
    AllowSharedLibrariesOption,
    DeprecatedCheckAnacondaForPyPiDependencies,
    IgnoreAnacondaOption,
    IndexUrlOption,
    SkipVersionCheckOption,
    deprecated_allow_native_libraries_option,
    resolve_allow_shared_libraries_yes_no_ask,
)
from snowflake.cli.plugins.snowpark.zipper import zip_dir
from snowflake.cli.plugins.stage.manager import StageManager
from snowflake.connector import DictCursor, ProgrammingError

log = logging.getLogger(__name__)

app = SnowTyper(
    name="snowpark",
    help="Manages procedures and functions.",
)

ObjectTypeArgument = typer.Argument(
    help="Type of Snowpark object",
    case_sensitive=False,
)

add_init_command(app, project_type="Snowpark", template="default_snowpark")


@app.command("deploy", requires_connection=True)
@with_project_definition("snowpark")
def deploy(
    replace: bool = ReplaceOption(
        help="Replaces procedure or function, even if no detected changes to metadata"
    ),
    **options,
) -> CommandResult:
    """
    Deploys procedures and functions defined in project. Deploying the project alters all objects defined in it.
    By default, if any of the objects exist already the commands will fail unless `--replace` flag is provided.
    All deployed objects use the same artifact which is deployed only once.
    """
    snowpark = cli_context.project_definition
    paths = SnowparkPackagePaths.for_snowpark_project(
        project_root=SecurePath(cli_context.project_root),
        snowpark_project_definition=snowpark,
    )

    procedures = snowpark.procedures
    functions = snowpark.functions

    if not procedures and not functions:
        raise ClickException(
            "No procedures or functions were specified in the project definition."
        )

    if not paths.artifact_file.exists():
        raise ClickException(
            "Artifact required for deploying the project does not exist in this directory. "
            "Please use build command to create it."
        )

    pm = ProcedureManager()
    fm = FunctionManager()
    om = ObjectManager()

    _assert_object_definitions_are_correct("function", functions)
    _assert_object_definitions_are_correct("procedure", procedures)
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
    stage_name = snowpark.stage_name
    stage_manager = StageManager()
    stage_name = stage_manager.to_fully_qualified_name(stage_name)
    stage_manager.create(
        stage_name=stage_name, comment="deployments managed by Snowflake CLI"
    )

    snowflake_dependencies = _read_snowflake_requrements_file(
        paths.snowflake_requirements_file
    )

    artifact_stage_directory = get_app_stage_path(stage_name, snowpark.project_name)
    artifact_stage_target = (
        f"{artifact_stage_directory}/{paths.artifact_file.path.name}"
    )

    stage_manager.put(
        local_path=paths.artifact_file.path,
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
            snowflake_dependencies=snowflake_dependencies,
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
            snowflake_dependencies=snowflake_dependencies,
            stage_artifact_path=artifact_stage_target,
        )
        deploy_status.append(operation_result)

    return CollectionResult(deploy_status)


def _assert_object_definitions_are_correct(
    object_type, object_definitions: List[Callable]
):
    for definition in object_definitions:
        database = definition.database
        schema = definition.schema_name
        name = definition.name
        fqn_parts = len(name.split("."))
        if fqn_parts == 3 and database:
            raise ClickException(
                f"database of {object_type} {name} is redefined in its name"
            )
        if fqn_parts >= 2 and schema:
            raise ClickException(
                f"schema of {object_type} {name} is redefined in its name"
            )


def _find_existing_objects(
    object_type: ObjectType,
    objects: List[Dict],
    om: ObjectManager,
):
    existing_objects = {}
    for object_definition in objects:
        identifier = build_udf_sproc_identifier(
            object_definition, om, include_parameter_names=False
        )
        try:
            current_state = om.describe(
                object_type=object_type.value.sf_name,
                name=identifier,
            )
            existing_objects[identifier] = current_state
        except ProgrammingError:
            pass
    return existing_objects


def _check_if_all_defined_integrations_exists(
    om: ObjectManager,
    functions: List[FunctionSchema],
    procedures: List[ProcedureSchema],
):
    existing_integrations = {
        i["name"].lower()
        for i in om.show(object_type="integration", cursor_class=DictCursor, like=None)
        if i["type"] == "EXTERNAL_ACCESS"
    }
    declared_integration: Set[str] = set()
    for object_definition in [*functions, *procedures]:
        external_access_integrations = {
            s.lower() for s in object_definition.external_access_integrations
        }
        secrets = [s.lower() for s in object_definition.secrets]

        if not external_access_integrations and secrets:
            raise SecretsWithoutExternalAccessIntegrationError(object_definition.name)

        declared_integration = declared_integration | external_access_integrations

    missing = declared_integration - existing_integrations
    if missing:
        raise ClickException(
            f"Following external access integration does not exists in Snowflake: {', '.join(missing)}"
        )


def get_app_stage_path(stage_name: Optional[str], project_name: str) -> str:
    artifact_stage_directory = f"@{(stage_name or DEPLOYMENT_STAGE)}/{project_name}"
    return artifact_stage_directory


def _deploy_single_object(
    manager: FunctionManager | ProcedureManager,
    object_type: ObjectType,
    object_definition: Callable,
    existing_objects: Dict[str, Dict],
    snowflake_dependencies: List[str],
    stage_artifact_path: str,
):
    identifier = build_udf_sproc_identifier(
        object_definition, manager, include_parameter_names=False
    )
    identifier_with_default_values = build_udf_sproc_identifier(
        object_definition,
        manager,
        include_parameter_names=True,
        include_default_values=True,
    )
    log.info("Deploying %s: %s", object_type, identifier_with_default_values)

    handler = object_definition.handler
    returns = object_definition.returns
    imports = object_definition.imports
    replace_object = False

    object_exists = identifier in existing_objects
    if object_exists:
        replace_object = check_if_replace_is_required(
            object_type=object_type,
            current_state=existing_objects[identifier],
            handler=handler,
            return_type=returns,
            snowflake_dependencies=snowflake_dependencies,
            imports=imports,
            stage_artifact_file=stage_artifact_path,
        )

    if object_exists and not replace_object:
        return {
            "object": identifier_with_default_values,
            "type": str(object_type),
            "status": "packages updated",
        }

    create_or_replace_kwargs = {
        "identifier": identifier_with_default_values,
        "handler": handler,
        "return_type": returns,
        "artifact_file": stage_artifact_path,
        "packages": snowflake_dependencies,
        "runtime": object_definition.runtime,
        "external_access_integrations": object_definition.external_access_integrations,
        "secrets": object_definition.secrets,
        "imports": imports,
    }
    if object_type == ObjectType.PROCEDURE:
        create_or_replace_kwargs[
            "execute_as_caller"
        ] = object_definition.execute_as_caller
    manager.create_or_replace(**create_or_replace_kwargs)

    status = "created" if not object_exists else "definition updated"
    return {
        "object": identifier_with_default_values,
        "type": str(object_type),
        "status": status,
    }


deprecated_pypi_download_option = typer.Option(
    YesNoAsk.NO.value,
    "--pypi-download",
    help="Whether to download non-Anaconda packages from PyPi.",
    hidden=True,
    callback=deprecated_flag_callback_enum(
        "--pypi-download flag is deprecated. Snowpark build command"
        " always tries to download non-Anaconda packages from external index (PyPi by default)."
    ),
)


def _read_snowflake_requrements_file(file_path: SecurePath):
    if not file_path.exists():
        return []
    return file_path.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB).splitlines()


@app.command("build", requires_connection=True)
@with_project_definition("snowpark")
def build(
    ignore_anaconda: bool = IgnoreAnacondaOption,
    allow_shared_libraries: bool = AllowSharedLibrariesOption,
    index_url: Optional[str] = IndexUrlOption,
    skip_version_check: bool = SkipVersionCheckOption,
    deprecated_package_native_libraries: YesNoAsk = deprecated_allow_native_libraries_option(
        "--package-native-libraries"
    ),
    deprecated_check_anaconda_for_pypi_deps: bool = DeprecatedCheckAnacondaForPyPiDependencies,
    _deprecated_pypi_download: YesNoAsk = deprecated_pypi_download_option,
    **options,
) -> CommandResult:
    """
    Builds the Snowpark project as a `.zip` archive that can be used by `deploy` command.
    The archive is built using only the `src` directory specified in the project file.
    """
    if not deprecated_check_anaconda_for_pypi_deps:
        ignore_anaconda = True
    snowpark_paths = SnowparkPackagePaths.for_snowpark_project(
        project_root=SecurePath(cli_context.project_root),
        snowpark_project_definition=cli_context.project_definition,
    )
    log.info("Building package using sources from: %s", snowpark_paths.source.path)

    anaconda_packages_manager = AnacondaPackagesManager()

    with SecurePath.temporary_directory() as packages_dir:
        if snowpark_paths.defined_requirements_file.exists():
            log.info("Resolving any requirements from requirements.txt...")
            requirements = package_utils.parse_requirements(
                requirements_file=snowpark_paths.defined_requirements_file,
            )
            anaconda_packages = (
                AnacondaPackages.empty()
                if ignore_anaconda
                else anaconda_packages_manager.find_packages_available_in_snowflake_anaconda()
            )
            download_result = package_utils.download_unavailable_packages(
                requirements=requirements,
                target_dir=packages_dir,
                anaconda_packages=anaconda_packages,
                skip_version_check=skip_version_check,
                pip_index_url=index_url,
            )
            if not download_result.succeeded:
                raise ClickException(download_result.error_message)

            log.info("Checking to see if packages have shared (.so/.dll) libraries...")
            if package_utils.detect_and_log_shared_libraries(
                download_result.downloaded_packages_details
            ):
                # TODO: yes/no/ask logic should be removed in 3.0
                if not (
                    allow_shared_libraries
                    or resolve_allow_shared_libraries_yes_no_ask(
                        deprecated_package_native_libraries
                    )
                ):
                    raise ClickException(
                        "Some packages contain shared (.so/.dll) libraries. "
                        "Try again with --allow-shared-libraries."
                    )
            if download_result.anaconda_packages:
                anaconda_packages.write_requirements_file_in_snowflake_format(  # type: ignore
                    file_path=snowpark_paths.snowflake_requirements_file,
                    requirements=download_result.anaconda_packages,
                )

        zip_dir(
            source=snowpark_paths.source.path,
            dest_zip=snowpark_paths.artifact_file.path,
        )
        if any(packages_dir.iterdir()):
            # if any packages were generated, append them to the .zip
            zip_dir(
                source=packages_dir.path,
                dest_zip=snowpark_paths.artifact_file.path,
                mode="a",
            )

    log.info("Package now ready: %s", snowpark_paths.artifact_file.path)

    return MessageResult(
        f"Build done. Artifact path: {snowpark_paths.artifact_file.path}"
    )


class _SnowparkObject(Enum):
    """This clas is used only for Snowpark execute where choice is limited."""

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


@app.command("execute", requires_connection=True)
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
