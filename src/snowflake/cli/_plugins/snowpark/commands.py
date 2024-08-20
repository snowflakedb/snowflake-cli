# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import typer
from click import ClickException, UsageError
from snowflake.cli._plugins.object.commands import (
    describe as object_describe,
)
from snowflake.cli._plugins.object.commands import (
    drop as object_drop,
)
from snowflake.cli._plugins.object.commands import (
    list_ as object_list,
)
from snowflake.cli._plugins.object.commands import (
    scope_option,
)
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.snowpark import package_utils
from snowflake.cli._plugins.snowpark.common import (
    check_if_replace_is_required,
)
from snowflake.cli._plugins.snowpark.manager import FunctionManager, ProcedureManager
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AnacondaPackagesManager,
)
from snowflake.cli._plugins.snowpark.package.commands import app as package_app
from snowflake.cli._plugins.snowpark.snowpark_package_paths import SnowparkPackagePaths
from snowflake.cli._plugins.snowpark.snowpark_shared import (
    AllowSharedLibrariesOption,
    IgnoreAnacondaOption,
    IndexUrlOption,
    SkipVersionCheckOption,
)
from snowflake.cli._plugins.snowpark.zipper import zip_dir
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.cli_global_context import (
    get_cli_context,
)
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.flags import (
    ReplaceOption,
    execution_identifier_argument,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import (
    DEFAULT_SIZE_LIMIT_MB,
    DEPLOYMENT_STAGE,
    ObjectType,
)
from snowflake.cli.api.exceptions import (
    NoProjectDefinitionError,
    SecretsWithoutExternalAccessIntegrationError,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.schemas.entities.snowpark_entity import (
    FunctionEntityModel,
    ProcedureEntityModel,
    SnowparkEntityModel,
)
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
    ProjectDefinitionV2,
)
from snowflake.cli.api.project.schemas.snowpark.callable import (
    FunctionSchema,
    ProcedureSchema,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector import DictCursor, ProgrammingError

log = logging.getLogger(__name__)

app = SnowTyperFactory(
    name="snowpark",
    help="Manages procedures and functions.",
)
app.add_typer(package_app)

ObjectTypeArgument = typer.Argument(
    help="Type of Snowpark object",
    case_sensitive=False,
    show_default=False,
)
IdentifierArgument = identifier_argument(
    "function/procedure",
    example="hello(int, string)",
)
LikeOption = like_option(
    help_example='`list function --like "my%"` lists all functions that begin with “my”',
)


@app.command("deploy", requires_connection=True)
@with_project_definition()
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
    cli_context = get_cli_context()
    pd = _get_v2_project_definition(cli_context)

    snowpark_entities = get_snowpark_entities(pd)

    with cli_console.phase("Performing initial validation"):
        if not snowpark_entities:
            raise ClickException(
                "No procedures or functions were specified in the project definition."
            )

        paths = SnowparkPackagePaths.for_snowpark_project(
            project_root=SecurePath(cli_context.project_root),
            snowpark_entities=snowpark_entities,
        )
        for key, entity in snowpark_entities.items():
            for artefact in entity.artifacts:
                path = (
                    artefact if artefact.is_file() else zip_file_name_for_dir(artefact)
                )
                if not (cli_context.project_root / path).exists():
                    raise UsageError(
                        f"Artefact {path} required for {entity.type} {key} does not exist."
                    )

    # Validate current state
    with cli_console.phase("Checking remote state"):
        om = ObjectManager()
        _check_if_all_defined_integrations_exists(om, snowpark_entities)
        existing_objects: Dict[str, Any] = _find_existing_objects(snowpark_entities, om)

        if existing_objects and not replace:
            existing_entities = [snowpark_entities[e] for e in existing_objects]
            msg = "Following objects already exists. Consider using --replace.\n"
            msg += "\n".join(f"{e.type}: {e.entity_id}" for e in existing_entities)
            raise ClickException(msg)

    with cli_console.phase("Preparing required stages and artifacts"):
        # Prepare artefact deployment strategy
        stages_to_artifact_map: Dict[str, set[tuple[Path, str]]] = defaultdict(set)
        entities_to_imports_map: Dict[str, set[str]] = defaultdict(set)

        if pd.defaults and pd.defaults.project_name:
            project_name = pd.defaults.project_name
        else:
            project_name = ""

        for entity_id, entity in snowpark_entities.items():
            stage = entity.stage
            remote_path = get_app_stage_path(stage, project_name)

            required_artifacts = set()
            for artefact in entity.artifacts:
                artefact_root_based_path = (
                    paths.get_snowpark_project_source_absolute_path(
                        cli_context.project_root, artefact
                    ).path
                )
                local_artifact = (
                    zip_file_name_for_dir(artefact_root_based_path)
                    if artefact_root_based_path.is_dir()
                    else artefact_root_based_path
                )

                required_artifacts.add((local_artifact, remote_path))

                import_path = remote_path + local_artifact.name
                entities_to_imports_map[entity_id].add(import_path)

            # This is not optimal, do only once
            if paths.dependencies_zip.exists():
                required_artifacts.add((paths.dependencies_zip.path, remote_path))
                entities_to_imports_map[entity_id].add(
                    remote_path + paths.dependencies_zip.name
                )

            stages_to_artifact_map[stage].update(required_artifacts)

        # Create stages and upload artifacts
        stage_manager = StageManager()
        for stage, artifacts in stages_to_artifact_map.items():
            cli_console.step(f"Creating stage: {stage}")
            stage = FQN.from_string(stage).using_context()
            stage_manager.create(
                fqn=stage, comment="deployments managed by Snowflake CLI"
            )
            for local_path, stage_path in artifacts:
                cli_console.step(f"Uploading {local_path.name} to {remote_path}")
                stage_manager.put(
                    local_path=local_path,
                    stage_path=stage_path,
                    overwrite=True,
                )

    # Create snowpark entities
    snowflake_dependencies = _read_snowflake_requirements_file(
        paths.snowflake_requirements_file
    )
    deploy_status = []

    with cli_console.phase("Creating Snowpark entities"):
        for entity in snowpark_entities.values():
            cli_console.step(f"Creating {entity.type} {entity.fqn}")
            operation_result = _deploy_single_object(
                entity=entity,
                existing_objects=existing_objects,
                snowflake_dependencies=snowflake_dependencies,
                entities_to_artifact_map=entities_to_imports_map,
            )
            deploy_status.append(operation_result)

    return CollectionResult(deploy_status)


def _find_existing_objects(
    objects: Dict[str, SnowparkEntityModel],
    om: ObjectManager,
) -> Dict[str, Any]:
    existing_objects = {}
    for entity_id, entity in objects.items():
        identifier = entity.udf_sproc_identifier.identifier_with_arg_types
        try:
            current_state = om.describe(
                object_type=entity.type,
                fqn=FQN.from_string(identifier),
            )
            existing_objects[entity_id] = current_state
        except ProgrammingError:
            pass
    return existing_objects


def _check_if_all_defined_integrations_exists(
    om: ObjectManager,
    snowpark_entities: Dict[str, FunctionEntityModel | ProcedureEntityModel],
):
    existing_integrations = {
        i["name"].lower()
        for i in om.show(object_type="integration", cursor_class=DictCursor, like=None)
        if i["type"] == "EXTERNAL_ACCESS"
    }
    declared_integration: Set[str] = set()
    for object_definition in snowpark_entities.values():
        external_access_integrations = {
            s.lower() for s in object_definition.external_access_integrations
        }
        secrets = [s.lower() for s in object_definition.secrets]

        if not external_access_integrations and secrets:
            raise SecretsWithoutExternalAccessIntegrationError(object_definition.fqn)

        declared_integration = declared_integration | external_access_integrations

    missing = declared_integration - existing_integrations
    if missing:
        raise ClickException(
            f"Following external access integration does not exists in Snowflake: {', '.join(missing)}"
        )


def get_app_stage_path(
    stage_name: Optional[str | FQN], project_name: str | None
) -> str:
    stage = stage_name or DEPLOYMENT_STAGE
    artifact_stage_directory = f"@{FQN.from_string(stage).using_context()}/"
    if project_name:
        artifact_stage_directory += f"{project_name}/"
    return artifact_stage_directory


def _deploy_single_object(
    entity: SnowparkEntityModel,
    existing_objects: Dict[str, Any],
    snowflake_dependencies: List[str],
    entities_to_artifact_map: Dict[str, set[str]],
):

    object_type = entity.get_type()
    is_procedure = isinstance(entity, ProcedureEntityModel)

    log.info(
        "Deploying %s: %s",
        object_type,
        entity.udf_sproc_identifier.identifier_with_arg_names_types,
    )

    handler = entity.handler
    returns = entity.returns
    imports = entity.imports
    external_access_integrations = entity.external_access_integrations
    runtime_ver = entity.runtime
    execute_as_caller = None
    if is_procedure:
        execute_as_caller = entity.execute_as_caller
    replace_object = False

    object_exists = entity.entity_id in existing_objects
    if object_exists:
        replace_object = check_if_replace_is_required(
            object_type=object_type,
            current_state=existing_objects[entity.entity_id],
            handler=handler,
            return_type=returns,
            snowflake_dependencies=snowflake_dependencies,
            external_access_integrations=external_access_integrations,
            imports=imports,
            stage_artifact_files=entities_to_artifact_map[entity.entity_id],
            runtime_ver=runtime_ver,
            execute_as_caller=execute_as_caller,
        )

    if object_exists and not replace_object:
        return {
            "object": entity.udf_sproc_identifier.identifier_with_arg_names_types_defaults,
            "type": str(object_type),
            "status": "packages updated",
        }

    create_or_replace_kwargs = {
        "identifier": entity.udf_sproc_identifier,
        "handler": handler,
        "return_type": returns,
        "artifact_files": entities_to_artifact_map[entity.entity_id],
        "packages": snowflake_dependencies,
        "runtime": entity.runtime,
        "external_access_integrations": entity.external_access_integrations,
        "secrets": entity.secrets,
        "imports": imports,
    }
    if is_procedure:
        create_or_replace_kwargs["execute_as_caller"] = entity.execute_as_caller

    manager = ProcedureManager() if is_procedure else FunctionManager()
    manager.create_or_replace(**create_or_replace_kwargs)

    status = "created" if not object_exists else "definition updated"
    return {
        "object": entity.udf_sproc_identifier.identifier_with_arg_names_types_defaults,
        "type": str(object_type),
        "status": status,
    }


def _read_snowflake_requirements_file(file_path: SecurePath):
    if not file_path.exists():
        return []
    return file_path.read_text(file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB).splitlines()


@app.command("build", requires_connection=True)
@with_project_definition()
def build(
    ignore_anaconda: bool = IgnoreAnacondaOption,
    allow_shared_libraries: bool = AllowSharedLibrariesOption,
    index_url: Optional[str] = IndexUrlOption,
    skip_version_check: bool = SkipVersionCheckOption,
    **options,
) -> CommandResult:
    """
    Builds artifacts required for the Snowpark project. The artifacts can be used by `deploy` command.
    For each directory in artifacts a .zip file is created. All non-anaconda dependencies are packaged in
    dependencies.zip file.
    """
    cli_context = get_cli_context()
    pd = _get_v2_project_definition(cli_context)

    snowpark_entities = get_snowpark_entities(pd)

    snowpark_paths = SnowparkPackagePaths.for_snowpark_project(
        project_root=SecurePath(cli_context.project_root),
        snowpark_entities=snowpark_entities,
    )

    anaconda_packages_manager = AnacondaPackagesManager()

    # Resolve dependencies
    if snowpark_paths.defined_requirements_file.exists():
        with (
            cli_console.phase("Resolving dependencies from requirements.txt"),
            SecurePath.temporary_directory() as temp_deps_dir,
        ):
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
                target_dir=temp_deps_dir,
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
                if not allow_shared_libraries:
                    raise ClickException(
                        "Some packages contain shared (.so/.dll) libraries. "
                        "Try again with --allow-shared-libraries."
                    )
            if download_result.anaconda_packages:
                anaconda_packages.write_requirements_file_in_snowflake_format(  # type: ignore
                    file_path=snowpark_paths.snowflake_requirements_file,
                    requirements=download_result.anaconda_packages,
                )

            cli_console.step(f"Creating {snowpark_paths.dependencies_zip.name}")
            zip_dir(
                source=temp_deps_dir.path,
                dest_zip=snowpark_paths.dependencies_zip.path,
            )

    with cli_console.phase("Preparing artifacts for source code"):
        for artefact in snowpark_paths.sources:
            if artefact.is_dir():
                zip_name = zip_file_name_for_dir(artefact.path)
                cli_console.step(f"Creating: {zip_name.name}")
                zip_dir(
                    source=artefact.path,
                    dest_zip=zip_name,
                )

    return MessageResult(f"Build done.")


def zip_file_name_for_dir(artefact: Path | SecurePath):
    if isinstance(artefact, SecurePath):
        artefact = artefact.path
    zip_name = artefact.parent / (artefact.stem + ".zip")
    return zip_name


def get_snowpark_entities(
    pd: ProjectDefinition,
) -> Dict[str, ProcedureEntityModel | FunctionEntityModel]:
    procedures: Dict[str, ProcedureEntityModel] = pd.get_entities_by_type("procedure")
    functions: Dict[str, FunctionEntityModel] = pd.get_entities_by_type("function")
    snowpark_entities = {**procedures, **functions}
    return snowpark_entities


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


@app.command("list", requires_connection=True)
def list_(
    object_type: _SnowparkObject = ObjectTypeArgument,
    like: str = LikeOption,
    scope: Tuple[str, str] = scope_option(
        help_example="`list function --in database my_db`"
    ),
    **options,
):
    """Lists all available procedures or functions."""
    return object_list(object_type=object_type.value, like=like, scope=scope, **options)


@app.command("drop", requires_connection=True)
def drop(
    object_type: _SnowparkObject = ObjectTypeArgument,
    identifier: FQN = IdentifierArgument,
    **options,
):
    """Drop procedure or function."""
    return object_drop(object_type=object_type.value, object_name=identifier, **options)


@app.command("describe", requires_connection=True)
def describe(
    object_type: _SnowparkObject = ObjectTypeArgument,
    identifier: FQN = IdentifierArgument,
    **options,
):
    """Provides description of a procedure or function."""
    return object_describe(
        object_type=object_type.value, object_name=identifier, **options
    )


def _migrate_v1_snowpark_to_v2(pd: ProjectDefinition):
    if not pd.snowpark:
        raise NoProjectDefinitionError(
            project_type="snowpark", project_root=get_cli_context().project_root
        )

    data: dict = {
        "definition_version": "2",
        "defaults": {
            "stage": pd.snowpark.stage_name,
            "project_name": pd.snowpark.project_name,
        },
        "entities": {},
    }

    for entity in [*pd.snowpark.procedures, *pd.snowpark.functions]:
        identifier = {"name": entity.name}
        if entity.database is not None:
            identifier["database"] = entity.database
        if entity.schema_name is not None:
            identifier["schema"] = entity.schema_name
        v2_entity = {
            "type": "function" if isinstance(entity, FunctionSchema) else "procedure",
            "stage": pd.snowpark.stage_name,
            "artifacts": [pd.snowpark.src],
            "handler": entity.handler,
            "returns": entity.returns,
            "signature": entity.signature,
            "runtime": entity.runtime,
            "external_access_integrations": entity.external_access_integrations,
            "secrets": entity.secrets,
            "imports": entity.imports,
            "identifier": identifier,
        }
        if isinstance(entity, ProcedureSchema):
            v2_entity["execute_as_caller"] = entity.execute_as_caller

        data["entities"][entity.name] = v2_entity

    return ProjectDefinitionV2(**data)


def _get_v2_project_definition(cli_context) -> ProjectDefinitionV2:
    pd = cli_context.project_definition
    if not pd.meets_version_requirement("2"):
        pd = _migrate_v1_snowpark_to_v2(pd)
    return pd
