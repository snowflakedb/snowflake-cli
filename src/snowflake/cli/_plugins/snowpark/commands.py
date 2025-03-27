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
from typing import Dict, Optional, Set, Tuple

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
    EntityToImportPathsMapping,
    SnowparkEntities,
    SnowparkObject,
    SnowparkObjectManager,
    StageToArtifactMapping,
    map_path_mapping_to_artifact,
    zip_and_copy_artifacts_to_deploy,
)
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AnacondaPackagesManager,
)
from snowflake.cli._plugins.snowpark.package.commands import app as package_app
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli._plugins.snowpark.snowpark_project_paths import (
    SnowparkProjectPaths,
)
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
    ForceReplaceOption,
    PruneOption,
    ReplaceOption,
    execution_identifier_argument,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import (
    DEFAULT_SIZE_LIMIT_MB,
)
from snowflake.cli.api.exceptions import (
    SecretsWithoutExternalAccessIntegrationError,
)
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
    ProjectDefinitionV2,
)
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector import DictCursor, ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

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


@app.command("deploy", requires_connection=True, require_warehouse=True)
@with_project_definition()
def deploy(
    replace: bool = ReplaceOption(
        help="Replaces procedure or function if there were changes in the definition. It only uploads new and "
        "overwrites existing files, but does not remove any files already on the stage."
    ),
    force_replace: bool = ForceReplaceOption(),
    prune: bool = PruneOption(
        help="Remove contents of the stage before uploading artifacts."
    ),
    **options,
) -> CommandResult:
    """
    Deploys procedures and functions defined in project. Deploying the project alters all objects defined in it.
    By default, if any of the objects exist already the commands will fail unless `--replace` flag is provided.
    Required artifacts are deployed before creating functions or procedures. Dependencies are deployed once to
    every stage specified in definitions.
    """
    cli_context = get_cli_context()
    pd = _get_v2_project_definition(cli_context)

    snowpark_entities = get_snowpark_entities(pd)
    project_paths = SnowparkProjectPaths(
        project_root=cli_context.project_root,
    )

    with cli_console.phase("Performing initial validation"):
        if not snowpark_entities:
            raise ClickException(
                "No procedures or functions were specified in the project definition."
            )
        validate_all_artifacts_exists(
            project_paths=project_paths, snowpark_entities=snowpark_entities
        )

    # Validate current state
    with cli_console.phase("Checking remote state"):
        om = ObjectManager()
        _check_if_all_defined_integrations_exists(om, snowpark_entities)
        existing_objects = (
            {}
            if force_replace
            else check_for_existing_objects(om, replace, snowpark_entities)
        )

    with cli_console.phase("Preparing required stages and artifacts"):
        entities_to_imports_map, stages_to_artifact_map = build_artifacts_mappings(
            project_paths=project_paths,
            snowpark_entities=snowpark_entities,
        )

        create_stages_and_upload_artifacts(stages_to_artifact_map, prune=prune)

    # Create snowpark entities
    with cli_console.phase("Creating Snowpark entities"):
        snowpark_manager = SnowparkObjectManager()
        snowflake_dependencies = _read_snowflake_requirements_file(
            project_paths.snowflake_requirements
        )
        deploy_status = []
        for entity in snowpark_entities.values():
            operation_result = snowpark_manager.deploy_entity(
                entity=entity,
                existing_objects=existing_objects,
                snowflake_dependencies=snowflake_dependencies,
                entities_to_artifact_map=entities_to_imports_map,
            )
            deploy_status.append(operation_result)

    return CollectionResult(deploy_status)


def validate_all_artifacts_exists(
    project_paths: SnowparkProjectPaths, snowpark_entities: SnowparkEntities
):
    for key, entity in snowpark_entities.items():
        for artifact in entity.artifacts:
            path = project_paths.get_artifact_dto(artifact).post_build_path
            if not path.exists():
                raise UsageError(
                    f"Artifact {path} required for {entity.type} {key} does not exist."
                )


def check_for_existing_objects(
    om: ObjectManager, replace: bool, snowpark_entities: SnowparkEntities
) -> Dict[str, SnowflakeCursor]:
    existing_objects: Dict[str, SnowflakeCursor] = _find_existing_objects(
        snowpark_entities, om
    )
    if existing_objects and not replace:
        existing_entities = [snowpark_entities[e] for e in existing_objects]
        msg = "Following objects already exists. Consider using --replace.\n"
        msg += "\n".join(f"{e.type}: {e.entity_id}" for e in existing_entities)
        raise ClickException(msg)
    return existing_objects


def build_artifacts_mappings(
    project_paths: SnowparkProjectPaths, snowpark_entities: SnowparkEntities
) -> Tuple[EntityToImportPathsMapping, StageToArtifactMapping]:
    stages_to_artifact_map: StageToArtifactMapping = defaultdict(set)
    entities_to_imports_map: EntityToImportPathsMapping = defaultdict(set)
    for name, entity in snowpark_entities.items():
        stage = entity.stage
        required_artifacts = set()
        for artifact in entity.artifacts:
            artifact_dto = project_paths.get_artifact_dto(artifact)
            required_artifacts.add(artifact_dto)
            entities_to_imports_map[name].add(artifact_dto.import_path(stage))
        stages_to_artifact_map[stage].update(required_artifacts)

        deps_artifact = project_paths.get_dependencies_artifact()
        if deps_artifact.post_build_path.exists():
            stages_to_artifact_map[stage].add(deps_artifact)
            entities_to_imports_map[name].add(deps_artifact.import_path(stage))
    return entities_to_imports_map, stages_to_artifact_map


def create_stages_and_upload_artifacts(
    stages_to_artifact_map: StageToArtifactMapping, prune: bool
):
    stage_manager = StageManager()
    if prune:
        # snowflake.cli._plugins.snowpark.snowpark_project_paths.Artifact class assumes that "stage"
        # is a stage object, not path on stage - whole stage is managed by snowpark - it can be removed
        for stage in stages_to_artifact_map.keys():
            cli_console.step(f"Removing contents of stage {stage}")
            stage_manager.remove(stage, path="")

    for stage, artifacts in stages_to_artifact_map.items():
        cli_console.step(f"Creating (if not exists) stage: {stage}")
        stage = FQN.from_stage(stage).using_context()
        stage_manager.create(fqn=stage, comment="deployments managed by Snowflake CLI")
        for artifact in artifacts:
            post_build_path = artifact.post_build_path
            cli_console.step(
                f"Uploading {post_build_path.name} to {artifact.upload_path(stage)}"
            )
            stage_manager.put(
                local_path=post_build_path,
                stage_path=artifact.upload_path(stage),
                overwrite=True,
            )


def _find_existing_objects(
    objects: SnowparkEntities,
    om: ObjectManager,
) -> Dict[str, SnowflakeCursor]:
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
    snowpark_entities: SnowparkEntities,
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

    project_paths = SnowparkProjectPaths(
        project_root=cli_context.project_root,
    )

    anaconda_packages_manager = AnacondaPackagesManager()

    # Clean up bundle root
    project_paths.remove_up_bundle_root()

    # Resolve dependencies
    if project_paths.requirements.exists():
        with (
            cli_console.phase("Resolving dependencies from requirements.txt"),
            SecurePath.temporary_directory() as temp_deps_dir,
        ):
            requirements = package_utils.parse_requirements(
                requirements_file=project_paths.requirements,
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
                    file_path=project_paths.snowflake_requirements,
                    requirements=download_result.anaconda_packages,
                )

            if any(temp_deps_dir.path.iterdir()):
                dep_artifact = project_paths.get_dependencies_artifact()
                cli_console.step(f"Creating {dep_artifact.path.name}")
                zip_dir(
                    source=temp_deps_dir.path,
                    dest_zip=dep_artifact.post_build_path,
                )
            else:
                cli_console.step(f"No external dependencies.")

    artifacts = set()
    with cli_console.phase("Preparing artifacts for source code"):
        for entity in get_snowpark_entities(pd).values():
            artifacts.update(
                map_path_mapping_to_artifact(project_paths, entity.artifacts)
            )

        if FeatureFlag.ENABLE_SNOWPARK_GLOB_SUPPORT.is_enabled():
            zip_and_copy_artifacts_to_deploy(artifacts, project_paths.bundle_root)
        else:
            for artifact in artifacts:
                artifact.build()

    return MessageResult(f"Build done.")


def get_snowpark_entities(
    pd: ProjectDefinition,
) -> Dict[str, ProcedureEntityModel | FunctionEntityModel]:
    procedures: Dict[str, ProcedureEntityModel] = pd.get_entities_by_type("procedure")
    functions: Dict[str, FunctionEntityModel] = pd.get_entities_by_type("function")
    snowpark_entities = {**procedures, **functions}
    return snowpark_entities


@app.command("execute", requires_connection=True)
def execute(
    object_type: SnowparkObject = ObjectTypeArgument,
    execution_identifier: str = execution_identifier_argument(
        "procedure/function", "hello(1, 'world')"
    ),
    **options,
) -> CommandResult:
    """Executes a procedure or function in a specified environment."""
    cursor = SnowparkObjectManager().execute(
        execution_identifier=execution_identifier, object_type=object_type
    )
    return SingleQueryResult(cursor)


@app.command("list", requires_connection=True)
def list_(
    object_type: SnowparkObject = ObjectTypeArgument,
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
    object_type: SnowparkObject = ObjectTypeArgument,
    identifier: FQN = IdentifierArgument,
    **options,
):
    """Drop procedure or function."""
    return object_drop(object_type=object_type.value, object_name=identifier, **options)


@app.command("describe", requires_connection=True)
def describe(
    object_type: SnowparkObject = ObjectTypeArgument,
    identifier: FQN = IdentifierArgument,
    **options,
):
    """Provides description of a procedure or function."""
    return object_describe(
        object_type=object_type.value, object_name=identifier, **options
    )


def _get_v2_project_definition(cli_context) -> ProjectDefinitionV2:
    pd = cli_context.project_definition
    if not pd.meets_version_requirement("2"):
        pd = convert_project_definition_to_v2(cli_context.project_root, pd)
    return pd
