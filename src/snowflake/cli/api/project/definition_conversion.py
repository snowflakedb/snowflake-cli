import logging
from pathlib import Path
from typing import Any, Dict, Literal, Optional

from click import ClickException
from snowflake.cli._plugins.snowpark.common import is_name_a_templated_one
from snowflake.cli.api.constants import (
    DEFAULT_ENV_FILE,
    DEFAULT_PAGES_DIR,
    PROJECT_TEMPLATE_VARIABLE_OPENING,
    SNOWPARK_SHARED_MIXIN,
)
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
    ProjectDefinitionV2,
)
from snowflake.cli.api.project.schemas.snowpark.callable import (
    FunctionSchema,
    ProcedureSchema,
)
from snowflake.cli.api.project.schemas.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit

log = logging.getLogger(__name__)


def convert_project_definition_to_v2(
    pd: ProjectDefinition, accept_templates: bool = False
) -> ProjectDefinitionV2:
    _check_if_project_definition_meets_requirements(pd, accept_templates)

    snowpark_data = convert_snowpark_to_v2_data(pd.snowpark) if pd.snowpark else {}
    streamlit_data = convert_streamlit_to_v2_data(pd.streamlit) if pd.streamlit else {}
    envs = convert_envs_to_v2(pd)

    data = {
        "definition_version": "2",
        "entities": get_list_of_all_entities(
            snowpark_data.get("entities", {}), streamlit_data.get("entities", {})
        ),
        "mixins": snowpark_data.get("mixins", None),
        "env": envs,
    }

    return ProjectDefinitionV2(**data)


def convert_snowpark_to_v2_data(snowpark: Snowpark) -> Dict[str, Any]:
    artifact_mapping = {"src": snowpark.src}
    if snowpark.project_name:
        artifact_mapping["dest"] = snowpark.project_name

    data: dict = {
        "mixins": {
            SNOWPARK_SHARED_MIXIN: {
                "stage": snowpark.stage_name,
                "artifacts": [artifact_mapping],
            }
        },
        "entities": {},
    }

    for index, entity in enumerate([*snowpark.procedures, *snowpark.functions]):
        identifier = {"name": entity.name}
        if entity.database is not None:
            identifier["database"] = entity.database
        if entity.schema_name is not None:
            identifier["schema"] = entity.schema_name

        entity_name = (
            f"snowpark_entity_{index}"
            if is_name_a_templated_one(entity.name)
            else entity.name
        )

        if entity_name in data["entities"]:
            raise ClickException(
                f"Entity with name {entity_name} seems to be duplicated. Please rename it and try again."
            )

        v2_entity = {
            "type": "function" if isinstance(entity, FunctionSchema) else "procedure",
            "stage": snowpark.stage_name,
            "handler": entity.handler,
            "returns": entity.returns,
            "signature": entity.signature,
            "runtime": entity.runtime,
            "external_access_integrations": entity.external_access_integrations,
            "secrets": entity.secrets,
            "imports": entity.imports,
            "identifier": identifier,
            "meta": {"use_mixins": [SNOWPARK_SHARED_MIXIN]},
        }
        if isinstance(entity, ProcedureSchema):
            v2_entity["execute_as_caller"] = entity.execute_as_caller

        data["entities"][entity_name] = v2_entity

    return data


def convert_streamlit_to_v2_data(streamlit: Streamlit):
    # Process env file and pages dir
    environment_file = _process_streamlit_files(streamlit.env_file, "environment")
    pages_dir = _process_streamlit_files(streamlit.pages_dir, "pages")

    # Build V2 definition
    artifacts = [
        streamlit.main_file,
        environment_file,
        pages_dir,
    ]
    artifacts = [a for a in artifacts if a is not None]

    if streamlit.additional_source_files:
        artifacts.extend(streamlit.additional_source_files)

    identifier = {"name": streamlit.name}
    if streamlit.schema_name:
        identifier["schema"] = streamlit.schema_name
    if streamlit.database:
        identifier["database"] = streamlit.database

    streamlit_name = (
        "streamlit_entity_1"
        if is_name_a_templated_one(streamlit.name)
        else streamlit.name
    )

    data = {
        "entities": {
            streamlit_name: {
                "type": "streamlit",
                "identifier": identifier,
                "title": streamlit.title,
                "query_warehouse": streamlit.query_warehouse,
                "main_file": str(streamlit.main_file),
                "pages_dir": str(streamlit.pages_dir),
                "stage": streamlit.stage,
                "artifacts": artifacts,
            }
        }
    }
    return data


def convert_envs_to_v2(pd: ProjectDefinition):
    if hasattr(pd, "env") and pd.env:
        data = {k: v for k, v in pd.env.items()}
        return data
    return None


def _check_if_project_definition_meets_requirements(
    pd: ProjectDefinition, accept_templates: bool
):
    if pd.meets_version_requirement("2"):
        raise ClickException("Project definition is already at version 2.")

    if PROJECT_TEMPLATE_VARIABLE_OPENING in str(pd):
        if not accept_templates:
            raise ClickException(
                "Project definition contains templates. They may not be migrated correctly, and require manual migration."
                "You can try again with --accept-templates  option, to attempt automatic migration."
            )
        log.warning(
            "Your V1 definition contains templates. We cannot guarantee the correctness of the migration."
        )
    if pd.native_app:
        raise ClickException(
            "Your project file contains a native app definition. Conversion of Native apps is not yet supported"
        )


def _process_streamlit_files(
    file_name: Optional[str], file_type: Literal["pages", "environment"]
):
    default = DEFAULT_PAGES_DIR if file_type == "pages" else DEFAULT_ENV_FILE

    if file_name and not Path(file_name).exists():
        raise ClickException(f"Provided file {file_name} does not exist")
    elif file_name is None and Path(default).exists():
        file_name = default
    return file_name


def get_list_of_all_entities(
    snowpark_entities: Dict[str, Any], streamlit_entities: Dict[str, Any]
):
    if snowpark_entities.keys() & streamlit_entities.keys():
        raise ClickException(
            "In your project, streamlit and snowpark entities share the same name. Please rename them and try again."
        )
    return snowpark_entities | streamlit_entities
