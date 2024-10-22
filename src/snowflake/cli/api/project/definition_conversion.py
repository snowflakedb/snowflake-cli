from __future__ import annotations

import logging
from pathlib import Path
from tempfile import TemporaryDirectory, mkstemp
from typing import Any, Dict, Literal, Optional

from click import ClickException
from snowflake.cli._plugins.nativeapp.artifacts import (
    BundleMap,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntityModel,
)
from snowflake.cli._plugins.snowpark.common import is_name_a_templated_one
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import (
    DEFAULT_ENV_FILE,
    DEFAULT_PAGES_DIR,
    PROJECT_TEMPLATE_VARIABLE_CLOSING,
    PROJECT_TEMPLATE_VARIABLE_OPENING,
    SNOWPARK_SHARED_MIXIN,
)
from snowflake.cli.api.entities.utils import render_script_template
from snowflake.cli.api.metrics import CLICounterField
from snowflake.cli.api.project.schemas.entities.common import (
    MetaField,
    SqlScriptHookType,
)
from snowflake.cli.api.project.schemas.project_definition import (
    ProjectDefinition,
    ProjectDefinitionV2,
)
from snowflake.cli.api.project.schemas.v1.native_app.application import (
    Application,
    ApplicationV11,
)
from snowflake.cli.api.project.schemas.v1.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.v1.native_app.package import Package, PackageV11
from snowflake.cli.api.project.schemas.v1.snowpark.callable import (
    FunctionSchema,
    ProcedureSchema,
)
from snowflake.cli.api.project.schemas.v1.snowpark.snowpark import Snowpark
from snowflake.cli.api.project.schemas.v1.streamlit.streamlit import Streamlit
from snowflake.cli.api.rendering.jinja import get_basic_jinja_env
from snowflake.cli.api.utils.definition_rendering import render_definition_template

log = logging.getLogger(__name__)

# A directory to hold temporary files created during in-memory definition conversion
# We need a global reference to this directory to prevent the object from being
# garbage collected before the files in the directory are used by other parts
# of the CLI. The directory will then be deleted on interpreter exit
_IN_MEMORY_CONVERSION_TEMP_DIR: TemporaryDirectory | None = None


def _get_temp_dir() -> TemporaryDirectory:
    global _IN_MEMORY_CONVERSION_TEMP_DIR
    if _IN_MEMORY_CONVERSION_TEMP_DIR is None:
        _IN_MEMORY_CONVERSION_TEMP_DIR = TemporaryDirectory(
            suffix="_pdf_conversion", ignore_cleanup_errors=True
        )
    return _IN_MEMORY_CONVERSION_TEMP_DIR


def _is_field_defined(template_context: Optional[Dict[str, Any]], *path: str) -> bool:
    """
    Determines if a field is defined in the provided template context. For example,

    _is_field_defined({"ctx": {"native_app": {"bundle_root": "my_root"}}}, "ctx", "native_app", "bundle_root")

    returns True. If the provided template context is None, this function returns True for all paths.

    """
    if template_context is None:
        return True  # No context, so assume that all variables are defined

    current_dict = template_context
    for key in path:
        if not isinstance(current_dict, dict):
            return False
        if key not in current_dict:
            return False
        current_dict = current_dict[key]

    return True


def convert_project_definition_to_v2(
    project_root: Path,
    definition_v1: ProjectDefinition,
    accept_templates: bool = False,
    template_context: Optional[Dict[str, Any]] = None,
    in_memory: bool = False,
) -> ProjectDefinitionV2:
    _check_if_project_definition_meets_requirements(definition_v1, accept_templates)

    snowpark_data = (
        convert_snowpark_to_v2_data(definition_v1.snowpark)
        if definition_v1.snowpark
        else {}
    )
    streamlit_data = (
        convert_streamlit_to_v2_data(definition_v1.streamlit)
        if definition_v1.streamlit
        else {}
    )
    native_app_data = (
        convert_native_app_to_v2_data(
            project_root, definition_v1.native_app, template_context
        )
        if definition_v1.native_app
        else {}
    )
    envs = convert_envs_to_v2(definition_v1)

    data = {
        "definition_version": "2",
        "entities": get_list_of_all_entities(
            snowpark_data.get("entities", {}),
            streamlit_data.get("entities", {}),
            native_app_data.get("entities", {}),
        ),
        "mixins": snowpark_data.get("mixins", None),
    }
    if envs is not None:
        data["env"] = envs

    if in_memory:
        # If this is an in-memory conversion, we need to evaluate templates right away
        # since the file won't be re-read as it would be for a permanent conversion
        definition_v2 = render_definition_template(data, {}).project_definition
    else:
        definition_v2 = ProjectDefinitionV2(**data)

    # If the user's files have any template tags in them, they
    # also need to be migrated to point to the v2 entities
    _convert_templates_in_files(project_root, definition_v1, definition_v2, in_memory)

    return definition_v2


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


def convert_streamlit_to_v2_data(streamlit: Streamlit) -> Dict[str, Any]:
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
                "comment": streamlit.comment,
                "query_warehouse": streamlit.query_warehouse,
                "main_file": str(streamlit.main_file),
                "pages_dir": str(streamlit.pages_dir),
                "stage": streamlit.stage,
                "artifacts": artifacts,
            }
        }
    }
    return data


def convert_native_app_to_v2_data(
    project_root: Path,
    native_app: NativeApp,
    template_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    def _make_meta(obj: Application | Package):
        meta = {}
        if obj.role:
            meta["role"] = obj.role
        if obj.warehouse:
            meta["warehouse"] = obj.warehouse
        if obj.post_deploy:
            meta["post_deploy"] = obj.post_deploy
        return meta

    def _find_manifest():
        # We don't know which file in the project directory is the actual manifest,
        # and we can't iterate through the artifacts property since the src can contain
        # glob patterns. The simplest solution is to bundle the app and find the
        # manifest file from the resultant BundleMap, since the bundle process ensures
        # that only a single source path can map to the corresponding destination path
        bundle_map = BundleMap(
            project_root=project_root, deploy_root=project_root / native_app.deploy_root
        )
        for artifact in native_app.artifacts:
            bundle_map.add(artifact)

        manifest_path = next(
            (
                src
                for src, dest in bundle_map.all_mappings(
                    absolute=True, expand_directories=True
                )
                if dest.name == "manifest.yml"
            ),
            None,
        )
        if not manifest_path:
            # The manifest field is required, so we can't gracefully handle it being missing
            raise ClickException(
                "manifest.yml file not found in any Native App artifact sources, "
                "unable to perform migration"
            )

        # Use a POSIX path to be consistent with other migrated fields
        # which use POSIX paths as default values
        return manifest_path.relative_to(project_root).as_posix()

    package_entity_name = "pkg"
    if (
        native_app.package
        and native_app.package.name
        and native_app.package.name != PackageV11.model_fields["name"].default
    ):
        package_identifier = native_app.package.name
    else:
        # Backport the PackageV11 default name template, updated for PDFv2
        package_identifier = _make_template(
            f"fn.concat_ids('{native_app.name}', '_pkg_', fn.sanitize_id(fn.get_username('unknown_user')) | lower)"
        )
    package = {
        "type": "application package",
        "identifier": package_identifier,
        "manifest": _find_manifest(),
        "artifacts": native_app.artifacts,
    }

    if _is_field_defined(template_context, "ctx", "native_app", "bundle_root"):
        package["bundle_root"] = native_app.bundle_root
    if _is_field_defined(template_context, "ctx", "native_app", "generated_root"):
        package["generated_root"] = native_app.generated_root
    if _is_field_defined(template_context, "ctx", "native_app", "deploy_root"):
        package["deploy_root"] = native_app.deploy_root
    if _is_field_defined(template_context, "ctx", "native_app", "source_stage"):
        package["stage"] = native_app.source_stage
    if _is_field_defined(template_context, "ctx", "native_app", "scratch_stage"):
        package["scratch_stage"] = native_app.scratch_stage

    if native_app.package:
        if _is_field_defined(
            template_context, "ctx", "native_app", "package", "distribution"
        ):
            package["distribution"] = native_app.package.distribution
        package_meta = _make_meta(native_app.package)
        if native_app.package.scripts:
            # Package scripts are not supported in PDFv2 but we
            # don't convert them here, conversion is deferred until
            # the final v2 Pydantic model is available
            # (see _convert_templates_in_files())
            pass
        if package_meta:
            package["meta"] = package_meta

    app_entity_name = "app"
    if (
        native_app.application
        and native_app.application.name
        and native_app.application.name != ApplicationV11.model_fields["name"].default
    ):
        app_identifier = native_app.application.name
    else:
        # Backport the ApplicationV11 default name template, updated for PDFv2
        app_identifier = _make_template(
            f"fn.concat_ids('{native_app.name}', '_', fn.sanitize_id(fn.get_username('unknown_user')) | lower)"
        )
    app = {
        "type": "application",
        "identifier": app_identifier,
        "from": {"target": package_entity_name},
    }
    if native_app.application:
        if app_meta := _make_meta(native_app.application):
            app["meta"] = app_meta
        if _is_field_defined(
            template_context, "ctx", "native_app", "application", "debug"
        ):
            app["debug"] = native_app.application.debug

    return {
        "entities": {
            package_entity_name: package,
            app_entity_name: app,
        }
    }


def convert_envs_to_v2(pd: ProjectDefinition):
    if hasattr(pd, "env") and pd.env:
        data = {k: v for k, v in pd.env.items()}
        return data
    return None


def _convert_templates_in_files(
    project_root: Path,
    definition_v1: ProjectDefinition,
    definition_v2: ProjectDefinitionV2,
    in_memory: bool,
):
    """Converts templates in other files to the new format"""
    # TODO handle artifacts using the "templates" processor
    # For now this only handles Native App package scripts
    metrics = get_cli_context().metrics
    metrics.set_counter_default(CLICounterField.PACKAGE_SCRIPTS, 0)

    if (na := definition_v1.native_app) and (pkg := na.package) and pkg.scripts:
        metrics.set_counter(CLICounterField.PACKAGE_SCRIPTS, 1)
        cli_console.warning(
            "WARNING: native_app.package.scripts is deprecated. Please migrate to using native_app.package.post_deploy."
        )
        # If the v1 definition has a Native App with a package, we know
        # that the v2 definition will have exactly one application package entity
        pkg_entity: ApplicationPackageEntityModel = list(
            definition_v2.get_entities_by_type(
                ApplicationPackageEntityModel.get_type()
            ).values()
        )[0]
        converted_post_deploy_hooks = _convert_package_script_files(
            project_root, pkg.scripts, pkg_entity, in_memory
        )
        if pkg_entity.meta is None:
            pkg_entity.meta = MetaField()
        if pkg_entity.meta.post_deploy is None:
            pkg_entity.meta.post_deploy = []
        pkg_entity.meta.post_deploy += converted_post_deploy_hooks


def _convert_package_script_files(
    project_root: Path,
    package_scripts: list[str],
    pkg_model: ApplicationPackageEntityModel,
    in_memory: bool,
):
    # PDFv2 doesn't support package scripts, only post-deploy scripts, so we
    # need to convert the Jinja syntax from {{ }} to <% %>
    # Luckily, package scripts only support {{ package_name }}, so let's convert that tag
    # to v2 template syntax by running it though the template process with a fake
    # package name that's actually a valid v2 template, which will be evaluated
    # when the script is used as a post-deploy script
    # If we're doing an in-memory conversion, we can just hardcode the converted
    # package name directly into the script since it's being written to a temporary file
    package_name_replacement = (
        pkg_model.fqn.name
        if in_memory
        else _make_template(f"ctx.entities.{pkg_model.entity_id}.identifier")
    )
    jinja_context = dict(package_name=package_name_replacement)
    post_deploy_hooks = []
    for script_file in package_scripts:
        original_script_file = script_file
        new_contents = render_script_template(
            project_root, jinja_context, script_file, get_basic_jinja_env()
        )
        if in_memory:
            # If we're converting the definition in-memory, we can't touch
            # the package scripts on disk, so we'll write them to a temporary file
            d = _get_temp_dir().name
            _, script_file = mkstemp(dir=d, suffix="_converted.sql", text=True)
        (project_root / script_file).write_text(new_contents)
        hook = SqlScriptHookType(sql_script=script_file)
        hook._display_path = original_script_file  # noqa: SLF001
        post_deploy_hooks.append(hook)
    return post_deploy_hooks


def _make_template(template: str) -> str:
    return f"{PROJECT_TEMPLATE_VARIABLE_OPENING} {template} {PROJECT_TEMPLATE_VARIABLE_CLOSING}"


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
    snowpark_entities: Dict[str, Any],
    streamlit_entities: Dict[str, Any],
    native_app_entities: Dict[str, Any],
):
    # Check all combinations of entity types for overlapping names
    # (No need to use itertools here, PDFv1 only supports these three types)
    for types, first, second in [
        ("streamlit and snowpark", streamlit_entities, snowpark_entities),
        ("streamlit and native app", streamlit_entities, native_app_entities),
        ("native app and snowpark", native_app_entities, snowpark_entities),
    ]:
        if first.keys() & second.keys():
            raise ClickException(
                f"In your project, {types} entities share the same name. Please rename them and try again."
            )
    return snowpark_entities | streamlit_entities | native_app_entities
