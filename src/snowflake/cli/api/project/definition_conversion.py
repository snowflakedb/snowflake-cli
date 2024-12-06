from __future__ import annotations

import logging
import shutil
import tempfile
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory, mkstemp
from typing import Any, Dict, Literal, Optional

from click import ClickException
from snowflake.cli._plugins.nativeapp.artifacts import (
    bundle_artifacts,
)
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.templates.templates_processor import (
    TemplatesProcessor,
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
from snowflake.cli.api.utils.dict_utils import deep_merge_dicts

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
    native_app_data, native_app_template_context = (
        convert_native_app_to_v2_data(definition_v1.native_app, template_context)
        if definition_v1.native_app
        else ({}, {})
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
    replacement_template_context = deepcopy(template_context) or {}
    deep_merge_dicts(replacement_template_context, native_app_template_context)
    if replacement_template_context:
        _convert_templates_in_files(
            project_root,
            definition_v1,
            definition_v2,
            in_memory,
            replacement_template_context,
        )

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
    artifacts = [str(a) for a in artifacts if a is not None]

    if streamlit.additional_source_files:
        for additional_file in streamlit.additional_source_files:
            artifacts.append(str(additional_file))

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
    native_app: NativeApp,
    template_context: Optional[Dict[str, Any]] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    def _make_meta(obj: Application | Package):
        meta = {}
        if obj.role:
            meta["role"] = obj.role
        if obj.warehouse:
            meta["warehouse"] = obj.warehouse
        if obj.post_deploy:
            meta["post_deploy"] = obj.post_deploy
        return meta

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

    pdfv2_yml = {
        "entities": {
            package_entity_name: package,
            app_entity_name: app,
        }
    }
    template_replacements = {
        "ctx": {
            "native_app": {
                "name": native_app.name,  # This is a literal since there's no equivalent in v2
                # omitting "artifacts" since lists are not supported in templates
                "bundle_root": _make_template(
                    f"ctx.entities.{package_entity_name}.bundle_root"
                ),
                "deploy_root": _make_template(
                    f"ctx.entities.{package_entity_name}.deploy_root"
                ),
                "generated_root": _make_template(
                    f"ctx.entities.{package_entity_name}.generated_root"
                ),
                "source_stage": _make_template(
                    f"ctx.entities.{package_entity_name}.stage"
                ),
                "scratch_stage": _make_template(
                    f"ctx.entities.{package_entity_name}.scratch_stage"
                ),
                "package": {
                    # omitting "scripts" since lists are not supported in templates
                    "role": _make_template(
                        f"ctx.entities.{package_entity_name}.meta.role"
                    ),
                    "name": _make_template(
                        f"ctx.entities.{package_entity_name}.identifier"
                    ),
                    "warehouse": _make_template(
                        f"ctx.entities.{package_entity_name}.meta.warehouse"
                    ),
                    "distribution": _make_template(
                        f"ctx.entities.{package_entity_name}.distribution"
                    ),
                    # omitting "post_deploy" since lists are not supported in templates
                },
                "application": {
                    "role": _make_template(f"ctx.entities.{app_entity_name}.meta.role"),
                    "name": _make_template(
                        f"ctx.entities.{app_entity_name}.identifier"
                    ),
                    "warehouse": _make_template(
                        f"ctx.entities.{app_entity_name}.meta.warehouse"
                    ),
                    "debug": _make_template(f"ctx.entities.{app_entity_name}.debug"),
                    # omitting "post_deploy" since lists are not supported in templates
                },
            }
        }
    }
    return pdfv2_yml, template_replacements


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
    replacement_template_context: dict[str, Any],
):
    """Converts templates in other files to the new format"""
    # Set up fakers so that references to ctx.env. and fn.
    # get templated to the same literal, since those references
    # are the same in v1 and v2
    replacement_template_context["ctx"]["env"] = _EnvFaker()
    replacement_template_context["fn"] = _FnFaker()

    metrics = get_cli_context().metrics

    if na := definition_v1.native_app:
        # If the v1 definition has a Native App, we know
        # that the v2 definition will have exactly one application package entity
        pkg_model: ApplicationPackageEntityModel = list(
            definition_v2.get_entities_by_type(
                ApplicationPackageEntityModel.get_type()
            ).values()
        )[0]

        # Convert templates in artifacts by passing them through the TemplatesProcessor
        # but providing a context that maps v1 template references to the equivalent v2
        # references instead of resolving to literals
        # For example, replacement_template_context might look like
        # {
        #     "ctx": {
        #         "native_app": {
        #             "bundle_root": "<% ctx.entities.pkg.bundle_root %>",
        #             "deploy_root": "<% ctx.entities.pkg.deploy_root %>",
        #             "application": {
        #                 "name": "<% ctx.entities.app.identifier %>",
        #             }
        #             and so on...
        #          }
        #     }
        # }
        # We only convert files on-disk if the "templates" processor is used in the artifacts
        # and if we're doing a permanent conversion. If we're doing an in-memory conversion,
        # the CLI global template context is already populated with the v1 definition, so
        # we don't want to convert the v1 template references in artifact files
        metrics.set_counter_default(CLICounterField.TEMPLATES_PROCESSOR, 0)
        artifacts_to_template = [
            artifact
            for artifact in pkg_model.artifacts
            for processor in artifact.processors
            if processor.name.lower() == TemplatesProcessor.NAME
        ]
        if not in_memory and artifacts_to_template:
            metrics.set_counter(CLICounterField.TEMPLATES_PROCESSOR, 1)

            # Create a temporary directory to hold the expanded templates,
            # as if a bundle step had been run but without affecting any
            # files on disk outside of the artifacts we want to convert
            with tempfile.TemporaryDirectory() as d:
                deploy_root = Path(d)
                bundle_ctx = BundleContext(
                    package_name=pkg_model.identifier,
                    artifacts=pkg_model.artifacts,
                    project_root=project_root,
                    bundle_root=project_root / pkg_model.bundle_root,
                    deploy_root=deploy_root,
                    generated_root=(
                        project_root / deploy_root / pkg_model.generated_root
                    ),
                )
                template_processor = TemplatesProcessor(bundle_ctx)
                bundle_map = bundle_artifacts(
                    project_root, deploy_root, artifacts_to_template
                )
                for src, dest in bundle_map.all_mappings(
                    absolute=True, expand_directories=True
                ):
                    if src.is_dir():
                        continue
                    # We call the implementation directly instead of calling process()
                    # since we need access to the BundleMap to copy files anyways
                    template_processor.expand_templates_in_file(
                        src, dest, replacement_template_context
                    )
                    # Copy the expanded file back to its original source location if it was modified
                    if not dest.is_symlink():
                        shutil.copyfile(dest, src)

        # Convert package script files to post-deploy hooks
        metrics.set_counter_default(CLICounterField.PACKAGE_SCRIPTS, 0)
        if (pkg := na.package) and pkg.scripts:
            metrics.set_counter(CLICounterField.PACKAGE_SCRIPTS, 1)
            cli_console.warning(
                "WARNING: native_app.package.scripts is deprecated. "
                "Please migrate to using native_app.package.post_deploy."
            )

            converted_post_deploy_hooks = _convert_package_script_files(
                project_root, pkg.scripts, pkg_model, in_memory
            )
            if pkg_model.meta is None:
                pkg_model.meta = MetaField()
            if pkg_model.meta.post_deploy is None:
                pkg_model.meta.post_deploy = []
            pkg_model.meta.post_deploy += converted_post_deploy_hooks


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


class _EnvFaker:
    def __getitem__(self, item):
        return _make_template(f"ctx.env.{item}")


class _FnFaker:
    def __getitem__(self, item):
        return lambda *args: _make_template(
            f"fn.{item}({', '.join(repr(a) for a in args)})"
        )


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
