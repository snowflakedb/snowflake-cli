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

from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context, cli_context_manager
from snowflake.cli.api.project.schemas.entities.application_entity import (
    ApplicationEntity,
)
from snowflake.cli.api.project.schemas.entities.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli.api.project.schemas.native_app.application import (
    ApplicationPostDeployHook,
    SqlScriptHookType,
)
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV11,
    DefinitionV20,
)


def _convert_v2_artifact_to_v1_dict(
    v2_artifact: Union[PathMapping, Path]
) -> Union[Dict, str]:
    if isinstance(v2_artifact, PathMapping):
        return {
            "src": v2_artifact.src,
            "dest": v2_artifact.dest,
            "processors": v2_artifact.processors,
        }
    return str(v2_artifact)


def _convert_v2_post_deploy_hook_to_v1_scripts(
    v2_post_deploy_hook: ApplicationPostDeployHook,
) -> List[str]:
    if isinstance(v2_post_deploy_hook, SqlScriptHookType):
        return v2_post_deploy_hook.sql_script
    raise ValueError(f"Unsupported post deploy hook type: {v2_post_deploy_hook}")


def _pdf_v2_to_v1(v2_definition: DefinitionV20) -> DefinitionV11:
    pdfv1: Dict[str, Any] = {"definition_version": "1.1", "native_app": {}}

    app_package_definition: ApplicationPackageEntity = None
    app_definition: Optional[ApplicationEntity] = None

    for key, entity in v2_definition.entities.items():
        if entity.get_type() == ApplicationPackageEntity.get_type():
            if app_package_definition:
                raise ClickException(
                    "More than one application package entity exists in the project definition file."
                )
            app_package_definition = entity
        elif entity.get_type() == ApplicationEntity.get_type():
            if app_definition:
                raise ClickException(
                    "More than one application entity exists in the project definition file."
                )
            app_definition = entity
    if not app_package_definition:
        raise ClickException(
            "Could not find an application package entity in the project definition file."
        )

    # NativeApp
    if app_definition and app_definition.name:
        pdfv1["native_app"]["name"] = app_definition.name
    else:
        pdfv1["native_app"]["name"] = app_package_definition.name.split("_pkg_")[0]
    pdfv1["native_app"]["artifacts"] = [
        _convert_v2_artifact_to_v1_dict(a) for a in app_package_definition.artifacts
    ]
    pdfv1["native_app"]["source_stage"] = app_package_definition.stage
    pdfv1["native_app"]["bundle_root"] = str(app_package_definition.bundle_root)
    pdfv1["native_app"]["generated_root"] = str(app_package_definition.generated_root)
    pdfv1["native_app"]["deploy_root"] = str(app_package_definition.deploy_root)

    # Package
    pdfv1["native_app"]["package"] = {}
    pdfv1["native_app"]["package"]["name"] = app_package_definition.name
    if app_package_definition.distribution:
        pdfv1["native_app"]["package"][
            "distribution"
        ] = app_package_definition.distribution
    if app_package_definition.meta and app_package_definition.meta.post_deploy:
        pdfv1["native_app"]["package"]["scripts"] = [
            _convert_v2_post_deploy_hook_to_v1_scripts(s)
            for s in app_package_definition.meta.post_deploy
        ]

    # Application
    if app_definition:
        pdfv1["native_app"]["application"] = {}
        pdfv1["native_app"]["application"]["name"] = app_definition.name
        if app_definition.meta and app_definition.meta.role:
            pdfv1["native_app"]["application"]["role"] = app_definition.meta.role
        if app_definition.meta and app_definition.meta.post_deploy:
            pdfv1["native_app"]["application"][
                "post_deploy"
            ] = app_definition.meta.post_deploy

    # Override the definition object in global context
    return DefinitionV11(**pdfv1)


def nativeapp_definition_v2_to_v1(func):
    """
    A command decorator that attempts to automatically convert a native app project from
    definition v2 to v1.1. Assumes with_project_definition() has already been called.
    The definition object in CliGlobalContext will be replaced with the converted object.
    Exactly one application package entity type is expected, and up to one application
    entity type is expected.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        original_pdf: DefinitionV20 = cli_context.project_definition
        if not original_pdf:
            raise ValueError(
                "Project definition could not be found. The nativeapp_definition_v2_to_v1 command decorator assumes with_project_definition() was called before it."
            )
        if original_pdf.definition_version == "2":
            pdfv1 = _pdf_v2_to_v1(original_pdf)
            cli_context_manager.set_project_definition(pdfv1)
        return func(*args, **kwargs)

    return wrapper
