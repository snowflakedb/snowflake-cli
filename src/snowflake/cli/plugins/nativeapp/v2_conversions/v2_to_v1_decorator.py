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
from typing import Any, Dict, Optional

from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context, cli_context_manager
from snowflake.cli.api.project.schemas.entities.application_entity import (
    ApplicationEntity,
)
from snowflake.cli.api.project.schemas.entities.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV11,
    DefinitionV20,
)


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
    pdfv1["native_app"]["name"] = "Auto converted NativeApp project from V2"
    pdfv1["native_app"]["artifacts"] = app_package_definition.artifacts
    pdfv1["native_app"]["source_stage"] = app_package_definition.stage

    # Package
    pdfv1["native_app"]["package"] = {}
    pdfv1["native_app"]["package"]["name"] = app_package_definition.name

    # Application
    if app_definition:
        pdfv1["native_app"]["application"] = {}
        pdfv1["native_app"]["application"]["name"] = app_definition.name
        if app_definition.meta and app_definition.meta.role:
            pdfv1["native_app"]["application"]["role"] = app_definition.meta.role

    # Override the definition object in global context
    return DefinitionV11(**pdfv1)


def nativeapp_definition_v2_to_v1(func):
    """
    A command decorator that attempts to automatically convert a native app project from
    definition v2 to v1.1.
    The definition object in CliGlobalContext will be replaced with the converted object.
    Exactly one application package entity type is expected, and up to one application
    entity type is expected.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        original_pdf: DefinitionV20 = cli_context.project_definition
        if original_pdf.definition_version == "2":
            pdfv1 = _pdf_v2_to_v1(original_pdf)
            cli_context_manager.set_project_definition(pdfv1)
        return func(*args, **kwargs)

    return wrapper
