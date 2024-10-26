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

import inspect
from functools import wraps
from typing import Any, Dict, Optional, Type, TypeVar, Union

import typer
from click import ClickException
from snowflake.cli._plugins.nativeapp.entities.application import ApplicationEntityModel
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.cli_global_context import (
    get_cli_context,
    get_cli_context_manager,
)
from snowflake.cli.api.commands.decorators import _options_decorator_factory
from snowflake.cli.api.project.definition_conversion import (
    convert_project_definition_to_v2,
)
from snowflake.cli.api.project.schemas.entities.common import EntityModelBase
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV11,
    DefinitionV20,
    ProjectDefinition,
    ProjectDefinitionV1,
)
from snowflake.cli.api.project.schemas.v1.native_app.path_mapping import PathMapping
from snowflake.cli.api.utils.definition_rendering import render_definition_template

APP_AND_PACKAGE_OPTIONS = [
    inspect.Parameter(
        "package_entity_id",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=typer.Option(
            default="",
            help="The ID of the package entity on which to operate when definition_version is 2 or higher.",
        ),
    ),
    inspect.Parameter(
        "app_entity_id",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=typer.Option(
            default="",
            help="The ID of the application entity on which to operate when definition_version is 2 or higher.",
        ),
    ),
]


def _convert_v2_artifact_to_v1_dict(
    v2_artifact: Union[PathMapping, str]
) -> Union[Dict, str]:
    if isinstance(v2_artifact, PathMapping):
        return {
            "src": v2_artifact.src,
            "dest": v2_artifact.dest,
            "processors": v2_artifact.processors,
        }
    return v2_artifact


def _pdf_v2_to_v1(
    v2_definition: DefinitionV20,
    package_entity_id: str = "",
    app_entity_id: str = "",
    app_required: bool = False,
) -> DefinitionV11:
    pdfv1: Dict[str, Any] = {"definition_version": "1.1", "native_app": {}}

    app_definition, app_package_definition = _find_app_and_package_entities(
        v2_definition=v2_definition,
        package_entity_id=package_entity_id,
        app_entity_id=app_entity_id,
        app_required=app_required,
    )

    # NativeApp
    if app_definition and app_definition.fqn.identifier:
        pdfv1["native_app"]["name"] = app_definition.fqn.identifier
    else:
        pdfv1["native_app"]["name"] = app_package_definition.fqn.identifier.split(
            "_pkg_"
        )[0]
    pdfv1["native_app"]["artifacts"] = [
        _convert_v2_artifact_to_v1_dict(a) for a in app_package_definition.artifacts
    ]
    pdfv1["native_app"]["source_stage"] = app_package_definition.stage
    pdfv1["native_app"]["bundle_root"] = app_package_definition.bundle_root
    pdfv1["native_app"]["generated_root"] = app_package_definition.generated_root
    pdfv1["native_app"]["deploy_root"] = app_package_definition.deploy_root
    pdfv1["native_app"]["scratch_stage"] = app_package_definition.scratch_stage

    # Package
    pdfv1["native_app"]["package"] = {}
    pdfv1["native_app"]["package"]["name"] = app_package_definition.fqn.identifier
    if app_package_definition.distribution:
        pdfv1["native_app"]["package"][
            "distribution"
        ] = app_package_definition.distribution
    if app_package_definition.meta and app_package_definition.meta.post_deploy:
        pdfv1["native_app"]["package"][
            "post_deploy"
        ] = app_package_definition.meta.post_deploy
    if app_package_definition.meta:
        if app_package_definition.meta.role:
            pdfv1["native_app"]["package"]["role"] = app_package_definition.meta.role
        if app_package_definition.meta.warehouse:
            pdfv1["native_app"]["package"][
                "warehouse"
            ] = app_package_definition.meta.warehouse

    # Application
    if app_definition:
        pdfv1["native_app"]["application"] = {}
        pdfv1["native_app"]["application"]["name"] = app_definition.fqn.identifier
        if app_definition.debug:
            pdfv1["native_app"]["application"]["debug"] = app_definition.debug
        if app_definition.meta:
            if app_definition.meta.role:
                pdfv1["native_app"]["application"]["role"] = app_definition.meta.role
            if app_definition.meta.warehouse:
                pdfv1["native_app"]["application"][
                    "warehouse"
                ] = app_definition.meta.warehouse
            if app_definition.meta.post_deploy:
                pdfv1["native_app"]["application"][
                    "post_deploy"
                ] = app_definition.meta.post_deploy

    result = render_definition_template(pdfv1, {})
    # Override the definition object in global context
    return result.project_definition


def _find_app_and_package_entities(
    v2_definition: DefinitionV20,
    package_entity_id: str,
    app_entity_id: str,
    app_required: bool,
):
    # Determine the application entity to convert, there can be zero or one
    app_definition = find_entity(
        v2_definition,
        ApplicationEntityModel,
        app_entity_id,
        disambiguation_option="--app-entity-id",
        required=app_required,
    )
    # Infer or verify the package if we have an app entity to convert
    if app_definition:
        target_package = app_definition.from_.target
        if package_entity_id:
            # If the user specified a package entity ID,
            # check that the app entity targets the user-specified package entity
            # if the app entity is used by the command being run
            if target_package != package_entity_id and app_required:
                raise ClickException(
                    f"The application entity {app_definition.entity_id} does not "
                    f"target the application package entity {package_entity_id}. Either"
                    f"use --package-entity-id {target_package} to target the correct package entity, "
                    f"or omit the --package-entity-id flag to automatically use the package entity "
                    f"that the application entity targets."
                )
        elif target_package in v2_definition.get_entities_by_type(
            ApplicationPackageEntityModel.get_type()
        ):
            # If the user didn't target a specific package entity, use the one the app entity targets
            package_entity_id = target_package
    # Determine the package entity to convert, there must be one
    app_package_definition = find_entity(
        v2_definition,
        ApplicationPackageEntityModel,
        package_entity_id,
        disambiguation_option="--package-entity-id",
        required=True,
    )
    assert app_package_definition is not None  # satisfy mypy
    return app_definition, app_package_definition


T = TypeVar("T", bound=EntityModelBase)


def find_entity(
    project_definition: DefinitionV20,
    entity_class: Type[T],
    entity_id: str,
    disambiguation_option: str,
    required: bool,
) -> T | None:
    """
    Find an entity of the specified type in the project definition file.

    If an ID is passed, only that entity will be considered,
    otherwise look for a single entity of the specified type.

    If there are multiple entities of the specified type,
    the user must specify which one to use using the CLI option
    named in the disambiguation_option parameter.

    If no entity is found, an error is raised if required is True,
    otherwise None is returned.
    """

    entity_type = entity_class.get_type()
    entities = project_definition.get_entities_by_type(entity_type)

    entity: Optional[T] = None

    if entity_id:
        # If we're looking for a specific entity, use that one directly
        entity = entities.get(entity_id)
    elif len(entities) == 1:
        # Otherwise, if there is only one entity, fall back to that one
        entity = next(iter(entities.values()))
    elif len(entities) > 1 and required:
        # If there are multiple entities and it's required,
        # the user must specify which one to use
        raise ClickException(
            f"More than one {entity_type} entity exists in the project definition file, "
            f"specify {disambiguation_option} to choose which one to operate on."
        )

    # If we don't have a package entity to convert, error out if it's required
    if not entity and required:
        with_id = f'with ID "{entity_id}" ' if entity_id else ""
        raise ClickException(
            f"Could not find an {entity_type} entity {with_id}in the project definition file."
        )

    return entity


def force_project_definition_v2(
    *, single_app_and_package: bool = True, app_required: bool = False
):
    """
    A command decorator that forces the project definition to be converted to v2.

    If a v1 definition is found, it is converted to v2 in-memory and the global context
    is updated with the new definition object.

    If a v2 definition is already found, it is used as-is, optionally limiting the number
    of application and package entities to one each (true by default).

    Assumes with_project_definition() has already been called.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cli_context = get_cli_context()
            original_pdf: Optional[ProjectDefinition] = cli_context.project_definition
            if not original_pdf:
                raise ValueError(
                    "Project definition could not be found. "
                    "The single_app_and_package() command decorator assumes "
                    "that with_project_definition() was called before it."
                )
            if isinstance(original_pdf, ProjectDefinitionV1):
                pdfv2 = convert_project_definition_to_v2(
                    cli_context.project_root,
                    original_pdf,
                    accept_templates=False,  # Templates should all be rendered by now
                    template_context=None,  # Force inclusion of all fields
                    in_memory=True,  # Convert the definition knowing it will be used immediately
                )
                for entity_id, entity in pdfv2.entities.items():
                    # Backfill kwargs for the command to use,
                    # there can only be one entity of each type
                    is_package = isinstance(entity, ApplicationPackageEntityModel)
                    key = "package_entity_id" if is_package else "app_entity_id"
                    kwargs[key] = entity_id

                cm = get_cli_context_manager()

                # Override the project definition so that the command operates on the new entities
                cm.override_project_definition = pdfv2
            elif single_app_and_package:
                package_entity_id = kwargs.get("package_entity_id", "")
                app_entity_id = kwargs.get("app_entity_id", "")
                app_definition, app_package_definition = _find_app_and_package_entities(
                    original_pdf, package_entity_id, app_entity_id, app_required
                )
                entities_to_keep = {app_package_definition.entity_id}
                kwargs["package_entity_id"] = app_package_definition.entity_id
                if app_definition:
                    entities_to_keep.add(app_definition.entity_id)
                    kwargs["app_entity_id"] = app_definition.entity_id
                for entity_id in list(original_pdf.entities):
                    if entity_id not in entities_to_keep:
                        # This happens after templates are rendered,
                        # so we can safely remove the entity
                        del original_pdf.entities[entity_id]
            return func(*args, **kwargs)

        if single_app_and_package:
            # Add the --app-entity-id and --package-entity-id options to the command
            return _options_decorator_factory(
                wrapper, additional_options=APP_AND_PACKAGE_OPTIONS
            )
        return wrapper

    return decorator
