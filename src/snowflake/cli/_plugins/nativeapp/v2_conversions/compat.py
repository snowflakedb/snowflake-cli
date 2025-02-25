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
from typing import Optional, Type, TypeVar

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
    DefinitionV20,
    ProjectDefinition,
    ProjectDefinitionV1,
)

APP_AND_PACKAGE_OPTIONS = [
    inspect.Parameter(
        "package_entity_id",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=typer.Option(
            default="",
            help="The ID of the package entity on which to operate when the definition_version is 2 or higher.",
        ),
    ),
    inspect.Parameter(
        "app_entity_id",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=typer.Option(
            default="",
            help="The ID of the application entity on which to operate when the definition_version is 2 or higher.",
        ),
    ),
]


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
                    entity_type = original_pdf.entities[entity_id].type.lower()
                    if (
                        entity_type in ["application", "application package"]
                        and entity_id not in entities_to_keep
                    ):
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
