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

from typing import cast

import factory
from snowflake.cli.api.project.schemas.entities.entities import Entity
from snowflake.cli.api.project.schemas.native_app.application import Application
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.package import Package
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV10,
    DefinitionV11,
    DefinitionV20,
    ProjectProperties,
    _ProjectDefinitionBase,
)
from snowflake.cli.api.utils.types import Context


class FileFactory(factory.Factory):
    filename = factory.Faker("file_name")
    contents = factory.Faker("text")

    @classmethod
    def _build(cls, model_class, *args, **kwargs: str) -> str:
        return kwargs["filename"]

    @classmethod
    def _create(cls, model_class, *args, **kwargs: str):
        filename = cls._build(model_class, *args, **kwargs)
        with open(filename, "w") as file:
            file.write(kwargs["contents"])
        return filename


class NativeAppManifestVersionFactory(factory.DictFactory):
    name = factory.Faker("word")
    label = factory.Faker("word")
    comment = factory.Faker("sentence")


class NativeAppManifestArtifactsFactory(factory.DictFactory):
    setup_script = factory.SubFactory(
        FileFactory, filename="setup.sql", contents="select 1;"
    )
    readme = factory.SubFactory(FileFactory, filename="README.md")


class NativeAppManifestConfigurationFactory(factory.DictFactory):
    log_level = "INFO"
    trace_level = "ALWAYS"


class NativeAppManifestFactory(factory.DictFactory):
    dump_filename = "manifest.yml"  # Where to save the manifest

    manifest_version = "1"
    version = factory.SubFactory(NativeAppManifestVersionFactory)
    artifacts = factory.SubFactory(NativeAppManifestArtifactsFactory)
    configuration = factory.SubFactory(NativeAppManifestConfigurationFactory)


class PackageFactory(factory.Factory):
    class Meta:
        model = Package


class ApplicationFactory(factory.Factory):
    class Meta:
        model = Application


class NativeAppFactory(factory.Factory):
    class Meta:
        model = NativeApp

    class Params:
        manifest = factory.SubFactory(NativeAppManifestFactory)

    name = factory.Faker("word")
    artifacts: list = []
    package = factory.SubFactory(PackageFactory)
    application = factory.SubFactory(ApplicationFactory)


class ProjectDefinitionBaseFactory(factory.Factory):
    class Meta:
        model = _ProjectDefinitionBase

    definition_version = "0"  # This will fail validation, so it needs to be overridden


class DefinitionV10Factory(ProjectDefinitionBaseFactory):
    class Meta:
        model = DefinitionV10

    definition_version = "1"
    native_app = factory.SubFactory(NativeAppFactory)


class DefinitionV11Factory(DefinitionV10Factory):
    class Meta:
        model = DefinitionV11

    definition_version = "1.1"
    env: dict[str, str] = {}


class DefinitionV20Factory(DefinitionV10Factory):
    class Meta:
        model = DefinitionV20

    definition_version = "2.0"
    entities: dict[str, Entity] = {}


class ProjectDefinition(factory.declarations.BaseDeclaration):
    SUB_FACTORIES = {
        cast(ProjectDefinitionBaseFactory, cls).definition_version: factory.SubFactory(
            cls
        )
        for cls in (
            ProjectDefinitionBaseFactory,
            DefinitionV10Factory,
            DefinitionV11Factory,
            DefinitionV20Factory,
        )
    }

    def evaluate(self, instance, step, extra):
        sub_factory = self.SUB_FACTORIES[instance.definition_version]
        return sub_factory.evaluate(instance, step, extra)


class ProjectPropertiesFactory(factory.Factory):
    class Meta:
        model = ProjectProperties

    class Params:
        definition_version = "1.1"  # PDF v1.1 is the latest public version

    project_definition = ProjectDefinition()
    project_context: Context = {}
