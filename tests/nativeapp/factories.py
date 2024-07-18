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
import yaml
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


class WithDumpYamlMixin:
    dump_filename: str | None = None

    @classmethod
    def _build(cls, model_class, *args, **kwargs):
        kwargs.pop("dump_filename", None)
        return super()._build(model_class, *args, **kwargs)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        if cls.dump_filename is None:
            raise ValueError(f"dump_filename must be set on {cls.__name__}")
        filename = kwargs.pop("dump_filename")
        obj = super()._create(model_class, *args, **kwargs)
        _dump_yaml(obj, filename)
        return obj


class _NativeAppManifestVersionFactory(factory.DictFactory):
    name = factory.Faker("word")
    label = factory.Faker("word")
    comment = factory.Faker("sentence")


class _NativeAppManifestArtifactsFactory(factory.DictFactory):
    setup_script_contents = "select 1;"
    readme_contents = factory.Faker("sentence")

    setup_script = "setup.sql"
    readme = "README.md"

    @classmethod
    def _build(cls, model_class, *args, **kwargs: str):
        kwargs.pop("setup_script_contents", None)
        kwargs.pop("readme_contents", None)
        return super()._build(model_class, *args, **kwargs)

    @classmethod
    def _create(cls, model_class, *args, **kwargs: str):
        obj = super()._create(model_class, *args, **kwargs)
        _dump_str(kwargs.pop("setup_script_contents"), kwargs["setup_script"])
        _dump_str(kwargs.pop("readme_contents"), kwargs["readme"])
        return obj


class _NativeAppManifestConfigurationFactory(factory.DictFactory):
    log_level = "INFO"
    trace_level = "ALWAYS"


class _NativeAppManifestFactory(WithDumpYamlMixin, factory.DictFactory):
    dump_filename = "manifest.yml"  # Where to save the manifest

    manifest_version = "1"
    version = factory.SubFactory(_NativeAppManifestVersionFactory)
    artifacts = factory.SubFactory(_NativeAppManifestArtifactsFactory)
    configuration = factory.SubFactory(_NativeAppManifestConfigurationFactory)


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
        manifest = factory.SubFactory(_NativeAppManifestFactory)

    name = factory.Faker("word")
    artifacts: list = []
    package = factory.SubFactory(PackageFactory)
    application = factory.SubFactory(ApplicationFactory)


class ProjectDefinitionBaseFactory(WithDumpYamlMixin, factory.Factory):
    class Meta:
        model = _ProjectDefinitionBase

    dump_filename = "snowflake.yml"  # Where to save the project definition file
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


class MultiVersionDefinitionFactory(factory.declarations.BaseDeclaration):
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

    project_definition = MultiVersionDefinitionFactory()
    project_context: Context = {}


def _dump_str(s: str, filename: str):
    with open(filename, "w") as f:
        f.write(s)


def _dump_yaml(data: dict, filename: str):
    with open(filename, "w") as f:
        yaml.dump(data, stream=f)
