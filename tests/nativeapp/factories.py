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

import os
from pathlib import Path

import factory
import yaml
from snowflake.cli.api.project.schemas.native_app.application import Application
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.package import Package
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV10,
    _ProjectDefinitionBase,
)

from tests.testing_utils.files_and_dirs import clear_none_values, merge_left


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

    # TODO: rewrite _create, no validation, and return none if no arguments are passed in
    distribution = None

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        if len(kwargs) == 1 and "distribution" in kwargs:
            return None
        return cls._build(model_class, *args, **kwargs)


class ApplicationFactory(factory.Factory):
    class Meta:
        model = Application

    # TODO: rewrite _create, no validation, and return none if no arguments are passed in
    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        if len(kwargs) == 0:
            return None
        return cls._build(model_class, *args, **kwargs)


class NativeAppFactory(factory.Factory):
    class Meta:
        model = NativeApp

    # TODO: package and application should be none unless they specify it
    # TODO: artifacts factory, should be PathMappingFactory?
    # Should be exactly what's passed in, if with src, dest or just src, write src
    # I dictate that interface for artifacts factory. artifacts__mapping: [{src: , dest:},{src:, dest:}] OR artifacts__paths: [src, src]
    name = factory.Faker("word")
    artifacts: list = []
    bundle_root = None
    deploy_root = None
    generated_root = None
    scratch_stage = None
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

    # @classmethod
    # def _create_with_merge(cls, model_class, *args, **kwargs):
    #     if "merge_project_definition" in kwargs:
    #         merge_definition = kwargs.pop("merge_project_definition")
    #     obj = cls._build(model_class, *args, **kwargs)
    #     pdf_dict = obj.model_dump()
    #     merge_left(pdf_dict, merge_definition)
    #     with open("snowflake.yml", "w") as file:
    #         yaml.dump(pdf_dict, file)
    #     return cls._build(model_class, *args, pdf_dict)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        temp_dir = os.getcwd()
        if "temp_dir" in kwargs:
            temp_dir = kwargs.pop("temp_dir")

        if "merge_project_definition" in kwargs:
            merge_definition = kwargs.pop("merge_project_definition")
        obj = cls._build(model_class, *args, **kwargs)

        pdf_dict = obj.model_dump()
        if merge_definition:
            merge_left(pdf_dict, merge_definition)
            pdf_dict = clear_none_values(pdf_dict)
            # TODO: if we are to return a new instance with merged props, we need to figure this part out
            obj = DefinitionV10.model_construct(values=pdf_dict)
        with open(Path(temp_dir) / "snowflake.yml", "w") as file:
            yaml.dump(pdf_dict, file)

        return obj


# TODO:
# - artifacts Factory
# - clean up
# DONE - don't write null to yml
# - rewrite some sample tests
#  after POC todos:
# - pdf v1.1
# - pdf v2
