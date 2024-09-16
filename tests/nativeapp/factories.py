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

from tests.testing_utils.files_and_dirs import clear_none_values, merge_left

# TODO
# - Artifacts factory
# - Write other files
# - Some defaults


class FactoryNoEmptyDict(factory.DictFactory):
    @classmethod
    def _create(cls, *args, **kwargs):
        if len(kwargs) == 0:
            return None
        return cls._build(*args, **kwargs)


class PackageFactory(FactoryNoEmptyDict):
    # Package has no required fields
    # We can throw a warning here for keys that are not in the schema?!
    pass


class ApplicationFactory(FactoryNoEmptyDict):
    # We can throw a warning here for keys that are not in the schema
    pass


# TODO: artifacts factory
# Should be exactly what's passed in, if with src, dest or just src, write src
# I dictate that interface for artifacts factory. artifacts__mapping: [{src: , dest:},{src:, dest:}] OR artifacts__paths: [src, src]


class ArtifactFactory(factory.ListFactory):
    pass


class NativeAppFactory(factory.DictFactory):

    name = factory.Faker("word")
    artifacts = factory.List([], list_factory=ArtifactFactory)
    package = factory.SubFactory(PackageFactory)
    application = factory.SubFactory(ApplicationFactory)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        if kwargs["package"] is None:
            kwargs.pop("package")
        if kwargs["application"] is None:
            kwargs.pop("application")
        return cls._build(model_class, *args, **kwargs)


class PdfV10Factory(factory.DictFactory):

    definition_version = "1"
    native_app = factory.SubFactory(NativeAppFactory)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        temp_dir = os.getcwd()
        merge_definition = None

        if "temp_dir" in kwargs:
            temp_dir = kwargs.pop("temp_dir")

        if "merge_project_definition" in kwargs:
            merge_definition = kwargs.pop("merge_project_definition")

        pdf_dict = cls._build(model_class, *args, **kwargs)

        if merge_definition:
            merge_left(pdf_dict, merge_definition)
            pdf_dict = clear_none_values(pdf_dict)

        if "skip_write" not in kwargs:
            with open(Path(temp_dir) / "snowflake.yml", "w") as file:
                yaml.dump(pdf_dict, file)

        return pdf_dict


# TODO:
# - artifacts Factory
# - clean up
# - rewrite some sample tests
# - ensure works: names with spaces in them
# - pass package or app as whole dicts

# TODO after POC:
# - pdf v1.1
# - pdf v2

# How to build a dict from factory
# factory.build(dict, FACTORY_CLASS=UserFactory)
