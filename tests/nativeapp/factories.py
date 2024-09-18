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

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import factory
import yaml

from tests.testing_utils.files_and_dirs import clear_none_values, merge_left

# TODO:
# - don't like temp_dir - refactor
# - rewrite a few more sample tests
# - pdf path array return with local yml
# - how do we  parametrize multiple configs?
# - move factories to proper files/directories
# - write src/dest pair to pdf from project factory
# - manifest factory
# - add util to make readme and setup.sql with defaults?

# TODO
# - snowflake.local.yml support in V1.*

# TODO after POC:
# - pdf v2


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
    # We can throw a warning here for keys that are not in the schema?!
    pass


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


# TODO: can add utils to this class (to get yml parent path etc)
@dataclass
class PdfV10Result:
    def __init__(self, yml: Union[dict, str], path: Path):
        self.yml = yml
        self.path = path


class PdfV10Factory(factory.DictFactory):

    definition_version = "1"
    native_app = factory.SubFactory(NativeAppFactory)

    @classmethod
    def _create(cls, model_class, *args, **kwargs) -> PdfV10Result:
        temp_dir = kwargs.pop("temp_dir", os.getcwd())
        merge_definition = kwargs.pop("merge_project_definition", None)
        skip_write = kwargs.pop("skip_write", False)
        return_string = kwargs.pop("return_string", False)

        pdf_dict = cls._build(model_class, *args, **kwargs)

        if merge_definition:
            merge_left(pdf_dict, merge_definition)
            pdf_dict = clear_none_values(pdf_dict)

        if not skip_write:
            with open(Path(temp_dir) / "snowflake.yml", "w") as file:
                yaml.dump(pdf_dict, file)

        return PdfV10Result(
            yml=json.dumps(pdf_dict) if return_string else pdf_dict,
            path=Path(temp_dir) / "snowflake.yml",
        )


class PackageV11Factory(PackageFactory):
    pass


class ApplicationV11Factory(PackageFactory):
    pass


class NativeAppV11Factory(NativeAppFactory):
    package = factory.SubFactory(PackageV11Factory)
    application = factory.SubFactory(ApplicationV11Factory)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        return super()._create(model_class, *args, **kwargs)


class PdfV11Factory(PdfV10Factory):
    definition_version = "1.1"
    native_app = factory.SubFactory(NativeAppV11Factory)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        return super()._create(model_class, *args, **kwargs)


class FileModel:
    def __init__(self, filename, contents):
        self.filename = filename
        self.contents = contents


class FileFactory(factory.DictFactory):
    class Meta:
        model = FileModel

    filename = factory.Faker("file_name")
    contents = factory.Faker("text")

    @classmethod
    def _build(cls, model_class, *args, **kwargs):
        return kwargs["filename"]

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        filename = cls._build(model_class, *args, **kwargs)
        output_file = Path(filename)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as file:
            file.write(kwargs["contents"])
        return output_file


# TODO: when V1.1 and V2.* - use one factory and pick based on definition version 🤔
class ProjectV10FactoryModel:
    def __init__(self, pdf, artifact_files, extra_files):
        self.artifact_files = artifact_files
        self.pdf = pdf
        self.extra_files = extra_files


class ProjectV10Factory(factory.Factory):
    class Meta:
        model = ProjectV10FactoryModel

    artifact_files: list[FileModel] = []

    # TODO rewrite this to allow src/dest pairs for artifacts writing in pdf
    pdf = factory.SubFactory(
        PdfV10Factory,
        native_app__artifacts=factory.LazyAttribute(
            lambda pd: list(
                map(
                    lambda file: file["filename"],
                    pd.factory_parent.factory_parent.artifact_files,
                )
            )
            if pd.factory_parent.factory_parent.artifact_files
            else []
        ),
    )

    # TODO: MAYBE - should be able to specifiy a file on disk to reference here? would make it easier to not have to specify content?

    extra_files: list[FileModel] = []

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        for artifact in kwargs["artifact_files"]:
            FileFactory(filename=artifact["filename"], contents=artifact["contents"])
        for file in kwargs["extra_files"]:
            FileFactory(filename=file["filename"], contents=file["contents"])
        return super()._create(model_class, *args, **kwargs)
