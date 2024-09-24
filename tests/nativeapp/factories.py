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

import factory
import yaml


class FactoryNoEmptyDict(factory.DictFactory):
    @classmethod
    def _create(cls, *args, **kwargs):
        if len(kwargs) == 0:
            return None
        return cls._build(*args, **kwargs)


class PackageFactory(FactoryNoEmptyDict):
    # Package has no required fields
    pass


class ApplicationFactory(FactoryNoEmptyDict):
    # Application has no required fields
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


@dataclass
class PdfFactoryResult:
    def __init__(self, yml: dict, path: Path):
        self.yml = yml
        self.path = path

    def get_yml_string(self):
        return json.dumps(self.yml)


class PdfV10Factory(factory.DictFactory):

    definition_version = "1"
    native_app = factory.SubFactory(NativeAppFactory)
    env = factory.SubFactory(FactoryNoEmptyDict)
    _filename = "snowflake.yml"

    # for snowflake.local.yml
    @classmethod
    def with_filename(cls, filename):
        class PdfV10FactoryWithFilename(cls):
            cls._filename = filename

        return PdfV10FactoryWithFilename

    @classmethod
    def _build(cls, model_class, *args, **kwargs):
        if kwargs["env"] is None:
            kwargs.pop("env")
        return super()._build(model_class, *args, **kwargs)

    @classmethod
    def _create(cls, model_class, *args, **kwargs) -> PdfFactoryResult:
        temp_dir = os.getcwd()

        yml = cls._build(model_class, *args, **kwargs)

        with open(Path(temp_dir) / cls._filename, "w") as file:
            yaml.dump(yml, file)

        return PdfFactoryResult(
            yml=yml,
            path=Path(temp_dir) / cls._filename,
        )


class PdfV11Factory(PdfV10Factory):
    definition_version = "1.1"


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


class ProjectFactoryModel:
    def __init__(self, pdf, files):
        self.pdf = pdf
        self.files = files


class ProjectV10Factory(factory.Factory):
    class Meta:
        model = ProjectFactoryModel

    pdf = factory.SubFactory(PdfV10Factory)

    # TODO: Should be able to specifiy a file on disk to reference here?
    # TODO: filename: content dictionary instead?
    files: list[FileModel] = []

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        for file in kwargs["files"]:
            FileFactory(filename=file["filename"], contents=file["contents"])
        return super()._create(model_class, *args, **kwargs)


# TODO: use one factory and pick based on definition version
class ProjectV11Factory(ProjectV10Factory):

    pdf = factory.SubFactory(PdfV11Factory)
