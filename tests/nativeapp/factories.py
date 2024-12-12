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

"""
Factories to configure project definitions and write PDF and other files on disk for testing


"""


class FactoryNoEmptyDict(factory.DictFactory):
    """
    Dict Factory that returns None if empty, instead of {}
    """

    @classmethod
    def _build(cls, model_class, *args, **kwargs):
        if len(kwargs) == 0:
            return None
        return super()._build(model_class, *args, **kwargs)

    @classmethod
    def _create(cls, *args, **kwargs):
        return cls._build(*args, **kwargs)


class PackageFactory(FactoryNoEmptyDict):
    """
    Package V1.* Factory for creating dict representing package in PDF.

    Usage: PackageFactory(name="pkg_name", role="package_role")

    Package model has no required fields, therefore this is a simple NoEmptyDict factory.
    """

    pass


class ApplicationFactory(FactoryNoEmptyDict):
    """
    Application V1.* Factory for creating dict representing application in PDF.

    Usage: ApplicationFactory(name="my_app", role="app_role")

    Application model has no required fields, therefore this is a simple NoEmptyDict factory.
    """

    pass


class ArtifactFactory(factory.ListFactory):
    """
    List Factory for creating an artifact list.

    Usage:
        - ArtifactFactory(["setup.sql", "README.md"])
        - ArtifactFactory([
                {"src": "app/*", "dest": "./", "processors":["processor1"]},
                {"src": "setup.sql", "dest": "setup.sql"}
        ])
    """

    pass


class NativeAppFactory(factory.DictFactory):
    """
    Factory for preparing native app dict.

    Usage:
        Create a native app with a faker-generated name and an empty artifacts list:
        - NativeAppFactory()

        Create a native app with the given name and artifacts list:
        - NativeAppFactory(name="my_app", artifacts=[{"src": "app/*", "dest": "./"}])

        Creates a native app dict with package role set to "pkg_role":
        - NativeAppFactory(name="my_app", artifacts=["setup.sql", "README.md"], package__role="pkg_role")
    """

    name = factory.Faker("word")
    artifacts = factory.List([], list_factory=ArtifactFactory)
    package = factory.SubFactory(PackageFactory)
    application = factory.SubFactory(ApplicationFactory)

    @classmethod
    def _build(cls, model_class, *args, **kwargs):
        if kwargs["package"] is None:
            kwargs.pop("package")
        if kwargs["application"] is None:
            kwargs.pop("application")
        return super()._build(model_class, *args, **kwargs)

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        return cls._build(model_class, *args, **kwargs)


class MetaFieldFactory(factory.DictFactory):
    post_deploy = factory.List([])


class EntityModelBaseFactory(factory.DictFactory):
    meta = factory.SubFactory(MetaFieldFactory)


class ApplicationPackageEntityModelFactory(EntityModelBaseFactory):
    type = "application package"  # noqa: A003
    manifest = "manifest.yml"
    artifacts = factory.List(
        ["setup.sql", "README.md", "manifest.yml"], list_factory=ArtifactFactory
    )
    stage_subdirectory = ""


class ApplicationEntityModelFactory(EntityModelBaseFactory):
    type = "application"  # noqa: A003
    fromm = factory.Dict({"target": "pkg"})

    class Meta:
        rename = {"fromm": "from"}


@dataclass
class PdfFactoryResult:
    yml: dict
    path: Path = Path()

    def as_json_str(self):
        return json.dumps(self.yml)


class _PdfFactory(factory.DictFactory):
    """
    Base class to prepare PDF dict and write to file.

    Returns:
        PdfFactoryResult
    """

    env = factory.SubFactory(FactoryNoEmptyDict)
    _filename = "snowflake.yml"

    # for snowflake.local.yml
    @classmethod
    def with_filename(cls, filename):
        class _PdfFactoryWithFilename(cls):
            _filename = filename

        return _PdfFactoryWithFilename

    @classmethod
    def _build(cls, model_class, *args, **kwargs):
        if kwargs["env"] is None:
            kwargs.pop("env")
        yml = super()._build(model_class, *args, **kwargs)
        return PdfFactoryResult(yml=yml)

    @classmethod
    def _create(cls, model_class, *args, **kwargs) -> PdfFactoryResult:
        temp_dir = os.getcwd()

        res = cls._build(model_class, *args, **kwargs)

        with open(Path(temp_dir) / cls._filename, "w") as file:
            yaml.dump(res.yml, file)

        return PdfFactoryResult(
            yml=res.yml,
            path=Path(temp_dir) / cls._filename,
        )


class PdfV10Factory(_PdfFactory):
    """
    Prepare PDF 1.0 dict and write to file.

    Returns:
        PdfFactoryResult

    Usage:
        Create a PDF dict with definition_version: "1", native_app with faker-generated name and an empty artifacts list and
          write to snowflake.yml in current directory:
        - PdfV10Factory()

        Create snowflake.local.yml and write to file
        - PdfV10Factory.with_filename("snowflake.local.yml")(native_app__name="my_local_name")

        Build and return yml but do not write to file:
        - PdfV10Factory.build(
            native_app__name="my_app",
            native_app__artifacts=["setup.sql", "README.md"],
            native_app__package__role="pkg_role"
        )
    """

    definition_version = "1"
    native_app = factory.SubFactory(NativeAppFactory)


class PdfV11Factory(PdfV10Factory):
    """Override of Pdfv10Factory to set definition_version to 1.1"""

    definition_version = "1.1"


class PdfV2Factory(_PdfFactory):
    """
    Prepare PDF 2 dict and write to file.

    Returns:
        PdfFactoryResult

    Usage:
        Create a PDF dict with definition_version: "2" with empty list of entities and
          write to snowflake.yml in current directory:
        - PdfV2Factory()

        Create snowflake.local.yml with some entities and write to file
        - PdfV2Factory.with_filename("snowflake.local.yml")(
            entities=dict(
                pkg=ApplicationPackageEntityModelFactory(
                    identifier="myapp_pkg",
                ),
                app=ApplicationEntityModelFactory(
                    identifier="myapp",
                    fromm__target="pkg",
                ),
            )
        )

        Build and return yml but do not write to file:
        - PdfV2Factory.build(
            entities=dict(
                pkg=ApplicationPackageEntityModelFactory(
                    identifier="myapp_pkg",
                ),
                app=ApplicationEntityModelFactory(
                    identifier="myapp",
                    fromm__target="pkg",
                ),
            )
        )
    """

    definition_version = "2"
    entities = factory.Dict({})
    env = factory.Dict({})


@dataclass
class FileModel:
    filename: Union[str, Path]
    contents: str


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


@dataclass
class ProjectFactoryModel:

    pdf: dict
    files: list[FileModel]


ProjectFiles = dict[str | Path, str]


class _ProjectFactory(factory.Factory):
    """
    Factory to create PDF dict, and write in working directory PDF to snowflake.yml file, and other optional files.
    """

    class Meta:
        model = ProjectFactoryModel

    pdf = factory.SubFactory(_PdfFactory)
    files: ProjectFiles = {}

    @classmethod
    def _create(cls, model_class, *args, **kwargs):
        for filename, contents in kwargs["files"].items():
            FileFactory(filename=filename, contents=contents)
        return super()._create(model_class, *args, **kwargs)


class ProjectV10Factory(_ProjectFactory):
    pdf = factory.SubFactory(PdfV10Factory)


class ProjectV11Factory(ProjectV10Factory):
    pdf = factory.SubFactory(PdfV11Factory)


class ProjectV2Factory(_ProjectFactory):
    pdf = factory.SubFactory(PdfV2Factory)


class ManifestFactory(factory.DictFactory):
    version = factory.Dict(
        {
            "name": factory.Faker("word"),
            "label": factory.Faker("word"),
            "comment": factory.Faker("sentence"),
        }
    )
    artifacts = factory.Dict(
        {
            "setup_script": "setup.sql",
            "extension_code": True,
            "readme": "README.md",
        }
    )
    configuration = factory.Dict(
        {
            "log_level": "fatal",
            "trace_level": "always",
        }
    )

    @classmethod
    def _create(cls, model_class, *args, **kwargs) -> str:
        res = cls._build(model_class, *args, **kwargs)

        return yaml.dump(res)
