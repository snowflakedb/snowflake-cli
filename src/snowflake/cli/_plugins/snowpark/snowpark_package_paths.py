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

from dataclasses import dataclass
from pathlib import Path
from typing import List

from snowflake.cli.api.project.schemas.project_definition import DefinitionV20
from snowflake.cli.api.secure_path import SecurePath

_DEFINED_REQUIREMENTS = "requirements.txt"
_REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"


@dataclass
class SnowparkPackagePaths:
    sources: List[SecurePath]
    artifact_file: SecurePath
    defined_requirements_file: SecurePath = SecurePath(_DEFINED_REQUIREMENTS)
    snowflake_requirements_file: SecurePath = SecurePath(_REQUIREMENTS_SNOWFLAKE)

    @classmethod
    def for_snowpark_project(
        cls, project_root: SecurePath, project_definition: DefinitionV20
    ) -> "SnowparkPackagePaths":
        sources = set()
        entities = project_definition.get_entities_by_type(
            "function"
        ) | project_definition.get_entities_by_type("procedure")
        for name, entity in entities.items():
            sources.add(entity.artifacts)

        return cls(
            sources=[
                cls._get_snowpark_project_source_absolute_path(
                    project_root, SecurePath(source)
                )
                for source in sources
            ],
            artifact_file=cls._get_snowpark_project_artifact_absolute_path(
                project_root=project_root,
            ),
            defined_requirements_file=project_root / _DEFINED_REQUIREMENTS,
            snowflake_requirements_file=project_root / _REQUIREMENTS_SNOWFLAKE,
        )

    @classmethod
    def _get_snowpark_project_source_absolute_path(
        cls, project_root: SecurePath, defined_source_path: SecurePath
    ) -> SecurePath:
        if defined_source_path.path.is_absolute():
            return defined_source_path
        return SecurePath((project_root / defined_source_path.path).path.resolve())

    @classmethod
    def _get_snowpark_project_artifact_absolute_path(
        cls, project_root: SecurePath
    ) -> SecurePath:

        artifact_file = project_root / "app.zip"
        return artifact_file

    @property
    def sources_paths(self) -> List[Path]:
        return [source.path for source in self.sources]
