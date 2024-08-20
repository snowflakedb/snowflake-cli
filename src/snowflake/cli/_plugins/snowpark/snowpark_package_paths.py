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
from typing import Dict, List

from snowflake.cli.api.secure_path import SecurePath

_DEFINED_REQUIREMENTS = "requirements.txt"
_REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"


@dataclass
class SnowparkPackagePaths:
    sources: List[SecurePath]
    defined_requirements_file: SecurePath = SecurePath(_DEFINED_REQUIREMENTS)
    snowflake_requirements_file: SecurePath = SecurePath(_REQUIREMENTS_SNOWFLAKE)
    dependencies_zip: SecurePath = SecurePath("dependencies.zip")

    @classmethod
    def for_snowpark_project(
        cls, project_root: SecurePath, snowpark_entities: Dict
    ) -> "SnowparkPackagePaths":
        sources = set()
        for name, entity in snowpark_entities.items():
            sources.update(entity.artifacts)

        return cls(
            sources=[
                cls.get_snowpark_project_source_absolute_path(
                    project_root, SecurePath(source)
                )
                for source in sources
            ],
            defined_requirements_file=project_root / _DEFINED_REQUIREMENTS,
            snowflake_requirements_file=project_root / _REQUIREMENTS_SNOWFLAKE,
            dependencies_zip=project_root / "dependencies.zip",
        )

    @classmethod
    def get_snowpark_project_source_absolute_path(
        cls, project_root: SecurePath | Path, defined_source_path: SecurePath | Path
    ) -> SecurePath:
        if isinstance(project_root, Path):
            project_root = SecurePath(project_root)

        if isinstance(defined_source_path, Path):
            defined_source_path = SecurePath(defined_source_path)

        if defined_source_path.path.is_absolute():
            return defined_source_path
        return SecurePath((project_root / defined_source_path.path).path.resolve())
