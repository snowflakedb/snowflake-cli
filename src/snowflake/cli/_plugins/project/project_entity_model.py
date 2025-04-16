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

from typing import List, Literal, Optional, TypeVar

from pydantic import Field, field_validator
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.entities.common import EntityBase, attach_spans_to_entity_actions
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.project.schemas.entities.common import (
    Artifacts,
    EntityModelBaseWithArtifacts,
    PathMapping,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)
from snowflake.cli.api.secure_path import SecurePath

T = TypeVar("T")


MANIFEST_FILE_NAME = "manifest.yml"


class ProjectEntityModel(EntityModelBaseWithArtifacts):
    type: Literal["project"] = DiscriminatorField()  # noqa: A003
    stage: Optional[str] = Field(
        title="Stage in which the project artifacts will be stored", default=None
    )

    @field_validator("artifacts")
    @classmethod
    def transform_artifacts(cls, orig_artifacts: Artifacts) -> List[PathMapping]:
        if not (
            SecurePath(get_cli_context().project_root) / MANIFEST_FILE_NAME
        ).exists():
            raise CliError(
                f"{MANIFEST_FILE_NAME} was not found in project root directory"
            )
        orig_artifacts.append(PathMapping(src=MANIFEST_FILE_NAME))
        return super().transform_artifacts(orig_artifacts)


@attach_spans_to_entity_actions(entity_name="project")
class ProjectEntity(EntityBase[ProjectEntityModel]):
    """Placeholder for project entity"""
