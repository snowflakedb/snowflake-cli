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

from pydantic import Field
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.entities.common import EntityBase, attach_spans_to_entity_actions
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBaseWithArtifacts,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
)
from snowflake.core import CreateMode

T = TypeVar("T")


class ProjectEntityModel(EntityModelBaseWithArtifacts):
    type: Literal["project"] = DiscriminatorField()  # noqa: A003
    stage: Optional[str] = Field(
        title="Stage in which the project artifacts will be stored", default=None
    )
    main_file: Optional[str] = Field(title="Path to the main file of the project")


@attach_spans_to_entity_actions(entity_name="project")
class ProjectEntity(EntityBase[ProjectEntityModel]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def action_deploy(
        self, action_ctx: ActionContext, mode: CreateMode, *args, **kwargs
    ):
        return self._execute_query(self.get_deploy_sql(mode))

    def action_drop(self, action_ctx: ActionContext, *args, **kwargs):
        return self._execute_query(self.get_drop_sql())

    def action_describe(self, action_ctx: ActionContext, *args, **kwargs):
        return self._execute_query(self.get_describe_sql())

    def action_execute(
        self,
        action_ctx: ActionContext,
        execution_arguments: List[str] | None = None,
        *args,
        **kwargs,
    ):
        return self._execute_query(self.get_execute_sql(execution_arguments))

    def get_execute_sql(self, execution_arguments):
        return "select 1"

    def get_deploy_sql(self, mode: CreateMode):
        pass
