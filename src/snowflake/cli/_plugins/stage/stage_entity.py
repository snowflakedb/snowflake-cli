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

from typing import Optional

from snowflake.cli._plugins.stage.stage_entity_model import StageEntityModel
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.project.schemas.entities.common import Identifier
from snowflake.connector import SnowflakeConnection
from snowflake.core import CreateMode
from snowflake.core.stage import StageCollection, StageResource


class StageEntity(EntityBase[StageEntityModel]):
    # TODO: discuss: those 2 methods could go to a mixing class for resources that support using snowapis
    #       figure out typing as we might often deal with <T>Collection, <T>Resource and <T>
    def get_collection(
        self, identifier: Optional[Identifier] = None
    ) -> StageCollection:
        conn: SnowflakeConnection = get_cli_context().connection
        root = self.snow_api_root
        if root is not None:
            db = (
                identifier.database
                if identifier is not None and identifier.database
                else conn.database
            )
            schema = (
                identifier.schema_
                if identifier is not None and identifier.schema_
                else conn.schema
            )
            return root.databases[db].schemas[schema].stages
        raise Exception("Could not init root")

    def get_resource(self, identifier: Identifier) -> StageResource:
        return self.get_collection(identifier)[identifier.name]

    def action_create(
        self, action_ctx: ActionContext, model_kwargs: Optional[dict] = None
    ) -> "StageEntity":
        return self.create(model_kwargs)

    def create(self, model_kwargs: Optional[dict] = None) -> "StageEntity":
        if model_kwargs is None:
            model_kwargs = {}
        model_kwargs["name"] = self.model.identifier.name
        stage_collection = self.get_collection()
        for attr, value in model_kwargs.items():
            setattr(self.model.snowapi_model, attr, value)
        self.model.revalidate()
        stage_resource = stage_collection.create(
            self.model.snowapi_model, mode=CreateMode.if_not_exists
        )
        self.model.snowapi_model = stage_resource.fetch()
        return self

    def action_drop(self, action_ctx: ActionContext) -> None:
        return self.drop()

    def drop(self) -> None:
        self.model.snowapi_model = None
        self.get_resource(self.model.identifier).drop()

    def action_fetch(self, action_ctx: ActionContext) -> "StageEntity":
        return self.fetch()

    def fetch(self) -> "StageEntity":
        self.model.snowapi_model = self.get_resource(self.model.identifier).fetch()
        return self

    # def clone(self):
    #     pass
    #
    # def get(self):
    #     pass
    #
    # def put(self):
    #     pass
