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
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.entities.common import EntityBase
from snowflake.cli.api.identifiers import FQN
from snowflake.connector import SnowflakeConnection
from snowflake.core import CreateMode
from snowflake.core.exceptions import NotFoundError
from snowflake.core.stage import StageCollection, StageResource


class StageEntity(EntityBase[StageEntityModel]):
    _resource: Optional[StageResource] = None
    _exists: bool = False

    def __init__(self, fqn: FQN, *args, **kwargs):
        entity_model_cls = self.get_entity_model_type()
        try:
            resource = self.get_resource(fqn)
            self._resource = resource
            _entity_model = entity_model_cls(type="stage", _model=resource.fetch())
            self._exists = True
        except NotFoundError:
            _entity_model = entity_model_cls(type="stage", _model={"name": fqn.name})
        except Exception as e:
            raise e
        super().__init__(_entity_model, *args, **kwargs)

    # TODO: discuss: those 2 methods could go to a mixing class for resources that support using snowapis
    #       figure out typing as we might often deal with <T>Collection, <T>Resource and <T>
    def get_collection(self, fqn: Optional[FQN] = None) -> StageCollection:
        conn: SnowflakeConnection = get_cli_context().connection
        root = self.snow_api_root
        if root is not None:
            db = fqn.database if fqn is not None and fqn.database else conn.database
            schema = fqn.schema if fqn is not None and fqn.schema else conn.schema
            return root.databases[db].schemas[schema].stages
        raise Exception("Could not init root")

    def get_resource(self, fqn: FQN) -> StageResource:
        return self.get_collection(fqn)[fqn.name]

    # TODO: discuss: this code looks mostly generic so might go as well to the mixin class. Don't do it too fast to
    #       avoid issues of premature abstraction
    def create(self, model_kwargs: Optional[dict] = None) -> "StageEntity":
        if model_kwargs is None:
            model_kwargs = {}
        stage_collection = self.get_collection()
        for attr, value in model_kwargs.items():
            setattr(self.model.snowapi_model, attr, value)
        stage_resource = stage_collection.create(
            self.model.snowapi_model, mode=CreateMode.if_not_exists
        )
        self.model.snowapi_model = stage_resource.fetch()
        self._resource = stage_resource
        self._exists = True
        return self

    def remove(self) -> None:
        self.get_resource(self.model.name).drop()

    # def clone(self):
    #     pass
    #
    # def get(self):
    #     pass
    #
    # def put(self):
    #     pass
