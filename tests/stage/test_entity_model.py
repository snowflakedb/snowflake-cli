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

from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import factory
import pytest
from snowflake.cli._plugins.stage.stage_entity_model import KindType, StageEntityModel
from snowflake.cli._plugins.workspace.context import WorkspaceContext
from snowflake.core import CreateMode
from snowflake.core.exceptions import NotFoundError
from snowflake.core.stage import Stage

from tests.nativeapp.patch_utils import mock_connection


@pytest.fixture()
def mock_connect():
    with mock.patch(
        "snowflake.cli._app.snow_connector.connect_to_snowflake"
    ) as mock_connect:
        yield mock_connect


# TODO: discuss if this is needed. I feel this or StageEntityModelFactory could be mostly useful for quickly
#       building Stages for other entities, where stages are not that important, just need to exist
class StageFactory(factory.Factory):
    class Meta:
        model = Stage

    name = factory.Faker("word")
    comment = factory.Faker("sentence")


class WorkspaceContextFactory(factory.Factory):
    class Meta:
        model = WorkspaceContext

    console = mock.MagicMock()
    project_root = Path(__file__)
    get_default_role = lambda: "role"
    get_default_warehouse = lambda: "warehouse"


class RootDouble(MagicMock):
    def assert_database(self, expected):
        self.databases.__getitem__.assert_called_with(expected)

    def assert_schema(self, expected):
        self.databases.__getitem__().schemas.__getitem__.assert_called_with(expected)


def test_entity_model_basic_parameters():
    stage = StageFactory(name="jwilkowski", comment="a helpful comment")
    stage_model = StageEntityModel(type="stage", _model=stage)

    assert stage_model.type == "stage"
    assert stage_model.name == "jwilkowski"
    assert stage_model.kind == KindType.PERMANENT.value
    assert stage_model.comment == "a helpful comment"


@mock.patch(
    "snowflake.cli._plugins.stage.stage_entity.StageEntity.snow_api_root",
    new_callable=mock.PropertyMock,
    return_value=RootDouble(),
)
@mock_connection(database="test_db", schema="test_schema")
def test_stage_create(_mock_connection, mock_root, runner):
    mock_stage_collection = MagicMock()
    mock_stage_collection.create.return_value.fetch.return_value = StageFactory(
        name="test_stage"
    )
    mock_stage_collection.__getitem__().fetch.side_effect = NotFoundError(mock_root)
    mock_root.return_value.databases.__getitem__().schemas.__getitem__().stages = (
        mock_stage_collection
    )

    result = runner.invoke(["stage", "createv2", "test_stage"])

    mock_root().assert_database("test_db")
    mock_root().assert_schema("test_schema")
    assert result.exit_code == 0, result.output
    mock_stage_collection.create.assert_called_once_with(
        Stage(name="test_stage"), mode=CreateMode.if_not_exists
    )
