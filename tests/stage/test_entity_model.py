from unittest import mock
from unittest.mock import MagicMock

import factory
import pytest
from snowflake.cli._plugins.stage.stage_entity import StageEntity
from snowflake.cli._plugins.stage.stage_entity_model import KindType, StageEntityModel
from snowflake.core import CreateMode
from snowflake.core.stage import Stage


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


def test_entity_model_basic_parameters():
    stage = StageFactory(name="jwilkowski", comment="a helpful comment")
    stage_model = StageEntityModel(type="stage", **stage.to_dict())
    # stage_model = StageEntityModel(type="stage", api_resource=stage)

    assert stage_model.type == "stage"
    assert stage_model.name == "jwilkowski"
    assert stage_model.kind == KindType.PERMANENT.value
    assert stage_model.comment == "a helpful comment"


def test_entity_create(mock_connect):
    mock_root = MagicMock()
    fake_stage = StageFactory()
    mock_stage_collection = MagicMock()

    # TODO: those mocks don't look great and would be irritating to maintain. Some clever helpers would be great
    mock_stage_collection.create.return_value.fetch.return_value = fake_stage
    mock_root.databases.__getitem__().schemas.__getitem__().stages = (
        mock_stage_collection
    )
    StageEntity.get_root = lambda: mock_root

    stage_entity = StageEntity.create(
        name=fake_stage.name,
        comment=fake_stage.comment,
        temporary=True if fake_stage.kind == KindType.TEMPORARY else False,
    )

    assert stage_entity.type == "stage"
    assert stage_entity.name == fake_stage.name
    mock_stage_collection.create.assert_called_once_with(
        fake_stage, mode=CreateMode.if_not_exists
    )
