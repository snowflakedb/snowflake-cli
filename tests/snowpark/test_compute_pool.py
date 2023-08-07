from io import StringIO
from unittest import mock

import typing

from snowcli.snow_connector import SnowflakeConnector

MOCK_CONNECTION = {
    "database": "databaseValue",
    "schema": "schemaValue",
    "role": "roleValue",
    "warehouse": "warehouseValue",
}


class _MockConnectionCtx(mock.MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queries: typing.List[str] = []

    def get_query(self):
        return self.queries[0]

    @property
    def warehouse(self):
        return "MockWarehouse"

    @property
    def database(self):
        return "MockDatabase"

    @property
    def schema(self):
        return "MockSchema"

    @property
    def role(self):
        return "mockRole"

    def execute_stream(self, query: StringIO):
        self.queries.append(query.read())
        return (mock.MagicMock(),)


@mock.patch("snowflake.connector.connect")
def test_create_cp(mock_connector, runner, snapshot):
    ctx = _MockConnectionCtx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "snowpark",
            "cp",
            "create",
            "--name",
            "cpName",
            "--num",
            "42",
            "--family",
            "familyValue",
        ]
    )

    assert result.exit_code == 0
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_list_cp(mock_connector, runner, snapshot):
    ctx = _MockConnectionCtx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "cp", "list"])

    assert result.exit_code == 0
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_drop_cp(mock_connector, runner, snapshot):
    ctx = _MockConnectionCtx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "cp", "drop", "cpNameToDrop"])

    assert result.exit_code == 0
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_stop_cp(mock_connector, runner, snapshot):
    ctx = _MockConnectionCtx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "cp", "stop", "cpNameToStop"])

    assert result.exit_code == 0
    assert ctx.get_query() == snapshot
