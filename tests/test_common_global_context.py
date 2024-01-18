from unittest import mock
from unittest.mock import call

import pytest
from snowflake.cli.api.cli_global_context import cli_context, cli_context_manager
from snowflake.cli.api.commands import flags
from snowflake.cli.api.exceptions import InvalidSchemaError


def test_default_setup_of_global_connection():
    assert cli_context_manager.connection_context.connection_name is None
    assert cli_context_manager.connection_context.account is None
    assert cli_context_manager.connection_context.database is None
    assert cli_context_manager.connection_context.role is None
    assert cli_context_manager.connection_context.schema is None
    assert cli_context_manager.connection_context.user is None
    assert cli_context_manager.connection_context.password is None
    assert cli_context_manager.connection_context.authenticator is None
    assert cli_context_manager.connection_context.private_key_path is None
    assert cli_context_manager.connection_context.warehouse is None
    assert cli_context_manager.connection_context.temporary_connection is False


def test_connection_details_callback():
    flags.RoleOption.callback("newValue")
    flags.WarehouseOption.callback("newValue2")

    assert cli_context_manager.connection_context.connection_name is None
    assert cli_context_manager.connection_context.account is None
    assert cli_context_manager.connection_context.database is None
    assert cli_context_manager.connection_context.role is "newValue"
    assert cli_context_manager.connection_context.schema is None
    assert cli_context_manager.connection_context.user is None
    assert cli_context_manager.connection_context.password is None
    assert cli_context_manager.connection_context.authenticator is None
    assert cli_context_manager.connection_context.private_key_path is None
    assert cli_context_manager.connection_context.warehouse is "newValue2"
    assert cli_context_manager.connection_context.temporary_connection is False


@mock.patch("snowflake.cli.app.snow_connector.connect_to_snowflake")
def test_connection_caching(mock_connect):
    flags.RoleOption.callback("newValue")
    flags.WarehouseOption.callback("newValue2")
    _ = cli_context.connection
    assert mock_connect.call_count == 1

    flags.UserOption.callback("newValue3")
    assert mock_connect.call_count == 1

    _ = cli_context.connection
    assert mock_connect.call_count == 2

    _ = cli_context.connection
    assert mock_connect.call_count == 2

    mock_connect.assert_has_calls(
        [
            call(
                temporary_connection=False,
                connection_name=None,
                role="newValue",
                warehouse="newValue2",
            ),
            call(
                temporary_connection=False,
                connection_name=None,
                role="newValue",
                warehouse="newValue2",
                user="newValue3",
            ),
        ]
    )


@pytest.mark.parametrize("schema", ["my_schema", '".my_schema3"', '"my.schema"'])
def test_schema_validation_ok(schema):
    cli_context_manager.connection_context.set_schema(schema)


@pytest.mark.parametrize("schema", ["db.schema"])
def test_schema_validation_error(schema):
    with pytest.raises(InvalidSchemaError) as e:
        cli_context_manager.connection_context.set_schema(schema)
        assert e.value.message == f"Invalid schema {schema}"
