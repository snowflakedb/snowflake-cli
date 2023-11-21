from unittest import mock
from unittest.mock import call

from snowcli.cli.common import flags
from snowcli.cli.common.cli_global_context import (
    global_context,
    global_context_manager,
    ConnectionContext,
)


def test_default_setup_of_global_connection():
    assert global_context_manager.connection_context == ConnectionContext(
        connection_name=None,
        account=None,
        database=None,
        role=None,
        schema=None,
        user=None,
        warehouse=None,
    )


def test_connection_details_callback():
    flags.RoleOption.callback("newValue")
    flags.WarehouseOption.callback("newValue2")
    assert global_context_manager.connection_context == ConnectionContext(
        connection_name=None,
        account=None,
        database=None,
        role="newValue",
        schema=None,
        user=None,
        warehouse="newValue2",
    )


@mock.patch("snowcli.cli.common.cli_global_context.connect_to_snowflake")
def test_connection_caching(mock_connect):
    flags.RoleOption.callback("newValue")
    flags.WarehouseOption.callback("newValue2")
    _ = global_context.connection
    assert mock_connect.call_count == 1

    flags.UserOption.callback("newValue3")
    assert mock_connect.call_count == 1

    _ = global_context.connection
    assert mock_connect.call_count == 2

    _ = global_context.connection
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
