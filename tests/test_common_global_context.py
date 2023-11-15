from unittest import mock
from unittest.mock import call

from snowcli.cli.common.cli_global_context import (
    ConnectionDetails,
    update_global_connection_detail_callback,
    global_context,
)


def test_default_setup_of_global_connection():
    assert global_context.connection_details == ConnectionDetails(
        connection_name=None,
        account=None,
        database=None,
        role=None,
        schema=None,
        user=None,
        warehouse=None,
    )


def test_connection_details_callback():
    update_global_connection_detail_callback("role")("newValue")
    update_global_connection_detail_callback("warehouse")("newValue2")
    assert global_context.connection_details == ConnectionDetails(
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
    update_global_connection_detail_callback("role")("newValue")
    update_global_connection_detail_callback("warehouse")("newValue2")
    _ = global_context.connection
    assert mock_connect.call_count == 1

    update_global_connection_detail_callback("user")("newValue3")
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
