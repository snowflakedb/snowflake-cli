from snowcli.cli.common.snow_cli_global_context import (
    ConnectionDetails,
    snow_cli_global_context_manager,
)


def test_default_setup_of_global_context():
    assert (
        snow_cli_global_context_manager.get_global_context_copy().connection
        == ConnectionDetails(
            connection_name=None,
            account=None,
            database=None,
            role=None,
            schema=None,
            user=None,
            warehouse=None,
        )
    )


def test_connection_details_callback():
    ConnectionDetails.update_callback("role")("newValue")
    ConnectionDetails.update_callback("warehouse")("newValue2")
    assert (
        snow_cli_global_context_manager.get_global_context_copy().connection
        == ConnectionDetails(
            connection_name=None,
            account=None,
            database=None,
            role="newValue",
            schema=None,
            user=None,
            warehouse="newValue2",
        )
    )
