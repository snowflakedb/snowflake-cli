from unittest import mock
from unittest.mock import PropertyMock

from tests.nativeapp.utils import NATIVEAPP_MANAGER_APP_PKG_DISTRIBUTION_IN_SF


def mock_connection():
    return mock.patch(
        "snowflake.cli.api.cli_global_context._CliGlobalContextAccess.connection",
        new_callable=PropertyMock,
    )


def mock_get_app_pkg_distribution_in_sf():
    return mock.patch(
        NATIVEAPP_MANAGER_APP_PKG_DISTRIBUTION_IN_SF,
        new_callable=PropertyMock,
    )


def mock_is_interactive_mode():
    return mock.patch(
        "snowflake.cli.plugins.nativeapp.utils.is_user_in_interactive_mode"
    )
