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

from unittest import mock
from unittest.mock import PropertyMock

from tests.nativeapp.utils import APP_PACKAGE_ENTITY_DISTRIBUTION_IN_SF


def mock_connection():
    connection = mock.Mock()
    connection.role = "role"
    connection.warehouse = "wh"
    return mock.patch(
        "snowflake.cli.api.cli_global_context._CliGlobalContextAccess.connection",
        new_callable=PropertyMock,
        return_value=connection,
    )


def mock_get_app_pkg_distribution_in_sf():
    return mock.patch(
        APP_PACKAGE_ENTITY_DISTRIBUTION_IN_SF,
    )


def mock_is_interactive_mode():
    return mock.patch(
        "snowflake.cli._plugins.nativeapp.utils.is_user_in_interactive_mode"
    )
