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

from dataclasses import asdict
from unittest import mock

import pytest
from snowflake.cli.api.connections import ConnectionContext, OpenConnectionCache


@pytest.fixture
def local_connection_cache():
    cache = OpenConnectionCache()
    yield cache
    cache.clear()


@pytest.mark.parametrize(
    "args",
    [
        {},
        {"connection_name": "myconn"},
        {"temporary_connection": True, "account": "myacct", "user": "myuser"},
    ],
)
def test_stable_connection_context_repr(args: dict, snapshot):
    ctx = ConnectionContext()
    ctx.update(**args)
    ctx.validate_and_complete()
    assert repr(ctx) == snapshot


def test_clone_connection_context():
    """
    Tests that the clone() method is working properly.
    """

    keys = (
        "connection_name",
        "account",
        "database",
        "role",
        "schema",
        "user",
        "password",
        "authenticator",
        "warehouse",
        "session_token",
        "master_token",
    )

    old_ctx = ConnectionContext()
    for key in keys:
        setattr(old_ctx, key, "value")

    new_ctx = old_ctx.clone()
    assert asdict(new_ctx) == asdict(old_ctx)

    for key in keys:
        setattr(new_ctx, key, "updated_values_should_not_appear_in_old_ctx")
        assert getattr(old_ctx, key) == "value"


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_connection_cache_caches(
    mock_command_info, mock_connect, local_connection_cache, test_snowcli_config
):
    mock_command_info.return_value = "application"

    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    ctx = ConnectionContext(connection_name="default")

    local_connection_cache[ctx]
    local_connection_cache[ctx]
    local_connection_cache[ctx]

    mock_connect.assert_called_once_with(
        application=mock_command_info.return_value,
        database="db_for_test",
        schema="test_public",
        role="test_role",
        warehouse="xs",
        password="dummy_password",
        application_name="snowcli",
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_connection_cache_caches_failures(
    mock_command_info, mock_connect, local_connection_cache, test_snowcli_config
):
    """Once a connect() call fails, subsequent accesses must re-raise without
    re-dialing — otherwise auth-policy rejection logs duplicate LOGIN_HISTORY
    events (one per access of the CLI's global connection: pre-command
    telemetry, command body, error handler, post-command telemetry).
    """
    from snowflake.cli.api.exceptions import InvalidConnectionConfigurationError
    from snowflake.connector.errors import DatabaseError

    mock_command_info.return_value = "application"
    mock_connect.side_effect = DatabaseError(
        msg="Failed to connect to DB: host:port. Sign-in disallowed by authentication policy",
        errno=250001,
    )

    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    ctx = ConnectionContext(connection_name="default")

    cached_exc = None
    for _ in range(3):
        with pytest.raises(InvalidConnectionConfigurationError) as excinfo:
            local_connection_cache[ctx]
        if cached_exc is None:
            cached_exc = excinfo.value
        else:
            assert excinfo.value is cached_exc

    assert mock_connect.call_count == 1


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_connection_cache_clear_failures_allows_retry(
    mock_command_info, mock_connect, local_connection_cache, test_snowcli_config
):
    from snowflake.cli.api.exceptions import InvalidConnectionConfigurationError
    from snowflake.connector.errors import DatabaseError

    mock_command_info.return_value = "application"
    mock_connect.side_effect = [
        DatabaseError(msg="boom", errno=250001),
        mock.MagicMock(),
    ]

    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    ctx = ConnectionContext(connection_name="default")

    with pytest.raises(InvalidConnectionConfigurationError):
        local_connection_cache[ctx]
    assert mock_connect.call_count == 1

    local_connection_cache.clear_failures()

    local_connection_cache[ctx]
    assert mock_connect.call_count == 2


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_connection_cache_clear_also_forgets_failures(
    mock_command_info, mock_connect, local_connection_cache, test_snowcli_config
):
    from snowflake.cli.api.exceptions import InvalidConnectionConfigurationError
    from snowflake.connector.errors import DatabaseError

    mock_command_info.return_value = "application"
    mock_connect.side_effect = [
        DatabaseError(msg="boom", errno=250001),
        mock.MagicMock(),
    ]

    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    ctx = ConnectionContext(connection_name="default")

    with pytest.raises(InvalidConnectionConfigurationError):
        local_connection_cache[ctx]

    local_connection_cache.clear()

    local_connection_cache[ctx]
    assert mock_connect.call_count == 2
