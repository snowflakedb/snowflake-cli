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


@mock.patch("snowflake.cli.api.connections.get_connection_dict")
def test_update_from_config_skips_load_for_temporary_connection(
    mock_get_connection_dict,
):
    """Regression test for SNOW-3000366: update_from_config must not look up
    a named connection when temporary_connection is set, since there is
    nothing to merge and get_connection_dict(None) raises."""
    ctx = ConnectionContext(
        temporary_connection=True,
        account="acct",
        user="user",
        private_key_file="/key",
    )

    result = ctx.update_from_config()

    assert result is ctx
    mock_get_connection_dict.assert_not_called()
    # Ensure the explicit fields are preserved unchanged
    assert ctx.account == "acct"
    assert ctx.user == "user"
    assert ctx.private_key_file == "/key"


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


def test_connection_cache_cleanup_tolerates_missing_key(local_connection_cache):
    """_cleanup() must be a no-op when the key is not in the cache.

    The cleanup timer scheduled by _touch() races with clear() and with a
    second _cleanup() for the same key: whichever wins removes the
    connection, and the loser fires later with a stale key. Before this
    guard, the loser raised KeyError on `self.connections.pop(key)`.
    """
    # No connections in the cache — simulating a fired timer after clear().
    local_connection_cache._cleanup("missing-key")  # noqa: SLF001

    # Also verify that a lingering cleanup future for the missing key is
    # cancelled rather than leaked.
    fake_future = mock.Mock()
    local_connection_cache.cleanup_futures["orphan-key"] = fake_future
    local_connection_cache._cleanup("orphan-key")  # noqa: SLF001
    fake_future.cancel.assert_called_once()
    assert "orphan-key" not in local_connection_cache.cleanup_futures
