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
from snowflake.cli.api.connections import (
    _REDACTED,
    ConnectionContext,
    OpenConnectionCache,
)
from snowflake.cli.api.exceptions import InvalidConnectionConfigurationError
from snowflake.connector.errors import DatabaseError

_SECRET_SENTINEL = "do-not-log-this-secret"
_SENSITIVE_FIELDS = ConnectionContext._sensitive_field_names()  # noqa: SLF001


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


@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_connection_cache_caches_failures(
    mock_command_info, mock_connect, local_connection_cache, test_snowcli_config
):
    """Once a connect() call fails, subsequent accesses must re-raise without
    re-dialing — otherwise auth-policy rejection logs duplicate LOGIN_HISTORY
    events (one per access of the CLI's global connection: pre-command
    telemetry, command body, error handler, post-command telemetry).
    """
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


@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_connection_cache_clear_failures_allows_retry(
    mock_command_info, mock_connect, local_connection_cache, test_snowcli_config
):
    mock_command_info.return_value = "application"
    mock_connect.side_effect = [
        DatabaseError(msg="boom", errno=250001),
        mock_connect.mocked_ctx,
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


@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_connection_cache_clear_also_forgets_failures(
    mock_command_info, mock_connect, local_connection_cache, test_snowcli_config
):
    mock_command_info.return_value = "application"
    mock_connect.side_effect = [
        DatabaseError(msg="boom", errno=250001),
        mock_connect.mocked_ctx,
    ]

    from snowflake.cli.api.config import config_init

    config_init(test_snowcli_config)

    ctx = ConnectionContext(connection_name="default")

    with pytest.raises(InvalidConnectionConfigurationError):
        local_connection_cache[ctx]

    local_connection_cache.clear()

    local_connection_cache[ctx]
    assert mock_connect.call_count == 2


# Regression tests for SNOW-3417288 — credential exposure through repr.
# ConnectionContext.__repr__ used to embed raw password / token / private_key
# values because it built on top of dataclasses.asdict(), which ignores the
# field(repr=False) marker. Debug logs that include repr(ctx) land in
# ~/.snowflake/logs/ by default, so the leak was persistent.


@pytest.mark.parametrize("field_name", sorted(_SENSITIVE_FIELDS))
def test_repr_redacts_every_sensitive_field(field_name: str):
    """Each known credential-bearing field must be redacted in repr()."""
    ctx = ConnectionContext()
    setattr(ctx, field_name, _SECRET_SENTINEL)
    rendered = repr(ctx)
    assert _SECRET_SENTINEL not in rendered
    # The field name itself SHOULD still be visible so operators can see
    # which auth method was in play when debugging.
    assert f"{field_name}=" in rendered
    assert repr(_REDACTED) in rendered


def test_redacted_constant_matches_masking_module():
    """``connections._REDACTED`` is intentionally a local copy of
    ``config_ng.masking.MASKED_VALUE`` (direct import would cause a circular
    dependency through ``config_ng/__init__.py``). This test keeps the two in
    sync so log readers see a single, consistent redaction style across the
    CLI.
    """
    from snowflake.cli.api.config_ng.masking import MASKED_VALUE

    assert _REDACTED == MASKED_VALUE


def test_sensitive_field_names_matches_repr_false_markers():
    """Regression guard: _SENSITIVE_FIELDS must be derived from the dataclass
    ``repr=False`` metadata, not a separate hand-maintained list. If a future
    contributor adds a credential field with ``repr=False`` the redaction
    should pick it up automatically.
    """
    from dataclasses import fields

    expected = {
        f.name
        for f in fields(ConnectionContext)
        if not f.repr and not f.name.startswith("_")
    }
    assert ConnectionContext._sensitive_field_names() == expected  # noqa: SLF001
    # Sanity: the known credential fields must be in the set today.
    known_credentials = {
        "password",
        "token",
        "session_token",
        "master_token",
        "private_key_raw",
        "private_key_passphrase",
        "oauth_client_secret",
        "mfa_passcode",
    }
    sensitive = ConnectionContext._sensitive_field_names()  # noqa: SLF001
    assert known_credentials.issubset(sensitive)


def test_repr_preserves_non_sensitive_fields():
    """Non-secret fields must continue to render verbatim."""
    ctx = ConnectionContext(
        connection_name="myconn",
        account="myacct",
        user="myuser",
        role="myrole",
        password=_SECRET_SENTINEL,
    )
    rendered = repr(ctx)
    assert "connection_name='myconn'" in rendered
    assert "account='myacct'" in rendered
    assert "user='myuser'" in rendered
    assert "role='myrole'" in rendered
    assert _SECRET_SENTINEL not in rendered


def test_safe_values_as_dict_redacts_sensitive_and_keeps_others():
    ctx = ConnectionContext(
        account="myacct",
        user="myuser",
        password=_SECRET_SENTINEL,
        token=_SECRET_SENTINEL,
    )
    safe = ctx.safe_values_as_dict()
    assert safe["account"] == "myacct"
    assert safe["user"] == "myuser"
    assert safe["password"] == _REDACTED
    assert safe["token"] == _REDACTED
    # Sanity: present_values_as_dict (used to build the live connection) must
    # still return the real credential values — we depend on that behaviour
    # for connect() calls.
    live = ctx.present_values_as_dict()
    assert live["password"] == _SECRET_SENTINEL
    assert live["token"] == _SECRET_SENTINEL


def test_cache_key_disambiguates_contexts_that_differ_only_by_credential():
    """
    If the cache keyed off repr(ctx), two contexts that share all non-secret
    fields but differ by password would collide and return the same connection
    — an availability regression as well as a security footgun. _full_cache_key
    must distinguish them while NOT retaining the raw credential as a dict-key
    string (which is why we hash).
    """
    ctx_a = ConnectionContext(connection_name="shared", password="pwd-A")
    ctx_b = ConnectionContext(connection_name="shared", password="pwd-B")
    assert repr(ctx_a) == repr(ctx_b)  # redacted repr collides (by design)
    key_a = ctx_a._full_cache_key()  # noqa: SLF001
    key_b = ctx_b._full_cache_key()  # noqa: SLF001
    assert key_a != key_b
    # The key must be an opaque hex digest, not a stringified context, so the
    # raw credential value is not retained in memory as a dict-key string.
    assert "pwd-A" not in key_a
    assert "pwd-B" not in key_b
    assert len(key_a) == 64  # sha256 hex digest
    assert key_a == ctx_a._full_cache_key()  # noqa: SLF001  # stable


@mock.patch("snowflake.connector.connect", side_effect=RuntimeError("boom"))
@mock.patch("snowflake.cli._app.snow_connector.command_info")
def test_connection_cache_failure_log_does_not_leak_credentials(
    mock_command_info,
    mock_connect,
    local_connection_cache,
    caplog,
):
    """
    When build_connection() fails, the cache logs a debug message. That message
    used to embed the full ConnectionContext repr (with raw password/token).
    It must now log only the redacted repr.
    """
    mock_command_info.return_value = "application"
    ctx = ConnectionContext(
        temporary_connection=True,
        account="acct",
        user="user",
        password=_SECRET_SENTINEL,
        token=_SECRET_SENTINEL,
    )

    import logging

    with caplog.at_level(logging.DEBUG, logger="snowflake.cli.api.connections"):
        with pytest.raises(RuntimeError):
            local_connection_cache[ctx]

    rendered_logs = "\n".join(record.getMessage() for record in caplog.records)
    assert _SECRET_SENTINEL not in rendered_logs
    # We should still see a "failed to connect" breadcrumb so the debug log
    # retains diagnostic value.
    assert "failed to connect" in rendered_logs
