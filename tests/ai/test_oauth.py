import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
import tomlkit
from packaging.requirements import Requirement
from snowflake.cli._plugins.ai import oauth
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secret import SecretType
from snowflake.cli.api.secure_path import SecurePath

BINDING = {
    "account": "test",
    "host": "test.snowflakecomputing.com",
    "user": "TEST",
    "role": "ANALYST",
}

POSIX_ONLY = pytest.mark.skipif(sys.platform == "win32", reason="POSIX OAuth helper")


def test_connector_gate_matches_shipping_dependency():
    project = tomlkit.parse(
        SecurePath(Path(__file__).resolve().parents[2] / "pyproject.toml").read_text(
            file_size_limit_mb=1
        )
    )
    dependencies = [Requirement(value) for value in project["project"]["dependencies"]]
    connector = [
        dep for dep in dependencies if dep.name == "snowflake-connector-python"
    ]
    assert len(connector) == 1
    assert str(connector[0].specifier) == f"=={oauth.SUPPORTED_CONNECTOR}"


@pytest.mark.parametrize(
    "host",
    [
        "https://evil.test",
        "test.snowflakecomputing.com/",
        "test.snowflakecomputing.com.evil.test",
        "test@evil.test",
    ],
)
def test_binding_refuses_unexpected_host(host):
    with pytest.raises(CliError):
        oauth.validate_binding({**BINDING, "host": host})


@POSIX_ONLY
def test_helper_stdout_contains_only_access_token(monkeypatch, capsys):
    monkeypatch.setenv(oauth.BINDING_ENV, json.dumps(BINDING))
    with mock.patch.object(
        oauth, "obtain_token", return_value=SecretType("test-access-token")
    ):
        assert oauth.main() == 0
    output = capsys.readouterr()
    assert output.out == "test-access-token"
    assert output.err == ""


@POSIX_ONLY
@pytest.mark.parametrize(
    "error,category",
    [
        (RuntimeError("secret-refresh-token"), 6),
        (TimeoutError(), 5),
        (PermissionError("secret-refresh-token"), 3),
        (ValueError("secret-refresh-token"), 2),
        (oauth.OAuthHelperError(1), 1),
        (oauth.OAuthHelperError(4), 4),
    ],
)
def test_helper_errors_do_not_leak(monkeypatch, capsys, error, category):
    monkeypatch.setenv(oauth.BINDING_ENV, json.dumps(BINDING))
    with mock.patch.object(oauth, "obtain_token", side_effect=error):
        assert oauth.main() == category
    output = capsys.readouterr()
    assert output.out == ""
    assert "secret-refresh-token" not in output.err
    assert output.err == oauth.ERROR_MESSAGES[category] + "\n"


@POSIX_ONLY
def test_connector_auth_instance_blocks_browser_and_captures_access_token(
    monkeypatch, tmp_path
):
    import snowflake.connector

    monkeypatch.setattr(snowflake.connector, "__version__", oauth.SUPPORTED_CONNECTOR)
    monkeypatch.setattr(oauth.Path, "home", lambda: tmp_path)
    connection = SimpleNamespace(**BINDING)

    def connect(**kwargs):
        auth = kwargs["auth_class"]
        with pytest.raises(CliError, match="Sign in again"):
            getattr(auth, "_request_tokens")()
        getattr(auth, "_store_tokens")(access_token="accepted-access-token")
        body = {"data": {}}
        auth.update_body(body)
        auth.reset_secrets()
        manager = mock.MagicMock()
        manager.__enter__.return_value = connection
        return manager

    with mock.patch.object(snowflake.connector, "connect", side_effect=connect):
        with mock.patch(
            "snowflake.connector.token_cache.TokenCache.make", return_value=None
        ):
            assert oauth.obtain_token(BINDING).value == "accepted-access-token"


@POSIX_ONLY
def test_connector_mismatch_fails_before_connect():
    with mock.patch("snowflake.connector.__version__", "4.7.1"):
        with pytest.raises(CliError, match=oauth.CONNECTOR_VERSION_MESSAGE):
            oauth.obtain_token(BINDING)


@POSIX_ONLY
def test_connector_expired_token_refresh_and_rotation(monkeypatch, tmp_path):
    import snowflake.connector

    monkeypatch.setattr(snowflake.connector, "__version__", oauth.SUPPORTED_CONNECTOR)
    monkeypatch.setattr(oauth.Path, "home", lambda: tmp_path)
    cache = mock.Mock()
    cache.retrieve.side_effect = ["expired-access", "refresh-secret"]
    connection = SimpleNamespace(
        **BINDING, service_name=None, _authenticator="oauth_authorization_code"
    )

    def connect(**kwargs):
        auth = kwargs["auth_class"]
        auth.prepare(
            conn=connection,
            authenticator="oauth_authorization_code",
            service_name=None,
            account="test",
            user="TEST",
        )
        with mock.patch.object(
            auth,
            "_get_refresh_token_response",
            return_value=SimpleNamespace(
                data=b'{"access_token":"new-access","refresh_token":"rotated-refresh"}'
            ),
        ):
            assert auth.reauthenticate(conn=connection) == {"success": True}
        body = {"data": {}}
        auth.update_body(body)
        auth.reset_secrets()
        manager = mock.MagicMock()
        manager.__enter__.return_value = connection
        return manager

    with mock.patch.object(
        snowflake.connector, "connect", side_effect=connect
    ), mock.patch(
        "snowflake.connector.token_cache.TokenCache.make", return_value=cache
    ):
        assert oauth.obtain_token(BINDING).value == "new-access"
    stored = [call.args[1] for call in cache.store.call_args_list]
    assert stored == ["new-access", "rotated-refresh"]


@POSIX_ONLY
def test_lock_rejects_symlink_directory(monkeypatch, tmp_path):
    monkeypatch.setattr(oauth.Path, "home", lambda: tmp_path)
    parent = tmp_path / ".snowflake"
    parent.mkdir()
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    (parent / "ai-oauth-locks").symlink_to(target, target_is_directory=True)
    with pytest.raises(CliError, match="0700"):
        with oauth.renewal_lock(BINDING):
            pytest.fail("Unsafe lock accepted")


@POSIX_ONLY
def test_binding_uses_authenticated_identity_not_future_default(monkeypatch):
    import snowflake.connector

    monkeypatch.setattr(snowflake.connector, "__version__", oauth.SUPPORTED_CONNECTOR)
    context = SimpleNamespace(
        temporary_connection=False,
        connection_name="pm",
        client_store_temporary_credential=True,
        oauth_enable_refresh_tokens=True,
        role="ANALYST",
        user="TEST",
    )
    connection = SimpleNamespace(
        **BINDING, auth_class=SimpleNamespace(_idp_host=BINDING["host"])
    )
    assert oauth.binding_for(context, connection) == BINDING
    context.role = "OTHER_ROLE"
    with pytest.raises(CliError, match="differs"):
        oauth.binding_for(context, connection)


@POSIX_ONLY
@pytest.mark.parametrize(
    "field",
    [
        "oauth_client_secret",
        "oauth_scope",
        "oauth_token_request_url",
        "secondary_roles",
    ],
)
def test_binding_rejects_custom_oauth_configuration(monkeypatch, field):
    monkeypatch.setattr("snowflake.connector.__version__", oauth.SUPPORTED_CONNECTOR)
    context = SimpleNamespace(
        temporary_connection=False,
        connection_name="pm",
        client_store_temporary_credential=True,
        oauth_enable_refresh_tokens=True,
        **{field: "unsupported"},
    )
    with pytest.raises(CliError, match="default-client"):
        oauth.binding_for(context, None)


def test_windows_rejects_renewal_before_connector_or_signals(monkeypatch, capsys):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr("snowflake.connector.__version__", oauth.SUPPORTED_CONNECTOR)
    with mock.patch("snowflake.connector.connect") as connect, mock.patch.object(
        oauth.signal, "signal"
    ) as signals:
        assert oauth.main() == 2
        with pytest.raises(CliError, match="Windows"):
            oauth.binding_for(None, None)
        with pytest.raises(oauth.OAuthHelperError):
            oauth.obtain_token(BINDING)
    connect.assert_not_called()
    signals.assert_not_called()
    assert capsys.readouterr().out == ""


@pytest.fixture
def binding_context(monkeypatch):
    monkeypatch.setattr("snowflake.connector.__version__", oauth.SUPPORTED_CONNECTOR)
    monkeypatch.setattr(sys, "platform", "linux")
    return SimpleNamespace(
        connection_name="test",
        temporary_connection=False,
        client_store_temporary_credential=True,
        oauth_enable_refresh_tokens=True,
        user="TEST",
        role="ANALYST",
    )


@pytest.mark.parametrize("field", ["user", "role"])
@pytest.mark.parametrize("value", [None, "", " "])
def test_binding_requires_explicit_identity(binding_context, field, value):
    setattr(binding_context, field, value)
    with pytest.raises(CliError, match="explicit"):
        oauth.binding_for(binding_context, None)


@pytest.mark.parametrize(
    "role,actual",
    [
        ("analyst", "ANALYST"),
        ('"MixedRole"', "MixedRole"),
        ('"Role""Name"', 'Role"Name'),
    ],
)
def test_binding_role_identifier_semantics(binding_context, role, actual):
    binding_context.role = role
    connection = SimpleNamespace(
        **{**BINDING, "role": actual},
        auth_class=SimpleNamespace(_idp_host=BINDING["host"]),
    )
    binding = oauth.binding_for(binding_context, connection)
    assert binding["role"] == (role if role.startswith('"') else role.upper())
    connection.role = actual.swapcase()
    with pytest.raises(CliError, match="differs"):
        oauth.binding_for(binding_context, connection)


@pytest.mark.parametrize(
    "field,value",
    [
        ("port", 443),
        ("protocol", "https"),
        ("proxy_host", "proxy.test"),
        ("proxy_password", "synthetic-secret"),
        ("ocsp_fail_open", False),
        ("oauth_redirect_uri", "http://127.0.0.1"),
        ("enable_diag", True),
    ],
)
def test_binding_rejects_unsupported_settings_without_values(
    binding_context, field, value
):
    setattr(binding_context, field, value)
    with pytest.raises(CliError, match="transport/client") as error:
        oauth.binding_for(binding_context, None)
    assert field in str(error.value)
    assert "synthetic-secret" not in str(error.value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("password", "unused-secret"),
        ("private_key_path", "/unused/key"),
        ("query_tag", "my-work"),
        ("region", "us-west-2"),
        ("application", "custom-client"),
        ("application_name", "custom-client"),
        ("login_timeout", 30),
        ("client_session_keep_alive", True),
        ("session_parameters", {"QUERY_TAG": "my-work"}),
        ("ocsp_mode", "FAIL_OPEN"),
    ],
)
def test_binding_ignores_non_renewal_options(binding_context, field, value):
    setattr(binding_context, field, value)
    connection = SimpleNamespace(
        **BINDING, auth_class=SimpleNamespace(_idp_host=BINDING["host"])
    )
    assert oauth.binding_for(binding_context, connection) == BINDING
