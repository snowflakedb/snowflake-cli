"""Run the helper through connector authentication, faking only HTTP and storage."""

import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock

import pytest
import snowflake.connector
from snowflake.cli._plugins.ai import oauth
from snowflake.connector.errors import DatabaseError
from snowflake.connector.network import ReauthenticationRequest
from snowflake.connector.token_cache import TokenType

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX OAuth helper")

BINDING = {
    "account": "test",
    "host": "test.snowflakecomputing.com",
    "user": "TEST",
    "role": "ANALYST",
}


@pytest.fixture
def authentication_flow(monkeypatch, tmp_path):
    if not oauth.supports_connector(snowflake.connector.__version__):
        pytest.fail(
            f"Installed connector {snowflake.connector.__version__} is older than "
            f"the minimum helper gate {oauth.SUPPORTED_CONNECTOR}."
        )
    monkeypatch.setattr(oauth.Path, "home", lambda: tmp_path)
    state = SimpleNamespace(
        tokens={
            TokenType.OAUTH_ACCESS_TOKEN: "synthetic-access",
            TokenType.OAUTH_REFRESH_TOKEN: "synthetic-refresh",
        },
        login_errors=[],
        logins=[],
        refreshes=[],
        refresh_response={
            "access_token": "synthetic-renewed",
            "refresh_token": "synthetic-rotated",
        },
        role="ANALYST",
        refresh_started=None,
        release_refresh=None,
        cache_opens=0,
        second_cache_opened=threading.Event(),
    )
    cache = mock.Mock()
    cache.retrieve.side_effect = lambda key: state.tokens.get(key.tokenType)
    cache.store.side_effect = lambda key, value: state.tokens.update(
        {key.tokenType: value}
    )
    cache.remove.side_effect = lambda key: state.tokens.pop(key.tokenType, None)

    def open_cache():
        state.cache_opens += 1
        if state.cache_opens == 2:
            state.second_cache_opened.set()
        return cache

    def request(rest, url, headers, body=None, **kwargs):
        if "login-request" in url:
            state.logins.append(json.loads(body)["data"]["TOKEN"])
            if state.login_errors:
                return {
                    "success": False,
                    "code": state.login_errors.pop(0),
                    "message": "synthetic login rejection",
                    "data": {},
                }
            return {
                "success": True,
                "data": {
                    "token": "synthetic-session",
                    "masterToken": "synthetic-master",
                    "sessionId": 1,
                    "parameters": [{"name": "AUTOCOMMIT", "value": True}],
                    "sessionInfo": {"roleName": state.role, "userName": "TEST"},
                },
            }
        return {"success": True, "data": {}}

    def token_request(pool, method, url, **kwargs):
        assert method == "POST"
        assert url == "https://test.snowflakecomputing.com/oauth/token-request"
        assert kwargs["fields"]["grant_type"] == "refresh_token"
        state.refreshes.append(kwargs["fields"]["refresh_token"])
        if state.refresh_started is not None:
            state.refresh_started.set()
            assert state.release_refresh.wait(timeout=5)
        return SimpleNamespace(
            status=200, data=json.dumps(state.refresh_response).encode()
        )

    with mock.patch(
        "snowflake.connector.token_cache.TokenCache.make", side_effect=open_cache
    ), mock.patch(
        "snowflake.connector.network.SnowflakeRestful._post_request", new=request
    ), mock.patch(
        "snowflake.connector.auth._oauth_base.urllib3.PoolManager.request_encode_body",
        new=token_request,
    ), mock.patch(
        "webbrowser.open", side_effect=AssertionError("Browser must not open")
    ):
        yield state


@pytest.mark.parametrize("login_code", [None, "390318", "390303"])
def test_cached_access_token_uses_real_login_and_refresh(
    authentication_flow, login_code
):
    state = authentication_flow
    if login_code:
        state.login_errors = [login_code]
    token = oauth.obtain_token(BINDING)
    assert token.value == ("synthetic-renewed" if login_code else "synthetic-access")
    assert state.logins == (
        ["synthetic-access", "synthetic-renewed"]
        if login_code
        else ["synthetic-access"]
    )
    assert state.refreshes == (["synthetic-refresh"] if login_code else [])
    if login_code:
        assert state.tokens[TokenType.OAUTH_REFRESH_TOKEN] == "synthetic-rotated"


def test_refresh_only_cache_renews_without_browser(authentication_flow):
    state = authentication_flow
    state.tokens.pop(TokenType.OAUTH_ACCESS_TOKEN)
    assert oauth.obtain_token(BINDING).value == "synthetic-renewed"
    assert state.logins == ["synthetic-renewed"]
    assert state.refreshes == ["synthetic-refresh"]


@pytest.mark.parametrize("login_code", ["390318", "390303", "390100"])
def test_rejected_renewed_token_does_not_loop(authentication_flow, login_code):
    state = authentication_flow
    state.login_errors = [login_code, login_code]
    with pytest.raises((DatabaseError, ReauthenticationRequest)):
        oauth.obtain_token(BINDING)
    assert len(state.logins) == (1 if login_code == "390100" else 2)
    assert len(state.refreshes) == (0 if login_code == "390100" else 1)


@pytest.mark.parametrize("access_present", [False, True])
def test_no_refresh_token_fails_without_browser(authentication_flow, access_present):
    state = authentication_flow
    state.tokens.pop(TokenType.OAUTH_REFRESH_TOKEN)
    if access_present:
        state.login_errors = ["390318"]
    else:
        state.tokens.pop(TokenType.OAUTH_ACCESS_TOKEN)
    with pytest.raises(oauth.OAuthHelperError) as error:
        oauth.obtain_token(BINDING)
    assert error.value.category == 1
    assert not state.refreshes


def test_failed_refresh_does_not_return_stale_token(authentication_flow):
    state = authentication_flow
    state.login_errors = ["390318"]
    state.refresh_response = {"error": "invalid_grant"}
    with pytest.raises(oauth.OAuthHelperError):
        oauth.obtain_token(BINDING)
    assert state.logins == ["synthetic-access"]
    assert len(state.refreshes) == 1


def test_authenticated_identity_is_verified(authentication_flow):
    authentication_flow.role = "OTHER_ROLE"
    with pytest.raises(oauth.OAuthHelperError) as error:
        oauth.obtain_token(BINDING)
    assert error.value.category == 4


def test_overlapping_helpers_read_rotated_cache_after_lock(authentication_flow):
    state = authentication_flow
    state.tokens.pop(TokenType.OAUTH_ACCESS_TOKEN)
    state.refresh_started = threading.Event()
    state.release_refresh = threading.Event()
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(oauth.obtain_token, BINDING)
        try:
            assert state.refresh_started.wait(timeout=5)
            second = executor.submit(oauth.obtain_token, BINDING)
            assert state.second_cache_opened.wait(timeout=5)
        finally:
            state.release_refresh.set()
        assert first.result(timeout=10).value == "synthetic-renewed"
        assert second.result(timeout=10).value == "synthetic-renewed"
    assert state.refreshes == ["synthetic-refresh"]
    assert state.logins == ["synthetic-renewed", "synthetic-renewed"]
