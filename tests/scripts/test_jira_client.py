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

"""Tests for jira_client.py — the shared JIRA REST client used by the daily
automated-ticket workflows. All tests are offline: no real HTTP calls are made.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest
import requests

_SCRIPTS_DIR = Path(__file__).parents[2] / ".github" / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

from jira_client import (  # noqa: E402
    JiraClient,
    _sanitize_exception,
    _validate_jql_token,
)


@pytest.mark.parametrize(
    "value",
    ["python-3-12-5", "SNOW-123456", "abc", "a.b_c-1"],
)
def test_validate_jql_token_accepts_safe_values(value):
    assert _validate_jql_token(value, "field") == value


@pytest.mark.parametrize(
    "value",
    ["' OR 1=1", "foo bar", "foo)OR(1=1", "label' --", ""],
)
def test_validate_jql_token_rejects_unsafe_values(value):
    with pytest.raises(ValueError, match="Unsafe value for JQL"):
        _validate_jql_token(value, "field")


def test_search_existing_ticket_rejects_unsafe_label_before_any_request():
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    with mock.patch.object(jira.session, "get") as mock_get:
        with pytest.raises(ValueError):
            jira.search_existing_ticket("' OR 1=1", "SNOW-1")
    mock_get.assert_not_called()


def test_search_existing_ticket_returns_key_when_found():
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    response = mock.Mock(status_code=200)
    response.json.return_value = {"issues": [{"key": "SNOW-999"}]}
    with mock.patch.object(jira.session, "get", return_value=response) as mock_get:
        result = jira.search_existing_ticket("python-3-12-5", "SNOW-1")

    assert result == "SNOW-999"
    call_kwargs = mock_get.call_args.kwargs
    assert "labels = python-3-12-5" in call_kwargs["params"]["jql"]
    assert "parent = SNOW-1" in call_kwargs["params"]["jql"]


def test_search_existing_ticket_returns_none_when_not_found():
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    response = mock.Mock(status_code=200)
    response.json.return_value = {"issues": []}
    with mock.patch.object(jira.session, "get", return_value=response):
        assert jira.search_existing_ticket("python-3-12-5", "SNOW-1") is None


def test_search_existing_ticket_returns_none_on_request_exception():
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    with mock.patch.object(jira.session, "get", side_effect=Exception("boom")):
        assert jira.search_existing_ticket("python-3-12-5", "SNOW-1") is None


def test_search_existing_ticket_does_not_leak_response_body_on_failure(capsys):
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    response = mock.Mock(
        status_code=401, text="Unauthorized: Basic realm token=super-secret-token"
    )
    with mock.patch.object(jira.session, "get", return_value=response):
        assert jira.search_existing_ticket("python-3-12-5", "SNOW-1") is None

    err = capsys.readouterr().err
    assert "super-secret-token" not in err
    assert "401" in err


def test_create_issue_returns_key_on_success():
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    response = mock.Mock(status_code=201)
    response.json.return_value = {"key": "SNOW-42"}
    with mock.patch.object(jira.session, "post", return_value=response):
        assert jira.create_issue({"fields": {}}) == "SNOW-42"


def test_create_issue_returns_none_on_failure():
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    response = mock.Mock(status_code=400, text="bad request")
    response.json.side_effect = ValueError("no json")
    with mock.patch.object(jira.session, "post", return_value=response):
        assert jira.create_issue({"fields": {}}) is None


def test_create_issue_does_not_leak_response_body_on_failure(capsys):
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    response = mock.Mock(
        status_code=400, text='{"errorMessages": ["token=super-secret-token"]}'
    )
    response.json.return_value = {"errorMessages": ["token=super-secret-token"]}
    with mock.patch.object(jira.session, "post", return_value=response):
        jira.create_issue({"fields": {}})

    out = capsys.readouterr().out
    assert "super-secret-token" not in out
    assert "400" in out


def test_from_env_exits_when_missing_variables(monkeypatch):
    monkeypatch.delenv("JIRA_BASE_URL", raising=False)
    monkeypatch.delenv("JIRA_USER_EMAIL", raising=False)
    monkeypatch.delenv("JIRA_API_TOKEN", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        JiraClient.from_env()
    assert exc_info.value.code == 1


def test_from_env_builds_client(monkeypatch):
    monkeypatch.setenv("JIRA_BASE_URL", "https://example.atlassian.net/")
    monkeypatch.setenv("JIRA_USER_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "token")

    jira = JiraClient.from_env()

    assert jira.base_url == "https://example.atlassian.net"
    assert jira.auth == ("user@example.com", "token")


@pytest.mark.parametrize(
    "base_url",
    [
        "http://example.atlassian.net",
        "ftp://example.atlassian.net",
        "example.atlassian.net",
    ],
)
def test_rejects_non_https_base_url(base_url):
    with pytest.raises(ValueError, match="must use https://"):
        JiraClient(base_url, "user@example.com", "token")


def test_from_env_exits_on_non_https_base_url(monkeypatch, capsys):
    monkeypatch.setenv("JIRA_BASE_URL", "http://example.atlassian.net")
    monkeypatch.setenv("JIRA_USER_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "super-secret-token")

    with pytest.raises(SystemExit) as exc_info:
        JiraClient.from_env()

    assert exc_info.value.code == 1
    assert "super-secret-token" not in capsys.readouterr().out


def test_sanitize_exception_omits_message_contents():
    e = requests.exceptions.ConnectionError(
        "HTTPSConnectionPool(host='example.atlassian.net'): "
        "Failed with Authorization: Basic dXNlcjpzdXBlci1zZWNyZXQtdG9rZW4="
    )
    result = _sanitize_exception(e)
    assert result == "ConnectionError"
    assert "dXNlcjpzdXBlci1zZWNyZXQtdG9rZW4" not in result


def test_search_existing_ticket_does_not_leak_exception_message(capsys):
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    with mock.patch.object(
        jira.session,
        "get",
        side_effect=requests.exceptions.ConnectionError("token=super-secret-token"),
    ):
        assert jira.search_existing_ticket("python-3-12-5", "SNOW-1") is None

    err = capsys.readouterr().err
    assert "super-secret-token" not in err
    assert "ConnectionError" in err


def test_transition_to_todo_does_not_leak_response_body_on_failure(capsys):
    jira = JiraClient("https://example.atlassian.net", "user@example.com", "token")
    response = mock.Mock(
        status_code=403, text="Forbidden: api_token=super-secret-token"
    )
    with mock.patch.object(jira.session, "get", return_value=response):
        assert jira.transition_to_todo("SNOW-1") is False

    err = capsys.readouterr().err
    assert "super-secret-token" not in err
    assert "403" in err
