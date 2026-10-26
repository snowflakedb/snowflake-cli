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

from __future__ import annotations

import json
from unittest import mock

import pytest
from snowflake.cli._plugins.feedback.constants import (
    ISSUES_URL,
    MAX_MESSAGE_BYTES,
    PRIVACY_NOTICE,
    RATE_LIMIT_MAX,
)
from snowflake.cli.api.exceptions import MissingConfigurationError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.connector.errors import DatabaseError

from tests_common.feature_flag_utils import with_feature_flags

_AGENT_ENV_VARS = (
    "CORTEX_SESSION_ID",
    "CURSOR_AGENT",
    "CLAUDECODE",
    "GEMINI_CLI",
    "OPENCODE",
    "CODEX_API_KEY",
)

_SUCCESS_JSON_KEYS = {
    "status",
    "privacy_notice",
    "feedback_id",
    "category",
    "rating",
    "truncated",
}


@pytest.fixture(autouse=True)
def _clear_agent_environment(monkeypatch):
    for name in _AGENT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _parse_json(output: str) -> dict:
    return json.loads(output)


def _telemetry(mock_connect):
    return mock_connect.return_value._telemetry  # noqa: SLF001


def _feedback_events(mock_connect) -> list[dict]:
    calls = _telemetry(mock_connect).try_add_log_to_batch.call_args_list
    events = [call.args[0].to_dict() for call in calls]
    return [
        event
        for event in events
        if event.get("message", {}).get("type") == "cli_user_feedback"
    ]


def _json_feedback(*extra: str) -> list[str]:
    return ["feedback", "--rating", "2", "--yes", "--format", "JSON", *extra]


def test_feedback_hidden_from_root_help_when_flag_off(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert "| feedback " not in result.output


def test_feedback_listed_in_root_help_when_flag_on(runner):
    with with_feature_flags({FeatureFlag.ENABLE_SNOW_FEEDBACK: True}):
        result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert "| feedback " in result.output


def test_feedback_help_lists_required_rating(runner):
    result = runner.invoke(["feedback", "--help"])
    assert result.exit_code == 0, result.output
    assert "--rating" in result.output
    assert "--message" in result.output
    assert "--category" in result.output
    assert "--yes" in result.output
    assert "--last-error" in result.output
    assert "error text is not sent" in result.output
    assert "include passwords" in result.output
    assert "AI agent" in result.output
    assert "preview and may be removed" in result.output


def test_rating_is_required(runner, mock_connect):
    result = runner.invoke(
        ["feedback", "--message", "note", "--yes", "--format", "JSON"]
    )
    assert result.exit_code != 0
    assert _feedback_events(mock_connect) == []


def test_rating_out_of_range_is_rejected(runner, mock_connect):
    result = runner.invoke(
        ["feedback", "--rating", "5", "--message", "note", "--yes", "--format", "JSON"]
    )
    assert result.exit_code != 0
    assert _feedback_events(mock_connect) == []


def test_non_tty_table_without_yes_requires_confirmation(runner, mock_connect):
    result = runner.invoke(["feedback", "--rating", "2", "--message", "x"])
    assert result.exit_code == 2, result.output
    assert "Pass --yes" in result.output
    assert PRIVACY_NOTICE in result.output
    assert _feedback_events(mock_connect) == []


def test_non_tty_without_yes_requires_confirmation(runner, mock_connect):
    result = runner.invoke(
        [
            "feedback",
            "--rating",
            "2",
            "--message",
            "sql error is confusing",
            "--format",
            "JSON",
        ]
    )
    assert result.exit_code == 2, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "confirmation_required"
    assert payload["privacy_notice"] == PRIVACY_NOTICE
    assert set(payload) == {"status", "privacy_notice"}
    assert _feedback_events(mock_connect) == []


def test_whitespace_message_is_rejected(runner, mock_connect):
    result = runner.invoke(_json_feedback("--message", "   \n"))
    assert result.exit_code == 1, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "rejected"
    assert payload["reason"] == "Message is empty."
    assert payload["privacy_notice"] == PRIVACY_NOTICE
    assert _feedback_events(mock_connect) == []


def test_human_can_submit_rating_without_message(runner, mock_connect):
    result = runner.invoke(_json_feedback())
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "submitted"
    assert payload["rating"] == 2
    assert payload["category"] == "other"
    assert payload["truncated"] is False
    assert "message" not in payload
    assert set(payload) == _SUCCESS_JSON_KEYS

    events = _feedback_events(mock_connect)
    assert len(events) == 1
    message = events[0]["message"]
    assert message["type"] == "cli_user_feedback"
    assert message["rating"] == 2
    assert "message" not in message


@pytest.mark.parametrize(
    "env_name,env_value",
    [
        ("CURSOR_AGENT", "1"),
        ("CLAUDECODE", "1"),
        ("GEMINI_CLI", "1"),
        ("OPENCODE", "1"),
        ("CORTEX_SESSION_ID", "sess"),
        ("CORTEX_SESSION_ID", ""),
        ("CODEX_API_KEY", "x"),
        ("CODEX_API_KEY", ""),
    ],
)
def test_agent_without_message_is_rejected(
    runner, mock_connect, monkeypatch, env_name, env_value
):
    monkeypatch.setenv(env_name, env_value)
    result = runner.invoke(_json_feedback())
    assert result.exit_code == 1, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "rejected"
    assert payload["reason"] == "Message is required when running as an AI agent."
    assert _feedback_events(mock_connect) == []


def test_agent_with_message_sends(runner, mock_connect, monkeypatch):
    monkeypatch.setenv("CURSOR_AGENT", "1")
    result = runner.invoke(_json_feedback("--message", "sql error is confusing"))
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "submitted"
    assert payload["rating"] == 2
    events = _feedback_events(mock_connect)
    assert len(events) == 1
    assert events[0]["message"]["message"] == "sql error is confusing"


def test_yes_sends_redacted_event(runner, mock_connect):
    token = "gho_abcdefghijklmnopqrstuvwxyz012345"
    result = runner.invoke(
        _json_feedback(
            "--message",
            f"stage copy failed with {token}",
            "--category",
            "bug",
        )
    )
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "submitted"
    assert payload["category"] == "bug"
    assert payload["rating"] == 2
    assert payload["truncated"] is False
    assert payload["privacy_notice"] == PRIVACY_NOTICE
    assert "feedback_id" in payload
    assert set(payload) == _SUCCESS_JSON_KEYS

    events = _feedback_events(mock_connect)
    assert len(events) == 1
    message = events[0]["message"]
    assert message["type"] == "cli_user_feedback"
    assert message["category"] == "bug"
    assert message["rating"] == 2
    assert message["truncated"] is False
    assert message["feedback_id"] == payload["feedback_id"]
    assert token not in json.dumps(events[0])
    assert token not in result.output


def test_rate_limit_sixth_call(runner, mock_connect):
    for i in range(RATE_LIMIT_MAX):
        result = runner.invoke(_json_feedback("--message", f"note {i}"))
        assert result.exit_code == 0, result.output
        assert _parse_json(result.output)["status"] == "submitted"

    blocked = runner.invoke(_json_feedback("--message", "one too many"))
    assert blocked.exit_code == 1, blocked.output
    payload = _parse_json(blocked.output)
    assert payload["status"] == "rate_limited"
    assert payload["retry_after_seconds"] >= 1
    assert payload["privacy_notice"] == PRIVACY_NOTICE
    assert len(_feedback_events(mock_connect)) == RATE_LIMIT_MAX


def test_auth_or_network_failure_shows_connection_error(runner, mock_connect):
    mock_connect.side_effect = DatabaseError("could not connect")
    result = runner.invoke(_json_feedback("--message", "docs are wrong"))
    assert result.exit_code == 1, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "send_failed"
    assert payload["privacy_notice"] == PRIVACY_NOTICE
    assert "issue_url" not in payload
    assert payload["reason"].startswith("Could not connect to Snowflake. Reason:")
    assert "could not connect" in payload["reason"]
    assert ISSUES_URL not in result.output


def test_unconfigured_connection_points_at_public_issues(runner, mock_connect):
    with mock.patch(
        "snowflake.cli._app.snow_connector.get_connection_dict",
        side_effect=MissingConfigurationError("Connection default is not configured"),
    ):
        result = runner.invoke(_json_feedback("--message", "docs are wrong"))
    assert result.exit_code == 1, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "no_connection"
    assert payload["issue_url"] == ISSUES_URL
    assert payload["privacy_notice"] == PRIVACY_NOTICE
    assert ISSUES_URL.startswith("https://github.com/snowflakedb/snowflake-cli/")


def test_send_failed(runner, mock_connect):
    channel = _telemetry(mock_connect)

    def _disable(*_args, **_kwargs):
        channel.is_enabled.return_value = False

    channel.send_batch.side_effect = _disable
    for _ in range(RATE_LIMIT_MAX + 1):
        channel.is_enabled.return_value = True
        result = runner.invoke(_json_feedback("--message", "ux is unclear"))
        assert result.exit_code == 1, result.output
        payload = _parse_json(result.output)
        assert payload["status"] == "send_failed"
        assert payload["privacy_notice"] == PRIVACY_NOTICE


def test_tty_declines_does_not_send(runner, mock_connect, monkeypatch):
    monkeypatch.setattr(
        "snowflake.cli._plugins.feedback.commands.is_tty_interactive",
        lambda: True,
    )
    with mock.patch(
        "snowflake.cli._plugins.feedback.commands.typer.confirm",
        return_value=False,
    ):
        result = runner.invoke(
            ["feedback", "--rating", "2", "--message", "never send this"]
        )
    assert result.exit_code != 0
    assert PRIVACY_NOTICE in result.output
    assert _feedback_events(mock_connect) == []


def test_tty_confirm_sends(runner, mock_connect, monkeypatch):
    monkeypatch.setattr(
        "snowflake.cli._plugins.feedback.commands.is_tty_interactive",
        lambda: True,
    )
    with mock.patch(
        "snowflake.cli._plugins.feedback.commands.typer.confirm",
        return_value=True,
    ):
        result = runner.invoke(["feedback", "--rating", "2", "--message", "ok"])
    assert result.exit_code == 0, result.output
    assert PRIVACY_NOTICE in result.output
    assert len(_feedback_events(mock_connect)) == 1


def test_tty_json_without_yes_requires_confirmation(runner, mock_connect, monkeypatch):
    monkeypatch.setattr(
        "snowflake.cli._plugins.feedback.commands.is_tty_interactive",
        lambda: True,
    )
    result = runner.invoke(["feedback", "--rating", "2", "--format", "JSON"])
    assert result.exit_code == 2, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "confirmation_required"
    assert _feedback_events(mock_connect) == []


def test_control_only_message_is_rejected(runner, mock_connect):
    result = runner.invoke(_json_feedback("--message", "\x01\x02\x03"))
    assert result.exit_code == 1, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "rejected"
    assert payload["reason"] == "Message is empty."
    assert _feedback_events(mock_connect) == []


def test_json_object_keys_are_stable_on_success(runner, mock_connect):
    result = runner.invoke(_json_feedback("--message", "stable keys"))
    assert result.exit_code == 0, result.output
    assert set(_parse_json(result.output)) == _SUCCESS_JSON_KEYS


@pytest.mark.parametrize(
    "status_command,expected_status,expected_keys",
    [
        (
            ["feedback", "--rating", "2", "--message", "x", "--format", "JSON"],
            "confirmation_required",
            {"status", "privacy_notice"},
        ),
        (
            [
                "feedback",
                "--rating",
                "2",
                "--message",
                "   ",
                "--yes",
                "--format",
                "JSON",
            ],
            "rejected",
            {"status", "privacy_notice", "reason"},
        ),
    ],
)
def test_json_object_keys_on_refusals(
    runner, mock_connect, status_command, expected_status, expected_keys
):
    result = runner.invoke(status_command)
    payload = _parse_json(result.output)
    assert payload["status"] == expected_status
    assert set(payload) == expected_keys


def test_long_message_is_truncated_in_event(runner, mock_connect):
    payload_text = "a" * (MAX_MESSAGE_BYTES + 50)
    result = runner.invoke(_json_feedback("--message", payload_text))
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["truncated"] is True
    events = _feedback_events(mock_connect)
    assert len(events) == 1
    sent = events[0]["message"]["message"]
    assert events[0]["message"]["truncated"] is True
    assert len(sent.encode("utf-8")) <= MAX_MESSAGE_BYTES
    assert payload_text not in sent


def test_failed_connection_does_not_consume_rate_limit(runner, mock_connect):
    mock_connect.side_effect = DatabaseError("could not connect")
    for _ in range(RATE_LIMIT_MAX + 1):
        result = runner.invoke(_json_feedback("--message", "docs are wrong"))
        assert result.exit_code == 1, result.output
        assert _parse_json(result.output)["status"] == "send_failed"


def test_silent_without_yes_requires_confirmation(runner, mock_connect, monkeypatch):
    monkeypatch.setattr(
        "snowflake.cli._plugins.feedback.commands.is_tty_interactive",
        lambda: True,
    )
    result = runner.invoke(
        ["feedback", "--silent", "--rating", "2", "--message", "never send"]
    )
    assert result.exit_code == 2, result.output
    assert "Pass --yes" in result.output
    assert _feedback_events(mock_connect) == []


def test_csv_without_yes_requires_confirmation(runner, mock_connect):
    result = runner.invoke(
        [
            "feedback",
            "--rating",
            "2",
            "--message",
            "sql error is confusing",
            "--format",
            "CSV",
        ]
    )
    assert result.exit_code == 2, result.output
    assert "confirmation_required" in result.output
    assert _feedback_events(mock_connect) == []


def test_csv_with_yes_submits(runner, mock_connect):
    result = runner.invoke(
        [
            "feedback",
            "--rating",
            "2",
            "--message",
            "sql error is confusing",
            "--yes",
            "--format",
            "CSV",
        ]
    )
    assert result.exit_code == 0, result.output
    assert "submitted" in result.output
    assert len(_feedback_events(mock_connect)) == 1


def test_disabled_telemetry_is_send_failed(runner, mock_connect):
    channel = _telemetry(mock_connect)
    channel.is_enabled.return_value = False
    result = runner.invoke(_json_feedback("--message", "ux is unclear"))
    assert result.exit_code == 1, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "send_failed"
    assert payload["privacy_notice"] == PRIVACY_NOTICE


def test_missing_telemetry_channel_is_no_connection(runner, mock_connect):
    mock_connect.return_value._telemetry = None  # noqa: SLF001
    result = runner.invoke(_json_feedback("--message", "docs are wrong"))
    assert result.exit_code == 1, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "no_connection"
    assert payload["issue_url"] == ISSUES_URL


def test_table_output_rejected_includes_notice(runner, mock_connect):
    result = runner.invoke(["feedback", "--rating", "2", "--message", "   ", "--yes"])
    assert result.exit_code == 1, result.output
    assert PRIVACY_NOTICE in result.output
    assert "Message is empty." in result.output
    assert _feedback_events(mock_connect) == []


def test_table_output_connection_error_omits_issues_url(runner, mock_connect):
    mock_connect.side_effect = DatabaseError("could not connect")
    result = runner.invoke(
        ["feedback", "--rating", "2", "--message", "docs are wrong", "--yes"]
    )
    assert result.exit_code == 1, result.output
    assert PRIVACY_NOTICE in result.output
    assert "Could not connect to Snowflake. Reason:" in result.output
    assert ISSUES_URL not in result.output


def test_table_output_unconfigured_connection_includes_github_url(runner, mock_connect):
    with mock.patch(
        "snowflake.cli._app.snow_connector.get_connection_dict",
        side_effect=MissingConfigurationError("Connection default is not configured"),
    ):
        result = runner.invoke(
            ["feedback", "--rating", "2", "--message", "docs are wrong", "--yes"]
        )
    assert result.exit_code == 1, result.output
    assert PRIVACY_NOTICE in result.output
    assert ISSUES_URL in result.output


def _write_last_error(test_snowcli_config, **extra):
    payload = {
        "id": "err-1",
        "error_type": "CliSqlError",
        "command": "sql",
        "timestamp": "2026-10-22T00:00:00Z",
    }
    payload.update(extra)
    path = test_snowcli_config.parent / "last_cli_error.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    return path


def test_last_error_flag_without_file_still_sends_the_note(runner, mock_connect):
    result = runner.invoke(
        _json_feedback("--message", "sql failed when mixing flags", "--last-error")
    )
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == "submitted"
    events = _feedback_events(mock_connect)
    assert len(events) == 1
    message = events[0]["message"]
    assert "sql failed" in message["message"]
    assert "last_error_id" not in message
    assert "last_error_type" not in message


def test_last_error_flag_sends_id_and_type_only(
    runner, mock_connect, test_snowcli_config
):
    secret = "invalid identifier SELCT"
    path = _write_last_error(test_snowcli_config, message=secret)
    result = runner.invoke(_json_feedback("--message", "first", "--last-error"))
    assert result.exit_code == 0, result.output
    user_payload = _parse_json(result.output)
    assert "last_error_id" not in user_payload
    assert secret not in result.output
    assert not path.exists()

    events = _feedback_events(mock_connect)
    assert len(events) == 1
    message = events[0]["message"]
    assert message["last_error_id"] == "err-1"
    assert message["last_error_type"] == "CliSqlError"
    assert "first" in message["message"]
    dumped = json.dumps(events[0])
    assert secret not in dumped
    assert '"command": "sql"' not in dumped

    again = runner.invoke(_json_feedback("--message", "second", "--last-error"))
    assert again.exit_code == 0, again.output
    events = _feedback_events(mock_connect)
    assert len(events) == 2
    assert "last_error_id" not in events[1]["message"]
    assert "second" in events[1]["message"]["message"]


def test_last_error_send_failed_keeps_sidecar(
    runner, mock_connect, test_snowcli_config
):
    path = _write_last_error(test_snowcli_config)
    channel = _telemetry(mock_connect)
    channel.is_enabled.return_value = False
    result = runner.invoke(_json_feedback("--message", "retry me", "--last-error"))
    assert result.exit_code == 1, result.output
    assert _parse_json(result.output)["status"] == "send_failed"
    assert path.is_file()

    channel.is_enabled.return_value = True
    result = runner.invoke(_json_feedback("--message", "retry me", "--last-error"))
    assert result.exit_code == 0, result.output
    assert not path.exists()
    events = _feedback_events(mock_connect)
    assert events[-1]["message"]["last_error_id"] == "err-1"


def test_without_last_error_flag_does_not_attach_sidecar(
    runner, mock_connect, test_snowcli_config
):
    secret = "invalid identifier SELCT"
    _write_last_error(test_snowcli_config, message=secret)
    result = runner.invoke(_json_feedback())
    assert result.exit_code == 0, result.output
    events = _feedback_events(mock_connect)
    assert len(events) == 1
    message = events[0]["message"]
    assert "last_error_id" not in message
    assert "last_error_type" not in message
    assert secret not in json.dumps(events[0])


def test_rating_error_does_not_advertise_feedback(runner):
    result = runner.invoke(["feedback", "--rating", "9"])
    assert result.exit_code != 0
    assert "To send feedback" not in result.output


def test_table_output_includes_submitted_line(runner, mock_connect, monkeypatch):
    monkeypatch.setattr(
        "snowflake.cli._plugins.feedback.commands.is_tty_interactive",
        lambda: True,
    )
    with mock.patch(
        "snowflake.cli._plugins.feedback.commands.typer.confirm",
        return_value=True,
    ):
        result = runner.invoke(["feedback", "--rating", "3", "--message", "ok"])
    assert result.exit_code == 0, result.output
    assert PRIVACY_NOTICE in result.output
    assert "Feedback submitted (" in result.output
