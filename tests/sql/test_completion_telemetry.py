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

from prompt_toolkit.completion import CompleteEvent
from prompt_toolkit.document import Document
from snowflake.cli._plugins.sql.completion.completer import SqlReplCompleter
from snowflake.cli._plugins.sql.completion.telemetry import (
    ALLOWED_PAYLOAD_KEYS,
    build_completion_payload,
    emit_completion,
    on_completion_accepted,
)

_SENTINEL_DB = "TELEMETRY_SENTINEL_DB"
_SENTINEL_SCHEMA = "TELEMETRY_SENTINEL_SCHEMA"
_SENTINEL_TABLE = "TELEMETRY_SENTINEL_TABLE"
_SENTINEL_PREFIX = "TELEMETRY_SENTINEL_PREFIX"
_SENTINEL_SQL = (
    f"SELECT * FROM {_SENTINEL_DB}.{_SENTINEL_SCHEMA}.{_SENTINEL_TABLE} "
    f"WHERE {_SENTINEL_PREFIX}"
)
_SENTINEL_SUGGESTION = "TELEMETRY_SENTINEL_SUGGESTION"


def _complete(buffer: str, payloads: list[dict]) -> None:
    def capture(**kwargs):
        payload = build_completion_payload(**kwargs)
        payloads.append(payload)
        return payload

    with mock.patch(
        "snowflake.cli._plugins.sql.completion.completer.emit_completion",
        side_effect=capture,
    ):
        list(
            SqlReplCompleter().get_completions(
                Document(buffer, cursor_position=len(buffer)),
                CompleteEvent(completion_requested=True),
            )
        )


def test_keyword_tab_emits_requested_shown():
    payloads: list[dict] = []
    _complete("SEL", payloads)

    requested = [p for p in payloads if p["event"] == "requested"]
    assert len(requested) == 1
    assert requested[0]["slot"] == "keyword"
    assert requested[0]["kind"] == "keyword"
    assert requested[0]["cache"] == "none"
    assert requested[0]["outcome"] == "shown"
    assert "latency_ms" not in requested[0]
    assert not any(p["event"] == "lookup" for p in payloads)


def test_identifier_tab_emits_requested_and_empty_lookup():
    payloads: list[dict] = []
    _complete("FROM ", payloads)

    events = {p["event"] for p in payloads}
    assert events == {"requested", "lookup"}
    requested = next(p for p in payloads if p["event"] == "requested")
    lookup = next(p for p in payloads if p["event"] == "lookup")
    assert requested["slot"] == "object"
    assert requested["outcome"] == "empty"
    assert lookup["outcome"] == "empty"
    assert lookup["cache"] == "none"
    assert lookup["slot"] == "object"


def test_payload_keys_are_only_the_spec_fields():
    payload = build_completion_payload(
        event="lookup",
        slot="column",
        kind="column",
        cache="miss",
        outcome="timeout",
        latency_ms=12,
        error_class="timeout",
    )
    assert set(payload) <= ALLOWED_PAYLOAD_KEYS
    assert payload["latency_ms"] == 12
    assert payload["error_class"] == "timeout"


def test_sentinels_absent_from_serialized_payloads():
    payloads: list[dict] = []
    _complete(_SENTINEL_SQL, payloads)
    blob = json.dumps(payloads)
    for sentinel in (
        _SENTINEL_SQL,
        _SENTINEL_DB,
        _SENTINEL_SCHEMA,
        _SENTINEL_TABLE,
        _SENTINEL_PREFIX,
        _SENTINEL_SUGGESTION,
        "SELECT * FROM",
    ):
        assert sentinel not in blob


def test_emit_does_not_reuse_command_event_types():
    with mock.patch("snowflake.cli._plugins.sql.completion.telemetry._send") as send:
        payload = emit_completion(
            event="requested",
            slot="keyword",
            kind="keyword",
            cache="none",
            outcome="shown",
        )
    assert payload["event"] == "requested"
    sent = send.call_args.args[0]
    assert sent["event"] == "requested"
    assert "executing_command" not in json.dumps(sent)
    assert "error_executing_command" not in json.dumps(sent)
    assert "result_executing_command" not in json.dumps(sent)


def test_disabled_outcome_is_expressible_without_sql():
    payload = build_completion_payload(
        event="lookup",
        slot="none",
        kind="none",
        cache="none",
        outcome="disabled",
    )
    assert payload["outcome"] == "disabled"
    assert json.dumps(payload).isascii()


def test_accept_hook_emits_accepted_without_names():
    payloads: list[dict] = []
    with mock.patch(
        "snowflake.cli._plugins.sql.completion.telemetry.emit_completion",
        side_effect=lambda **kwargs: payloads.append(
            build_completion_payload(**kwargs)
        ),
    ):
        on_completion_accepted(
            slot="object",
            kind="table",
            suggestion=_SENTINEL_SUGGESTION,
        )
    assert len(payloads) == 1
    assert payloads[0]["event"] == "accepted"
    assert payloads[0]["slot"] == "object"
    assert payloads[0]["kind"] == "table"
    blob = json.dumps(payloads)
    assert _SENTINEL_SUGGESTION not in blob
    assert set(payloads[0]) <= ALLOWED_PAYLOAD_KEYS


def test_classify_error_emits_internal_error_not_buffer():
    payloads: list[dict] = []

    def capture(**kwargs):
        payload = build_completion_payload(**kwargs)
        payloads.append(payload)
        return payload

    with mock.patch(
        "snowflake.cli._plugins.sql.completion.completer.classify",
        side_effect=RuntimeError(_SENTINEL_SQL),
    ), mock.patch(
        "snowflake.cli._plugins.sql.completion.completer.emit_completion",
        side_effect=capture,
    ):
        assert (
            list(
                SqlReplCompleter().get_completions(
                    Document(_SENTINEL_SQL, cursor_position=len(_SENTINEL_SQL)),
                    CompleteEvent(completion_requested=True),
                )
            )
            == []
        )
    assert payloads
    assert payloads[0]["outcome"] == "error"
    assert payloads[0]["error_class"] == "internal"
    assert _SENTINEL_SQL not in json.dumps(payloads)


def test_invalid_slot_does_not_raise():
    assert (
        emit_completion(
            event="requested",
            slot="not-a-slot",
            kind="keyword",
            cache="none",
            outcome="shown",
        )
        is None
    )


def test_telemetry_failure_still_yields_completions():
    with mock.patch(
        "snowflake.cli._plugins.sql.completion.completer.emit_completion",
        side_effect=RuntimeError("telemetry down"),
    ):
        texts = [
            completion.text
            for completion in SqlReplCompleter().get_completions(
                Document("SEL"), CompleteEvent(completion_requested=True)
            )
        ]
    assert "SELECT" in texts


def test_threaded_completer_queues_event_without_click_context():
    import asyncio

    from prompt_toolkit.completion import ThreadedCompleter
    from snowflake.cli._app.telemetry import CLITelemetryClient, _telemetry
    from snowflake.cli.api.cli_global_context import get_cli_context

    channel = mock.MagicMock()
    context = get_cli_context()
    _telemetry._pending.clear()  # noqa: SLF001

    async def collect():
        wrapped = ThreadedCompleter(SqlReplCompleter())
        return [
            item
            async for item in wrapped.get_completions_async(
                Document("SEL"),
                CompleteEvent(completion_requested=True),
            )
        ]

    try:
        with mock.patch.object(
            CLITelemetryClient,
            "_telemetry",
            new_callable=mock.PropertyMock,
            return_value=channel,
        ), mock.patch.object(
            type(context),
            "connection",
            new_callable=mock.PropertyMock,
            side_effect=AssertionError,
        ):
            items = asyncio.run(collect())
            assert any(item.text == "SELECT" for item in items)
            assert channel.try_add_log_to_batch.call_count == 0
            queued = list(_telemetry._pending)  # noqa: SLF001
            assert queued
            messages = [message for message, _ts in queued]
            assert all(message.get("type") == "repl_completion" for message in messages)
            blob = json.dumps(messages)
            assert "SEL" not in blob
            assert "SELECT" not in blob
            with mock.patch.object(
                type(context),
                "connection_if_open",
                new_callable=mock.PropertyMock,
                return_value=mock.MagicMock(),
            ):
                _telemetry.drain_if_open()
            assert channel.try_add_log_to_batch.call_count == len(queued)
            assert _telemetry._pending == []  # noqa: SLF001
            for call in channel.try_add_log_to_batch.call_args_list:
                sent = call.args[0].to_dict()["message"]
                assert sent["type"] == "repl_completion"
                assert sent["source"] == "snowcli"
                assert sent["event"]
                sent_blob = json.dumps(sent)
                assert "SEL" not in sent_blob
                assert "SELECT" not in sent_blob
    finally:
        _telemetry._pending.clear()  # noqa: SLF001


def test_drain_if_open_never_dials():
    from snowflake.cli._app.telemetry import _telemetry, log_repl_completion
    from snowflake.cli.api.cli_global_context import get_cli_context

    context = get_cli_context()
    connection = mock.PropertyMock(side_effect=AssertionError)
    _telemetry._pending.clear()  # noqa: SLF001
    try:
        log_repl_completion(
            {
                "event": "accepted",
                "slot": "keyword",
                "kind": "keyword",
                "cache": "none",
                "outcome": "shown",
            }
        )
        pending_before = len(_telemetry._pending)  # noqa: SLF001
        assert pending_before
        may_open = mock.patch(
            "snowflake.cli._app.telemetry._telemetry_may_open_connection",
            return_value=True,
        )
        with may_open as may_open_mock, mock.patch.object(
            type(context),
            "connection_if_open",
            new_callable=mock.PropertyMock,
            return_value=None,
        ), mock.patch.object(type(context), "connection", new=connection):
            _telemetry.drain_if_open()
        connection.assert_not_called()
        may_open_mock.assert_not_called()
        assert len(_telemetry._pending) == pending_before  # noqa: SLF001
    finally:
        _telemetry._pending.clear()  # noqa: SLF001


def test_concurrent_completion_enqueue_and_drain_lose_nothing(monkeypatch):
    import sys
    import threading

    from snowflake.cli._app.telemetry import (
        CLITelemetryClient,
        _telemetry,
        log_repl_completion,
    )
    from snowflake.cli.api.cli_global_context import get_cli_context

    channel = mock.MagicMock()
    context = get_cli_context()
    previous_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    monkeypatch.setattr(CLITelemetryClient, "_PENDING_LIMIT", 10000)
    _telemetry._pending.clear()  # noqa: SLF001
    payload = {
        "event": "accepted",
        "slot": "keyword",
        "kind": "keyword",
        "cache": "none",
        "outcome": "shown",
    }
    threads: list[threading.Thread] = []
    try:
        with mock.patch.object(
            CLITelemetryClient,
            "_telemetry",
            new_callable=mock.PropertyMock,
            return_value=channel,
        ), mock.patch.object(
            type(context),
            "connection_if_open",
            new_callable=mock.PropertyMock,
            return_value=mock.MagicMock(),
        ):

            def worker():
                for _ in range(200):
                    log_repl_completion(payload)

            threads = [threading.Thread(target=worker) for _ in range(4)]
            for thread in threads:
                thread.start()
            while any(thread.is_alive() for thread in threads):
                _telemetry.drain_if_open()
            for thread in threads:
                thread.join()
            _telemetry.drain_if_open()
            assert _telemetry._pending == []  # noqa: SLF001
        assert channel.try_add_log_to_batch.call_count == 800
    finally:
        for thread in threads:
            thread.join(timeout=1)
        sys.setswitchinterval(previous_interval)
        _telemetry._pending.clear()  # noqa: SLF001
