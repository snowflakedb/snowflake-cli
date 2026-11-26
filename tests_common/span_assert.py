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

"""Scoped assertions over completed telemetry spans.

A test owns a prefix or a subtree. Extra spans outside that view do not fail
the test. ``assert_exactly`` is closed only inside the view, so a later
``core.*`` or ``cmd.other.*`` span cannot break a command's tree.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, NamedTuple, Optional, Sequence, Union

from snowflake.cli._app.telemetry import CLITelemetryField, TelemetryEvent
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.metrics import CLIMetrics, CLIMetricsSpan

_UNSET = object()
_Span = Mapping[str, Any]


class SpanEdge(NamedTuple):
    name: str
    parent: Optional[str] = None


def span(name: str, *, parent: Optional[str] = None) -> SpanEdge:
    return SpanEdge(name, parent)


def _name(span: _Span) -> str:
    return span[CLIMetricsSpan.NAME_KEY]


def _parent(span: _Span) -> Optional[str]:
    return span[CLIMetricsSpan.PARENT_KEY]


def _id(span: _Span) -> Optional[str]:
    return span.get(CLIMetricsSpan.ID_KEY)


def _parent_id(span: _Span) -> Optional[str]:
    return span.get(CLIMetricsSpan.PARENT_ID_KEY)


def _error(span: _Span) -> Optional[str]:
    return span[CLIMetricsSpan.ERROR_KEY]


def _error_code(span: _Span) -> Any:
    return span.get(CLIMetricsSpan.ERROR_CODE_KEY)


def _edges(spans: Sequence[_Span]) -> list[SpanEdge]:
    return [span(_name(row), parent=_parent(row)) for row in spans]


def _looks_like_telemetry_batch(source: Any) -> bool:
    calls = getattr(source, "call_args_list", None)
    if not calls:
        return False
    try:
        first = calls[0].args[0]
    except (IndexError, AttributeError, TypeError):
        return False
    return callable(getattr(first, "to_dict", None))


def _batch_from(source: Any):
    if _looks_like_telemetry_batch(source):
        return source
    mocked_ctx = getattr(source, "mocked_ctx", None)
    for candidate in (mocked_ctx, source):
        if candidate is None:
            continue
        telemetry = getattr(candidate, "_telemetry", None)
        batch = getattr(telemetry, "try_add_log_to_batch", None)
        if _looks_like_telemetry_batch(batch):
            return batch
    raise TypeError(
        "Expected a try_add_log_to_batch mock, a connection with _telemetry, "
        f"or mock_connect; got {type(source)!r}"
    )


def _messages_of_type(source: Any, event: TelemetryEvent) -> list[dict]:
    batch = _batch_from(source)
    messages = [call.args[0].to_dict()["message"] for call in batch.call_args_list]
    return [message for message in messages if message.get("type") == event.value]


class CompletedSpans:
    """Completed spans from one telemetry payload or metrics object."""

    def __init__(self, spans: Sequence[_Span]):
        self._spans = list(spans)

    @classmethod
    def from_result(cls, source: Any, *, check_limits: bool = True) -> CompletedSpans:
        """Last ``result_executing_command`` on a telemetry batch or connection."""
        return cls.from_message(
            cls._require_event(source, TelemetryEvent.CMD_EXECUTION_RESULT),
            check_limits=check_limits,
        )

    @classmethod
    def from_error(cls, source: Any, *, check_limits: bool = True) -> CompletedSpans:
        """Last ``error_executing_command`` on a telemetry batch or connection."""
        return cls.from_message(
            cls._require_event(source, TelemetryEvent.CMD_EXECUTION_ERROR),
            check_limits=check_limits,
        )

    @classmethod
    def from_message(
        cls, message: Mapping[str, Any], *, check_limits: bool = True
    ) -> CompletedSpans:
        spans_field = message[CLITelemetryField.SPANS.value]
        if check_limits:
            assert (
                spans_field[CLITelemetryField.NUM_SPANS_PAST_DEPTH_LIMIT.value] == 0
            ), spans_field
            assert (
                spans_field[CLITelemetryField.NUM_SPANS_PAST_TOTAL_LIMIT.value] == 0
            ), spans_field
        return cls(spans_field[CLITelemetryField.COMPLETED_SPANS.value])

    @classmethod
    def from_metrics(cls, metrics: Optional[CLIMetrics] = None) -> CompletedSpans:
        if metrics is None:
            metrics = get_cli_context().metrics
        return cls(metrics.completed_spans)

    @staticmethod
    def _require_event(source: Any, event: TelemetryEvent) -> dict:
        matches = _messages_of_type(source, event)
        assert matches, f"No {event.value} telemetry event in {source!r}"
        return matches[-1]

    def prefix(self, *prefixes: str) -> CompletedSpans:
        if not prefixes:
            raise ValueError("prefix() requires at least one prefix")
        return CompletedSpans(
            [span for span in self._spans if _name(span).startswith(prefixes)]
        )

    def named(self, name: str) -> CompletedSpans:
        return CompletedSpans([span for span in self._spans if _name(span) == name])

    def under(self, *root_names: str) -> CompletedSpans:
        if not root_names:
            raise ValueError("under() requires at least one root name")
        selected = [False] * len(self._spans)
        reached_ids: set[str] = set()
        for index, row in enumerate(self._spans):
            if _name(row) in root_names:
                selected[index] = True
                span_id = _id(row)
                if span_id is not None:
                    reached_ids.add(span_id)
        changed = True
        while changed:
            changed = False
            for index, row in enumerate(self._spans):
                if selected[index]:
                    continue
                parent_id = _parent_id(row)
                if parent_id is not None and parent_id in reached_ids:
                    selected[index] = True
                    span_id = _id(row)
                    if span_id is not None:
                        reached_ids.add(span_id)
                    changed = True
        return CompletedSpans(
            [row for index, row in enumerate(self._spans) if selected[index]]
        )

    def get(self, name: str, *, parent: Union[str, None, object] = _UNSET) -> dict:
        matches = [span for span in self._spans if _name(span) == name]
        if parent is not _UNSET:
            matches = [span for span in matches if _parent(span) == parent]
        if not matches:
            raise AssertionError(
                f"Span {name!r} not found. Recorded spans: {self._dump()}"
            )
        if len(matches) > 1:
            raise AssertionError(
                f"Span {name!r} is not unique. Recorded spans: {self._dump()}"
            )
        return dict(matches[0])

    def assert_present(self, *names: str) -> None:
        missing = [name for name in names if name not in self]
        assert not missing, f"Missing spans {missing}. Recorded spans: {self._dump()}"

    def assert_absent(self, *names: str) -> None:
        present = [name for name in names if name in self]
        assert (
            not present
        ), f"Unexpected spans {present}. Recorded spans: {self._dump()}"

    def assert_parent(self, name: str, parent: Optional[str]) -> None:
        span = self.get(name)
        actual = _parent(span)
        assert actual == parent, (
            f"Expected {name!r} parent {parent!r}, got {actual!r}. "
            f"Recorded spans: {self._dump()}"
        )

    def assert_error(
        self,
        name: str,
        error: str,
        *,
        code: Any = _UNSET,
        parent: Union[str, None, object] = _UNSET,
    ) -> None:
        span = self.get(name, parent=parent)
        actual = _error(span)
        assert actual == error, (
            f"Expected {name!r} error {error!r}, got {actual!r}. "
            f"Recorded spans: {self._dump()}"
        )
        if code is not _UNSET:
            actual_code = _error_code(span)
            assert actual_code == code, (
                f"Expected {name!r} error_code {code!r}, got {actual_code!r}. "
                f"Recorded spans: {self._dump()}"
            )

    def assert_clean(self, *names: str) -> None:
        spans: Iterable[_Span] = (
            [self.get(name) for name in names] if names else self._spans
        )
        dirty = [
            (_name(span), _error(span)) for span in spans if _error(span) is not None
        ]
        assert (
            not dirty
        ), f"Expected no errors, got {dirty}. Recorded spans: {self._dump()}"

    def assert_each(
        self,
        *,
        parent: Union[str, None, object] = _UNSET,
        error: Union[str, None, object] = _UNSET,
        code: Any = _UNSET,
    ) -> None:
        if parent is _UNSET and error is _UNSET and code is _UNSET:
            raise ValueError(
                "assert_each() requires at least one of parent, error, code"
            )
        mismatches = []
        for span in self._spans:
            reasons = []
            if parent is not _UNSET and _parent(span) != parent:
                reasons.append(f"parent {_parent(span)!r} != {parent!r}")
            if error is not _UNSET and _error(span) != error:
                reasons.append(f"error {_error(span)!r} != {error!r}")
            if code is not _UNSET and _error_code(span) != code:
                reasons.append(f"error_code {_error_code(span)!r} != {code!r}")
            if reasons:
                mismatches.append((_name(span), reasons))
        assert (
            not mismatches
        ), f"Spans did not match {mismatches}. Recorded spans: {self._dump()}"

    def assert_len(
        self, *, exactly: Optional[int] = None, at_least: Optional[int] = None
    ) -> None:
        if (exactly is None) == (at_least is None):
            raise ValueError(
                "assert_len() requires exactly one of exactly= or at_least="
            )
        actual = len(self._spans)
        if exactly is not None:
            noun = "span" if exactly == 1 else "spans"
            assert (
                actual == exactly
            ), f"Expected {exactly} {noun}, got {actual}. Recorded spans: {self._dump()}"
            return
        assert at_least is not None
        noun = "span" if at_least == 1 else "spans"
        assert actual >= at_least, (
            f"Expected at least {at_least} {noun}, got {actual}. "
            f"Recorded spans: {self._dump()}"
        )

    def assert_exactly(self, *edges: SpanEdge) -> None:
        actual = _edges(self._spans)
        expected = list(edges)
        assert (
            actual == expected
        ), f"Span tree mismatch.\nExpected: {expected}\nActual:   {actual}"

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        return any(_name(span) == name for span in self._spans)

    def __iter__(self):
        return iter(self._spans)

    def __len__(self) -> int:
        return len(self._spans)

    def _dump(self) -> list[SpanEdge]:
        return _edges(self._spans)
