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

from types import SimpleNamespace

import pytest

from tests_common.deflake import (
    DEFAULT_MAX_RETRIES,
    TRANSIENT_MAX_RETRIES,
    DeflakePlugin,
    TestPhase,
    TestResult,
    matches_known_server_issue,
    max_retries_for,
)

HTTP_429 = (
    "snowflake.connector.errors.TooManyRequests: 290429: 290429: "
    "HTTP 429: Too Many Requests"
)
HTTP_503 = "ERROR snowflake.connector.network: 290503: HTTP 503: Service Unavailable"


@pytest.mark.parametrize(
    "text,expected",
    [
        (HTTP_429, True),
        (HTTP_503, True),
        ("Exceeded maximum number of inbound queries allowed for this instance", True),
        ("GS instance is still unavailable at 2026-09-22", True),
        ("Insufficient resource during interleaved execution", True),
        ("assert result.exit_code == 0", False),
        ("", False),
    ],
)
def test_matches_known_server_issue(text, expected):
    assert matches_known_server_issue(text) is expected


@pytest.mark.parametrize(
    "text",
    [
        "HTTP 429:\nToo Many Requests",
        "E       ...HTTP 429: Too\nE       Many Requests",
        (
            "E       snowflake.connector.errors.TooManyRequests: ┌─ Error "
            + "─" * 55
            + "┐\n"
            "E         │ 290429 (08001): 01c741ea-081a-988e-0001-c1be39e42a7e: HTTP 429: Too Many   │\n"
            "E         │ Requests                                                                    │\n"
        ),
    ],
)
def test_matches_known_server_issue_across_pretty_wrapped_lines(text):
    assert matches_known_server_issue(text) is True


@pytest.mark.parametrize(
    "text,expected",
    [
        (HTTP_429, TRANSIENT_MAX_RETRIES),
        (HTTP_503, TRANSIENT_MAX_RETRIES),
        ("assert 1 == 0", DEFAULT_MAX_RETRIES),
    ],
)
def test_max_retries_for_transient_vs_ordinary(text, expected):
    assert max_retries_for(text) == expected


def test_is_known_server_issue_on_call_phase_429():
    result = TestResult("tests_integration/test_object.py::test_create")
    result.call = TestPhase(outcome="failed", longrepr=HTTP_429)
    assert DeflakePlugin.is_known_server_issue(result) is True


def test_is_known_server_issue_ignores_ordinary_assertion():
    result = TestResult("tests_integration/test_object.py::test_create")
    result.call = TestPhase(outcome="failed", longrepr="assert result.exit_code == 0")
    assert DeflakePlugin.is_known_server_issue(result) is False


def test_protocol_retries_transient_failures_with_backoff(monkeypatch):
    sleeps: list[int] = []
    monkeypatch.setattr("tests_common.deflake._sleep", sleeps.append)

    class Runner:
        def __init__(self) -> None:
            self.calls = 0

        def runtestprotocol(self, item, nextitem):
            self.calls += 1
            still_retrying = self.calls < 4
            return [
                SimpleNamespace(
                    should_retry=still_retrying, transient_retry=still_retrying
                )
            ]

    plugin = DeflakePlugin.__new__(DeflakePlugin)
    plugin.runner = Runner()
    item = SimpleNamespace(
        nodeid="t",
        location=("t", 1, "t"),
        ihook=SimpleNamespace(
            pytest_runtest_logstart=lambda **kwargs: None,
            pytest_runtest_logfinish=lambda **kwargs: None,
        ),
    )

    plugin.pytest_runtest_protocol(item, None)

    assert plugin.runner.calls == 4
    assert sleeps == [5, 10, 20]
