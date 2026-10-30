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

import threading

import pytest
from snowflake.cli._plugins.sql.completion.context import CompletionKind
from snowflake.cli._plugins.sql.completion.fetch_indicator import (
    FetchIndicator,
    IndicatingProvider,
)
from snowflake.cli._plugins.sql.completion.introspection import MetadataError


def test_indicator_is_blank_when_idle():
    indicator = FetchIndicator(invalidate=lambda: None)
    assert indicator.text() == []
    assert not indicator.is_running


def test_indicator_spins_only_while_running():
    ticks = []
    indicator = FetchIndicator(invalidate=lambda: ticks.append(1), clock=lambda: 0.0)
    with indicator.running():
        assert indicator.is_running
        assert indicator.text()[0][1].endswith("fetching names")
    assert not indicator.is_running
    assert indicator.text() == []
    assert len(ticks) >= 2


def test_indicator_frame_advances_with_clock():
    now = [0.0]
    indicator = FetchIndicator(invalidate=lambda: None, clock=lambda: now[0])
    with indicator.running():
        first = indicator.text()[0][1]
        now[0] = 0.1
        second = indicator.text()[0][1]
    assert first != second


def test_indicator_ticker_redraws_until_done():
    redrawn = threading.Event()
    calls = []

    def invalidate():
        calls.append(1)
        if len(calls) >= 3:
            redrawn.set()

    indicator = FetchIndicator(invalidate=invalidate)
    with indicator.running():
        assert redrawn.wait(2)


class _Provider:
    def __init__(self, indicator, result=None, error=None):
        self._indicator = indicator
        self._result = result
        self._error = error
        self.running_during_lookup = None

    def lookup(self, kind, path, prefix):
        self.running_during_lookup = self._indicator.is_running
        if self._error:
            raise self._error
        return self._result


def test_indicating_provider_runs_lookup_under_indicator():
    indicator = FetchIndicator(invalidate=lambda: None)
    inner = _Provider(indicator, result=["T1", "T2"])
    names = IndicatingProvider(inner, indicator).lookup(CompletionKind.TABLE, (), "t")
    assert names == ("T1", "T2")
    assert inner.running_during_lookup is True
    assert not indicator.is_running


def test_indicating_provider_stops_spinner_on_error():
    indicator = FetchIndicator(invalidate=lambda: None)
    inner = _Provider(indicator, error=MetadataError("timeout"))
    with pytest.raises(MetadataError):
        IndicatingProvider(inner, indicator).lookup(CompletionKind.TABLE, (), "t")
    assert not indicator.is_running
