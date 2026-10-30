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

"""Snowpark Session reuse in ``FeatureManager``.

``_build_session`` must return the one CLI-owned Snowpark Session cached on
``SqlExecutionMixin.snowpark_session`` — not construct a fresh Session (and a
fresh wrapper over ``self._conn``) on every call. A single ``snow feature
plan`` calls ``_build_session`` once per state-fetch facade plus the init-first
guard, so a per-call ``Session.builder...create()`` leaks ~5 sessions.

These tests deliberately do NOT use the autouse ``_build_session`` patch that
``test_manager.py`` installs; they exercise the real method against a mocked
Snowpark ``Session`` class so no real connection is opened.
"""

from __future__ import annotations

from unittest import mock

import pytest


@pytest.fixture
def patched_snowpark_session():
    """Patch every ``Session`` binding ``_build_session`` might reach.

    ``SqlExecutionMixin.snowpark_session`` does ``from
    snowflake.snowpark.session import Session`` while a per-call factory would
    do ``from snowflake.snowpark import Session``; these are distinct module
    attributes, so both are patched to the same mock class. ``create()``
    always returns the same sentinel session so the test can count how many
    times a Session was actually built.
    """
    fake_session = mock.MagicMock(name="snowpark_session")
    session_cls = mock.MagicMock(name="Session")
    session_cls.builder.configs.return_value.create.return_value = fake_session

    with mock.patch("snowflake.snowpark.session.Session", session_cls), mock.patch(
        "snowflake.snowpark.Session", session_cls, create=True
    ):
        yield session_cls, fake_session


def _create_call_count(session_cls: mock.MagicMock) -> int:
    return session_cls.builder.configs.return_value.create.call_count


def test_build_session_caches_single_session(patched_snowpark_session):
    """Two ``_build_session`` calls return the same object and build once."""
    from snowflake.cli._plugins.feature.manager import FeatureManager

    session_cls, fake_session = patched_snowpark_session
    mgr = FeatureManager(connection=mock.MagicMock(name="conn"))

    first = mgr._build_session()  # noqa: SLF001
    second = mgr._build_session()  # noqa: SLF001

    assert first is second
    assert first is fake_session
    assert _create_call_count(session_cls) == 1


def test_build_session_returns_mixin_snowpark_session(patched_snowpark_session):
    """``_build_session`` reuses ``SqlExecutionMixin.snowpark_session``.

    Accessing the mixin cache first must satisfy a subsequent
    ``_build_session`` without building a second Session.
    """
    from snowflake.cli._plugins.feature.manager import FeatureManager

    session_cls, _ = patched_snowpark_session
    mgr = FeatureManager(connection=mock.MagicMock(name="conn"))

    cached = mgr.snowpark_session
    built = mgr._build_session()  # noqa: SLF001

    assert built is cached
    assert _create_call_count(session_cls) == 1
