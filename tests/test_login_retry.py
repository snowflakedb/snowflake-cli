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

from unittest.mock import Mock

import pytest
from snowflake.connector.errors import DatabaseError, OperationalError

from tests_common.login_retry import (
    LOGIN_TIMEOUT_MAX_RETRIES,
    connect_with_login_retry,
    is_login_timeout_error,
)

LOGIN_TIMEOUT = OperationalError(
    msg="Failed to login: login timed out after 120s",
    errno=14,
    sqlstate="HYT00",
)
AUTH_FAILED = OperationalError(
    msg="Incorrect username or password was specified",
    errno=250001,
    sqlstate="08001",
)


@pytest.mark.parametrize(
    "exc,expected",
    [
        (LOGIN_TIMEOUT, True),
        (AUTH_FAILED, False),
        (DatabaseError(msg="SQL compilation error"), False),
        (RuntimeError("login timed out after 120s"), True),
        (RuntimeError("assert result.exit_code == 0"), False),
    ],
)
def test_is_login_timeout_error(exc, expected):
    assert is_login_timeout_error(exc) is expected


def test_connect_retries_login_timeout_then_succeeds(monkeypatch):
    conn = Mock(name="connection")
    connects: list[int] = []
    sleeps: list[int] = []

    def fake_connect(**kwargs):
        connects.append(1)
        if len(connects) < 3:
            raise LOGIN_TIMEOUT
        return conn

    monkeypatch.setattr("tests_common.login_retry.connector.connect", fake_connect)
    monkeypatch.setattr("tests_common.login_retry._sleep", sleeps.append)

    assert connect_with_login_retry(account="a") is conn
    assert len(connects) == 3
    assert sleeps == [5, 10]


def test_connect_does_not_retry_ordinary_operational_error(monkeypatch):
    monkeypatch.setattr(
        "tests_common.login_retry.connector.connect",
        Mock(side_effect=AUTH_FAILED),
    )
    sleeps: list[int] = []
    monkeypatch.setattr("tests_common.login_retry._sleep", sleeps.append)

    with pytest.raises(OperationalError) as raised:
        connect_with_login_retry(account="a")

    assert raised.value is AUTH_FAILED
    assert sleeps == []


def test_connect_raises_after_retries_exhausted(monkeypatch):
    monkeypatch.setattr(
        "tests_common.login_retry.connector.connect",
        Mock(side_effect=LOGIN_TIMEOUT),
    )
    sleeps: list[int] = []
    monkeypatch.setattr("tests_common.login_retry._sleep", sleeps.append)

    with pytest.raises(OperationalError) as raised:
        connect_with_login_retry(account="a")

    assert raised.value is LOGIN_TIMEOUT
    assert len(sleeps) == LOGIN_TIMEOUT_MAX_RETRIES
    assert sleeps == [5, 10]
