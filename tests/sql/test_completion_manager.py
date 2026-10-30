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

# ruff: noqa: SLF001
from unittest import mock

import pytest
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli._plugins.sql.statement_reader import CompiledStatement


def _manager_with_hook():
    manager = SqlManager()
    seen: list[str] = []
    manager.on_compiled_success = seen.append
    return manager, seen


def test_yield_from_success_notifies_rendered_sql():
    manager, seen = _manager_with_hook()
    manager.execute_string = mock.Mock(return_value=iter([mock.Mock()]))
    stmts = [
        CompiledStatement(statement="CREATE TABLE T_RENDERED"),
        CompiledStatement(statement="CREATE TABLE T_TWO"),
    ]
    list(
        manager._execute_compiled_statements(stmts, cursor_class=object)  # noqa: SLF001
    )  # noqa: SLF001
    assert seen == ["CREATE TABLE T_RENDERED", "CREATE TABLE T_TWO"]


def test_async_statement_skips_success_hook():
    manager, seen = _manager_with_hook()
    cursor = mock.Mock()
    conn = mock.Mock()
    conn.cursor.return_value = cursor
    stmts = [CompiledStatement(statement="CREATE TABLE T_ASYNC", execute_async=True)]
    with mock.patch.object(type(manager), "_conn", conn):
        list(
            manager._execute_compiled_statements(
                stmts, cursor_class=object
            )  # noqa: SLF001
        )  # noqa: SLF001
    cursor.execute_async.assert_called_once_with("CREATE TABLE T_ASYNC")
    assert seen == []


def test_failed_stmt_two_does_not_patch_stmt_three():
    manager, seen = _manager_with_hook()

    def execute_string(sql, **_kwargs):
        if "T2" in sql:
            raise RuntimeError("stmt 2 failed")
        return iter([mock.Mock()])

    manager.execute_string = execute_string
    stmts = [
        CompiledStatement(statement="CREATE TABLE T1"),
        CompiledStatement(statement="CREATE TABLE T2"),
        CompiledStatement(statement="CREATE TABLE T3"),
    ]
    with pytest.raises(RuntimeError, match="stmt 2 failed"):
        list(
            manager._execute_compiled_statements(
                stmts, cursor_class=object
            )  # noqa: SLF001
        )  # noqa: SLF001
    assert seen == ["CREATE TABLE T1"]
