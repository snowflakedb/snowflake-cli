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

from unittest import mock


@mock.patch("snowflake.connector.connect")
def test_list_views(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["view", "list"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == ["show views like '%%'"]


@mock.patch("snowflake.connector.connect")
def test_list_views_with_like(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["view", "list", "--like", "my%"], catch_exceptions=False)

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == ["show views like 'my%'"]


@mock.patch("snowflake.connector.connect")
def test_create_view(mock_connector, runner, mock_ctx, mock_cursor):
    ctx = mock_ctx(
        mock_cursor(
            rows=[{"status": "View MY_VIEW successfully created."}],
            columns=["status"],
        )
    )
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["view", "create", "MY_VIEW", "--query", "SELECT 1"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == ["create view IDENTIFIER('MY_VIEW') as SELECT 1"]


@mock.patch("snowflake.connector.connect")
def test_create_view_with_replace(mock_connector, runner, mock_ctx, mock_cursor):
    ctx = mock_ctx(
        mock_cursor(
            rows=[{"status": "View MY_VIEW successfully created."}],
            columns=["status"],
        )
    )
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["view", "create", "MY_VIEW", "--query", "SELECT 1", "--replace"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_queries() == ["create or replace view IDENTIFIER('MY_VIEW') as SELECT 1"]
