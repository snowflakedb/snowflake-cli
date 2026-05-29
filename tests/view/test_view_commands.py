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

from tests_common.feature_flag_utils import with_feature_flags
from snowflake.cli.api.feature_flags import FeatureFlag


@mock.patch("snowflake.connector.connect")
@with_feature_flags({FeatureFlag.ENABLE_VIEW_COMMANDS: True})
def test_list_views(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["view", "list"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show views like '%%'"


@mock.patch("snowflake.connector.connect")
@with_feature_flags({FeatureFlag.ENABLE_VIEW_COMMANDS: True})
def test_list_views_with_like(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["view", "list", "--like", "MY_%"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show views like 'MY_%'"


@mock.patch("snowflake.connector.connect")
@with_feature_flags({FeatureFlag.ENABLE_VIEW_COMMANDS: True})
def test_create_view(mock_connector, runner, mock_ctx, mock_statement_success):
    ctx = mock_ctx(cursor=mock_statement_success())
    mock_connector.return_value = ctx
    result = runner.invoke(["view", "create", "MY_VIEW", "SELECT 1"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "create view IDENTIFIER('MY_VIEW') as SELECT 1"


@with_feature_flags({FeatureFlag.ENABLE_VIEW_COMMANDS: True})
def test_create_view_missing_query(runner):
    result = runner.invoke(["view", "create", "MY_VIEW"])
    assert result.exit_code != 0


def test_view_hidden_without_flag(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert "view" not in result.output
