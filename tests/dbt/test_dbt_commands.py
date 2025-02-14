# Copyright (c) 2025 Snowflake Inc.
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

import pytest


@pytest.fixture
def mock_connect(mock_ctx):
    with mock.patch("snowflake.connector.connect") as _fixture:
        ctx = mock_ctx()
        _fixture.return_value = ctx
        _fixture.mocked_ctx = _fixture.return_value
        yield _fixture


def test_dbt_list(mock_connect, runner):

    result = runner.invoke(["dbt", "list"])

    assert result.exit_code == 0, result.output
    assert mock_connect.mocked_ctx.get_query() == "SHOW DBT"


@pytest.mark.parametrize(
    "args,expected_query",
    [
        (
            ["dbt", "execute", "compile", "--name=pipeline_name"],
            "EXECUTE DBT pipeline_name compile",
        ),
        (
            [
                "dbt",
                "execute",
                "compile",
                "--name=pipeline_name",
                "-f",
                "--select @source:snowplow,tag:nightly models/export",
            ],
            "EXECUTE DBT pipeline_name compile -f --select @source:snowplow,tag:nightly models/export",
        ),
        (
            ["dbt", "execute", "compile", "--name=pipeline_name", "--vars '{foo:bar}'"],
            "EXECUTE DBT pipeline_name compile --vars '{foo:bar}'",
        ),
        (
            [
                "dbt",
                "execute",
                "compile",
                "--name=pipeline_name",
                "--format=JSON",
                "--debug",
            ],
            "EXECUTE DBT pipeline_name compile",
        ),
    ],
)
def test_dbt_execute(mock_connect, runner, args, expected_query):

    result = runner.invoke(args)

    assert result.exit_code == 0, result.output
    assert mock_connect.mocked_ctx.get_query() == expected_query
