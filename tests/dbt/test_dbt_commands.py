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

from __future__ import annotations

from textwrap import dedent
from unittest import mock

import pytest
import yaml
from snowflake.cli.api.identifiers import FQN


@pytest.fixture
def mock_connect(mock_ctx):
    with mock.patch("snowflake.connector.connect") as _fixture:
        ctx = mock_ctx()
        _fixture.return_value = ctx
        _fixture.mocked_ctx = _fixture.return_value
        yield _fixture


class TestDBTList:
    def test_dbt_list(self, mock_connect, runner):

        result = runner.invoke(["dbt", "list"])

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.get_query() == "SHOW DBT PROJECT"


class TestDBTDeploy:
    @pytest.fixture
    def dbt_project_path(self, tmp_path_factory):
        source_path = tmp_path_factory.mktemp("dbt_project")
        dbt_file = source_path / "dbt_project.yml"
        dbt_file.touch()
        with dbt_file.open(mode="w") as fd:
            yaml.dump({"version": "1.2.3"}, fd)
        yield source_path

    @pytest.fixture
    def mock_cli_console(self):
        with mock.patch("snowflake.cli.api.console") as _fixture:
            yield _fixture

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploys_project_from_source(
        self, mock_create, mock_put_recursive, mock_connect, runner, dbt_project_path
    ):

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-adapter-version=3.4.5",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == """CREATE DBT PROJECT TEST_PIPELINE
FROM @MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage MAIN_FILE='@MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage/dbt_project.yml'
DBT_VERSION='1.2.3' DBT_ADAPTER_VERSION='3.4.5'"""
        )
        stage_fqn = FQN.from_string(f"dbt_TEST_PIPELINE_stage").using_context()
        mock_create.assert_called_once_with(stage_fqn, temporary=True)
        mock_put_recursive.assert_called_once_with(
            dbt_project_path, "@MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage"
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_dbt_version_from_option_has_precedence_over_file(
        self, _mock_create, _mock_put_recursive, mock_connect, runner, dbt_project_path
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-version=2.3.4",
                "--dbt-adapter-version=3.4.5",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == """CREATE DBT PROJECT TEST_PIPELINE
FROM @MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage MAIN_FILE='@MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage/dbt_project.yml'
DBT_VERSION='2.3.4' DBT_ADAPTER_VERSION='3.4.5'"""
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_force_flag_uses_create_or_replace(
        self, _mock_create, _mock_put_recursive, mock_connect, runner, dbt_project_path
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--force",
                "--dbt-adapter-version=3.4.5",
            ]
        )

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.get_query().startswith(
            "CREATE OR REPLACE DBT PROJECT"
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_execute_in_warehouse(
        self, _mock_create, _mock_put_recursive, mock_connect, runner, dbt_project_path
    ):

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-adapter-version=3.4.5",
                "--execute-in-warehouse=XL",
            ]
        )

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.get_query() == dedent(
            """CREATE DBT PROJECT TEST_PIPELINE
FROM @MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage MAIN_FILE='@MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage/dbt_project.yml'
DBT_VERSION='1.2.3' DBT_ADAPTER_VERSION='3.4.5' WAREHOUSE='XL'"""
        )

    def test_raises_when_dbt_project_is_not_available(
        self, dbt_project_path, mock_connect, runner
    ):
        dbt_file = dbt_project_path / "dbt_project.yml"
        dbt_file.unlink()

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-adapter-version=3.4.5",
            ],
        )

        assert result.exit_code == 1, result.output
        assert "dbt_project.yml does not exist in provided path." in result.output
        assert mock_connect.mocked_ctx.get_query() == ""

    def test_raises_when_dbt_project_version_is_not_specified(
        self, dbt_project_path, mock_connect, runner
    ):
        dbt_file = dbt_project_path / "dbt_project.yml"
        with dbt_file.open(mode="w") as fd:
            yaml.dump({}, fd)

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-adapter-version=3.4.5",
            ]
        )

        assert result.exit_code == 1, result.output
        assert (
            "dbt-version was not provided and is not available in dbt_project.yml"
            in result.output
        )
        assert mock_connect.mocked_ctx.get_query() == ""


class TestDBTExecute:
    @pytest.mark.parametrize(
        "args,expected_query",
        [
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "test",
                ],
                "EXECUTE DBT PROJECT pipeline_name test",
                id="simple-command",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "run",
                    "-f",
                    "--select @source:snowplow,tag:nightly models/export",
                ],
                "EXECUTE DBT PROJECT pipeline_name run -f --select @source:snowplow,tag:nightly models/export",
                id="with-dbt-options",
            ),
            pytest.param(
                ["dbt", "execute", "pipeline_name", "compile", "--vars '{foo:bar}'"],
                "EXECUTE DBT PROJECT pipeline_name compile --vars '{foo:bar}'",
                id="with-dbt-vars",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "compile",
                    "--format=TXT",  # collision with CLI's option; unsupported option
                    "-v",  # collision with CLI's option
                    "-h",
                    "--debug",
                    "--info",
                    "--config-file=/",
                ],
                "EXECUTE DBT PROJECT pipeline_name compile --format=TXT -v -h --debug --info --config-file=/",
                id="with-dbt-conflicting-options",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--format=JSON",
                    "pipeline_name",
                    "compile",
                ],
                "EXECUTE DBT PROJECT pipeline_name compile",
                id="with-cli-flag",
            ),
        ],
    )
    def test_dbt_execute(self, mock_connect, runner, args, expected_query):

        result = runner.invoke(args)

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.get_query() == expected_query
