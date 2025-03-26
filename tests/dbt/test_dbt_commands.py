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

from unittest import mock

import pytest
from snowflake.cli.api.identifiers import FQN


class TestDBTList:
    def test_list_command_alias(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "list",
                "dbt-project",
                "--like",
                "%PROJECT_NAME%",
                "--in",
                "database",
                "my_db",
            ]
        )

        assert result.exit_code == 0, result.output
        result = runner.invoke(
            ["dbt", "list", "--like", "%PROJECT_NAME%", "--in", "database", "my_db"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert (
            queries[0]
            == queries[1]
            == "show dbt projects like '%PROJECT_NAME%' in database my_db"
        )


class TestDBTDeploy:
    @pytest.fixture
    def dbt_project_path(self, tmp_path_factory):
        source_path = tmp_path_factory.mktemp("dbt_project")
        dbt_file = source_path / "dbt_project.yml"
        dbt_file.touch()
        yield source_path

    @pytest.fixture
    def mock_cli_console(self):
        with mock.patch("snowflake.cli.api.console") as _fixture:
            yield _fixture

    @pytest.fixture
    def mock_exists(self):
        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager.exists", return_value=False
        ) as _fixture:
            yield _fixture

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploys_project_from_source(
        self,
        mock_create,
        mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_exists,
    ):

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == """CREATE DBT PROJECT TEST_PIPELINE
FROM @MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage"""
        )
        stage_fqn = FQN.from_string(f"dbt_TEST_PIPELINE_stage").using_context()
        mock_create.assert_called_once_with(stage_fqn, temporary=True)
        mock_put_recursive.assert_called_once_with(
            dbt_project_path, "@MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage"
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_dbt_version_from_option_has_precedence_over_file(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_exists,
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == """CREATE DBT PROJECT TEST_PIPELINE
FROM @MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage"""
        )

    @pytest.mark.parametrize("exists", (True, False))
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_force_flag_uses_create_or_replace(
        self,
        _mock_create,
        _mock_put_recursive,
        exists,
        mock_connect,
        runner,
        dbt_project_path,
        mock_exists,
    ):
        mock_exists.return_value = exists

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--force",
            ]
        )

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.get_query().startswith(
            "CREATE OR REPLACE DBT PROJECT"
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
            ],
        )

        assert result.exit_code == 1, result.output
        assert f"dbt_project.yml does not exist in directory" in result.output
        assert mock_connect.mocked_ctx.get_query() == ""

    def test_raises_when_dbt_project_exists_and_is_not_force(
        self, dbt_project_path, mock_connect, runner, mock_exists
    ):
        mock_exists.return_value = True

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
            ],
        )

        assert result.exit_code == 1, result.output
        assert (
            "DBT project TEST_PIPELINE already exists. Use --force flag to overwrite"
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
                "EXECUTE DBT PROJECT pipeline_name args='test'",
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
                "EXECUTE DBT PROJECT pipeline_name args='run -f --select @source:snowplow,tag:nightly models/export'",
                id="with-dbt-options",
            ),
            pytest.param(
                ["dbt", "execute", "pipeline_name", "compile", "--vars '{foo:bar}'"],
                "EXECUTE DBT PROJECT pipeline_name args='compile --vars '{foo:bar}''",
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
                "EXECUTE DBT PROJECT pipeline_name args='compile --format=TXT -v -h --debug --info --config-file=/'",
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
                "EXECUTE DBT PROJECT pipeline_name args='compile'",
                id="with-cli-flag",
            ),
        ],
    )
    def test_dbt_execute(self, mock_connect, runner, args, expected_query):

        result = runner.invoke(args)

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.kwargs[0]["_exec_async"] is False
        assert mock_connect.mocked_ctx.get_query() == expected_query

    def test_execute_async(self, mock_connect, runner):
        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--run-async",
                "pipeline_name",
                "compile",
            ]
        )

        assert result.exit_code == 0, result.output
        assert result.output.startswith("Command submitted")
        assert mock_connect.mocked_ctx.kwargs[0]["_exec_async"] is True
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name args='compile'"
        )
