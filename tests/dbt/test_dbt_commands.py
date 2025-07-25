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

from pathlib import Path
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dbt.constants import (
    OUTPUT_COLUMN_NAME,
    PROFILES_FILENAME,
    RESULT_COLUMN_NAME,
)
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
        dbt_project_file = source_path / "dbt_project.yml"
        dbt_project_file.write_text(yaml.dump({"profile": "dev"}))
        dbt_profiles_file = source_path / PROFILES_FILENAME
        dbt_profiles_file.write_text(
            yaml.dump(
                {
                    "dev": {
                        "outputs": {
                            "local": {
                                "account": "test_account",
                                "database": "testdb",
                                "role": "test_role",
                                "schema": "test_schema",
                                "threads": 2,
                                "type": "snowflake",
                                "user": "test_user",
                                "warehouse": "test_warehouse",
                            }
                        }
                    }
                },
            )
        )
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
        mock_put_recursive.assert_called_once()

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_force_flag_uses_create_or_replace(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
    ):

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

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_alters_existing_object(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_exists,
    ):
        mock_exists.return_value = True

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
            ]
        )

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.get_query().startswith(
            """ALTER DBT PROJECT TEST_PIPELINE ADD VERSION
FROM @MockDatabase.MockSchema.dbt_TEST_PIPELINE_stage"""
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_dbt_deploy_with_custom_profiles_dir(
        self,
        _mock_create,
        mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_exists,
    ):
        new_profiles_directory = Path(dbt_project_path) / "dbt_profiles"
        new_profiles_directory.mkdir(parents=True, exist_ok=True)
        profiles_file = dbt_project_path / PROFILES_FILENAME
        profiles_file.rename(new_profiles_directory / PROFILES_FILENAME)

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                f"--profiles-dir={new_profiles_directory}",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_put_recursive.assert_called_once()

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploys_project_with_fqn_uses_name_only_for_stage(
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
                "MockDatabase.MockSchema.test_dbt_project",
                f"--source={dbt_project_path}",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == """CREATE DBT PROJECT MockDatabase.MockSchema.test_dbt_project
FROM @MockDatabase.MockSchema.dbt_test_dbt_project_stage"""
        )
        # Verify stage creation uses only the name part of the FQN
        stage_fqn = FQN.from_string(f"dbt_test_dbt_project_stage").using_context()
        mock_create.assert_called_once_with(stage_fqn, temporary=True)
        mock_put_recursive.assert_called_once()

    def test_raises_when_dbt_project_yml_is_not_available(
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

    def test_raises_when_dbt_project_yml_does_not_specify_profile(
        self, dbt_project_path, mock_connect, runner
    ):
        with open((dbt_project_path / "dbt_project.yml"), "w") as f:
            yaml.dump({}, f)

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
            ],
        )

        assert result.exit_code == 1, result.output
        assert "`profile` is not defined in dbt_project.yml" in result.output
        assert mock_connect.mocked_ctx.get_query() == ""

    def test_raises_when_profiles_yml_is_not_available(
        self, dbt_project_path, mock_connect, runner
    ):
        (dbt_project_path / PROFILES_FILENAME).unlink()

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
            ],
        )

        assert result.exit_code == 1, result.output
        assert f"profiles.yml does not exist in directory" in result.output
        assert mock_connect.mocked_ctx.get_query() == ""

    def test_raises_when_profiles_yml_does_not_contain_selected_profile(
        self, dbt_project_path, mock_connect, runner
    ):
        with open((dbt_project_path / PROFILES_FILENAME), "w") as f:
            yaml.dump({}, f)

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
            ],
        )

        assert result.exit_code == 1, result.output
        assert "profile dev is not defined in profiles.yml" in result.output
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
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "database.schema.pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT database.schema.pipeline_name args='run'",
                id="with-fqn",
            ),
        ],
    )
    def test_dbt_execute(self, mock_connect, mock_cursor, runner, args, expected_query):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor

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

    def test_dbt_execute_dbt_failure_returns_non_0_code(
        self, mock_connect, mock_cursor, runner
    ):
        cursor = mock_cursor(
            rows=[(False, "1 of 4 FAIL 1 not_null_my_first_dbt_model_id")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "pipeline_name",
                "test",
            ]
        )

        assert result.exit_code == 1, result.output
        assert "1 of 4 FAIL 1 not_null_my_first_dbt_model_id" in result.output

    def test_dbt_execute_malformed_server_response(
        self, mock_connect, mock_cursor, runner
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=["foo", "bar"],
        )
        mock_connect.mocked_ctx.cs = cursor

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "pipeline_name",
                "test",
            ]
        )

        assert result.exit_code == 1, result.output
        assert "Malformed server response" in result.output

    def test_dbt_execute_no_rows_in_response(self, mock_connect, mock_cursor, runner):
        cursor = mock_cursor(
            rows=[],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "pipeline_name",
                "test",
            ]
        )

        assert result.exit_code == 1, result.output
        assert "No data returned from server" in result.output
