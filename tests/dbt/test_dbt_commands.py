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
from snowflake.cli._plugins.dbt.constants import (
    OUTPUT_COLUMN_NAME,
    PROFILES_FILENAME,
    RESULT_COLUMN_NAME,
)
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.secure_path import SecurePath

from tests_common.feature_flag_utils import with_feature_flags


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


class TestDBTDrop:
    def test_drop_command_alias(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "drop",
                "dbt-project",
                "PROJECT_NAME",
            ]
        )

        assert result.exit_code == 0, result.output
        result = runner.invoke(
            ["dbt", "drop", "PROJECT_NAME"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert queries[0] == queries[1] == "drop dbt project IDENTIFIER('PROJECT_NAME')"


class TestDBTDescribe:
    def test_describe_command_alias(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "describe",
                "dbt-project",
                "PROJECT_NAME",
            ]
        )

        assert result.exit_code == 0, result.output
        result = runner.invoke(
            ["dbt", "describe", "PROJECT_NAME"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert (
            queries[0]
            == queries[1]
            == "describe dbt project IDENTIFIER('PROJECT_NAME')"
        )


class TestDBTDeploy:
    @pytest.fixture
    def mock_deploy(self):
        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager.deploy"
        ) as _fixture:
            yield _fixture

    def test_deploys_project_from_source(
        self,
        runner,
        dbt_project_path,
        mock_deploy,
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
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert str(mock_deploy.call_args[0][0]) == "TEST_PIPELINE"
        assert call_kwargs["path"] == SecurePath(dbt_project_path)
        assert call_kwargs["attrs"].dbt_version is None

    def test_force_flag_uses_create_or_replace(self, runner, mock_deploy):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--force",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert str(mock_deploy.call_args[0][0]) == "TEST_PIPELINE"
        assert call_kwargs["force"] is True

    def test_deploy_with_case_sensitive_name(self, runner, mock_deploy):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                '"MockDaTaBaSe"."PuBlIc"."caseSenSITIVEnAME"',
                f"--force",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        assert (
            str(mock_deploy.call_args[0][0])
            == '"MockDaTaBaSe"."PuBlIc"."caseSenSITIVEnAME"'
        )

    def test_dbt_deploy_with_custom_profiles_dir(
        self, runner, dbt_project_path, mock_deploy
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
                f"--profiles-dir={new_profiles_directory}",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert str(mock_deploy.call_args[0][0]) == "TEST_PIPELINE"
        assert call_kwargs["profiles_path"] == SecurePath(new_profiles_directory)

    def test_deploy_with_default_target_passes_to_manager(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--default-target=prod",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].default_target == "prod"
        assert call_kwargs["attrs"].unset_default_target is False

    def test_deploy_with_unset_default_target_passes_to_manager(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--unset-default-target",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].default_target is None
        assert call_kwargs["attrs"].unset_default_target is True

    def test_deploys_project_with_single_external_access_integration(
        self,
        runner,
        dbt_project_path,
        mock_deploy,
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--external-access-integration",
                "google_apis_access_integration",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].external_access_integrations == [
            "google_apis_access_integration"
        ]
        assert call_kwargs["attrs"].install_local_deps is False

    def test_deploys_project_with_multiple_external_access_integrations(
        self,
        runner,
        dbt_project_path,
        mock_deploy,
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--external-access-integration",
                "google_apis_access_integration",
                "--external-access-integration",
                "dbt_hub",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert sorted(call_kwargs["attrs"].external_access_integrations) == sorted(
            ["google_apis_access_integration", "dbt_hub"]
        )
        assert call_kwargs["attrs"].install_local_deps is False

    def test_deploys_project_with_local_deps(
        self,
        runner,
        dbt_project_path,
        mock_deploy,
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--install-local-deps",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert not call_kwargs["attrs"].external_access_integrations
        assert call_kwargs["attrs"].install_local_deps is True

    def test_deploy_with_both_default_target_and_unset_default_target_fails(
        self,
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
                "--default-target=prod",
                "--unset-default-target",
            ]
        )

        assert result.exit_code == 2, result.output
        assert (
            "Parameters '--unset-default-target' and '--default-target' are incompatible"
            in result.output
        )

    @with_feature_flags({FeatureFlag.ENABLE_DBT_VERSION: True})
    def test_deploy_with_dbt_version_passes_to_manager(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-version=1.9.0",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].dbt_version == "1.9.0"

    @with_feature_flags({FeatureFlag.ENABLE_DBT_VERSION: True})
    def test_deploy_with_invalid_dbt_version_fails(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-version=1.9",
            ]
        )

        assert result.exit_code == 2, result.output
        assert "Invalid version format '1.9'" in result.output
        mock_deploy.assert_not_called()

    @with_feature_flags({FeatureFlag.ENABLE_DBT_VERSION: True})
    def test_deploy_with_patch_version_passes_to_manager(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-version=1.9.4",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].dbt_version == "1.9.4"

    @with_feature_flags({FeatureFlag.ENABLE_DBT_VERSION: True})
    def test_deploy_with_prerelease_version_passes_to_manager(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-version=2.0.0-preview",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].dbt_version == "2.0.0-preview"


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
                "EXECUTE DBT PROJECT pipeline_name args='compile --vars \\'{foo:bar}\\''",
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
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "run",
                    "--vars",
                    '{"key": "value"}',
                ],
                "EXECUTE DBT PROJECT pipeline_name args='run --vars \\'{\"key\": \"value\"}\\''",
                id="vars-json-format",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "run",
                    "--vars",
                    '{"key": "value", "date": 20180101}',
                ],
                'EXECUTE DBT PROJECT pipeline_name args=\'run --vars \\\'{"key": "value", "date": 20180101}\\\'\'',
                id="vars-json-multiple-keys",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "run",
                    "--vars",
                    "{key: value, date: 20180101}",
                ],
                "EXECUTE DBT PROJECT pipeline_name args='run --vars \\'{key: value, date: 20180101}\\''",
                id="vars-yaml-format",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "run",
                    "--vars",
                    "key: value",
                ],
                "EXECUTE DBT PROJECT pipeline_name args='run --vars \\'key: value\\''",
                id="vars-single-key-value",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "run",
                    "--vars",
                    "{foo: foobar}",
                ],
                "EXECUTE DBT PROJECT pipeline_name args='run --vars \\'{foo: foobar}\\''",
                id="vars-yaml-with-braces",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "pipeline_name",
                    "run",
                    "--vars",
                    "start_date: 2016-06-01",
                    "--select",
                    "my_model",
                ],
                "EXECUTE DBT PROJECT pipeline_name args='run --vars \\'start_date: 2016-06-01\\' --select my_model'",
                id="vars-with-other-flags",
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

    @with_feature_flags({FeatureFlag.ENABLE_DBT_VERSION: True})
    def test_dbt_execute_with_dbt_version_when_flag_enabled(
        self, mock_connect, mock_cursor, runner
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--dbt-version=2.0.0",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name dbt_version='2.0.0' args='run'"
        )

    @with_feature_flags({FeatureFlag.ENABLE_DBT_VERSION: True})
    def test_dbt_execute_with_invalid_dbt_version_fails(self, mock_connect, runner):
        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--dbt-version=1.2.3.beta",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 2, result.output
        assert "Invalid version format '1.2.3.beta'" in result.output

    @with_feature_flags({FeatureFlag.ENABLE_DBT_VERSION: True})
    def test_dbt_execute_with_patch_version(self, mock_connect, mock_cursor, runner):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--dbt-version=1.9.4",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name dbt_version='1.9.4' args='run'"
        )

    @with_feature_flags({FeatureFlag.ENABLE_DBT_VERSION: True})
    def test_dbt_execute_with_prerelease_version(
        self, mock_connect, mock_cursor, runner
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--dbt-version=2.0.0-preview",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name dbt_version='2.0.0-preview' args='run'"
        )
