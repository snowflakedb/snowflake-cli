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
from snowflake.cli.api.exceptions import CliArgumentError
from snowflake.cli.api.secure_path import SecurePath


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

    def test_dbt_deploy_with_env_file_dir(
        self, runner, dbt_project_path, env_yml_dir, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                f"--env-file-dir={env_yml_dir}",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["env_file_path"] == SecurePath(env_yml_dir)

    def test_dbt_deploy_without_env_file_dir_passes_none(
        self, runner, dbt_project_path, mock_deploy
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
        assert call_kwargs["env_file_path"] is None

    def test_deploy_with_default_environment_passes_to_manager(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--default-env=dev",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].default_env == "dev"
        assert call_kwargs["attrs"].unset_default_env is False

    def test_deploy_with_unset_default_environment_passes_to_manager(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--unset-default-env",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].default_env is None
        assert call_kwargs["attrs"].unset_default_env is True

    def test_deploy_with_both_default_environment_and_unset_default_environment_fails(
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
                "--default-env=dev",
                "--unset-default-env",
            ]
        )

        assert result.exit_code == 2, result.output
        # Box-rendered error wraps across lines; check the key parts.
        assert "'--unset-default-env'" in result.output
        assert "'--default-env'" in result.output
        assert "incompatible" in result.output

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

    def test_deploy_with_dotted_prerelease_version_passes_to_manager(
        self, runner, dbt_project_path, mock_deploy
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--dbt-version=2.0.0-preview.175",
            ]
        )

        assert result.exit_code == 0, result.output
        mock_deploy.assert_called_once()
        call_kwargs = mock_deploy.call_args[1]
        assert call_kwargs["attrs"].dbt_version == "2.0.0-preview.175"

    def test_deploy_with_invalid_dbt_version_returns_exit_code_2(
        self, runner, dbt_project_path
    ):
        def raise_invalid_version(*args, **kwargs):
            raise CliArgumentError(
                "Invalid value '99.99.99' for --dbt-version. "
                "Supported versions: 1.9.4."
            )

        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager._validate_dbt_version",
            side_effect=raise_invalid_version,
        ), mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager._validate_profiles",
            return_value=None,
        ):
            result = runner.invoke(
                [
                    "dbt",
                    "deploy",
                    "TEST_PIPELINE",
                    f"--source={dbt_project_path}",
                    "--dbt-version=99.99.99",
                    "--enhanced-exit-codes",
                ]
            )

        assert result.exit_code == 2, result.output
        assert "Invalid value '99.99.99'" in result.output


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
                "EXECUTE DBT PROJECT pipeline_name args='compile --vars ''{foo:bar}'''",
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
                "EXECUTE DBT PROJECT pipeline_name args='run --vars ''{\"key\": \"value\"}'''",
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
                "EXECUTE DBT PROJECT pipeline_name args='run --vars ''{\"key\": \"value\", \"date\": 20180101}'''",
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
                "EXECUTE DBT PROJECT pipeline_name args='run --vars ''{key: value, date: 20180101}'''",
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
                "EXECUTE DBT PROJECT pipeline_name args='run --vars ''key: value'''",
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
                "EXECUTE DBT PROJECT pipeline_name args='run --vars ''{foo: foobar}'''",
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
                "EXECUTE DBT PROJECT pipeline_name args='run --vars ''start_date: 2016-06-01'' --select my_model'",
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

    @pytest.mark.parametrize(
        "args,expected_query",
        [
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--env=dev",
                    "pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT pipeline_name ENVIRONMENT='dev' args='run'",
                id="environment-named",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--env=NO_ENV",
                    "pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT pipeline_name ENVIRONMENT='NO_ENV' args='run'",
                id="environment-no-env-sentinel",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--env-vars",
                    '{"DBT_FOO": "1"}',
                    "pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT pipeline_name ENV_VARS=('DBT_FOO'='1') args='run'",
                id="env-vars-json-single",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--env-vars",
                    '{"DBT_FOO": "1", "DBT_BAR": "2"}',
                    "pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT pipeline_name "
                "ENV_VARS=('DBT_FOO'='1', 'DBT_BAR'='2') args='run'",
                id="env-vars-json-multi",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--env-vars",
                    "{DBT_FOO: '1', DBT_BAR: '2'}",
                    "pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT pipeline_name "
                "ENV_VARS=('DBT_FOO'='1', 'DBT_BAR'='2') args='run'",
                id="env-vars-yaml-quoted-strings",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--env-vars",
                    '{"DBT_URL": "https://example.com/?a=b"}',
                    "pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT pipeline_name "
                "ENV_VARS=('DBT_URL'='https://example.com/?a=b') args='run'",
                id="env-vars-value-with-equals",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--env-vars",
                    'DBT_MSG: "it\'s"',
                    "pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT pipeline_name "
                "ENV_VARS=('DBT_MSG'='it''s') args='run'",
                id="env-vars-value-with-single-quote-escaped",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--dbt-version=1.9.0",
                    "--env=prod",
                    "--env-vars",
                    '{"DBT_FOO": "1"}',
                    "pipeline_name",
                    "run",
                ],
                "EXECUTE DBT PROJECT pipeline_name dbt_version='1.9.0' "
                "ENVIRONMENT='prod' ENV_VARS=('DBT_FOO'='1') args='run'",
                id="all-options-ordering",
            ),
            pytest.param(
                [
                    "dbt",
                    "execute",
                    "--env=dev",
                    "--env-vars",
                    '{"DBT_OVERRIDE": "1"}',
                    "pipeline_name",
                    "run",
                    "--vars",
                    '{"key": "value"}',
                ],
                "EXECUTE DBT PROJECT pipeline_name ENVIRONMENT='dev' "
                "ENV_VARS=('DBT_OVERRIDE'='1') args='run --vars ''{\"key\": \"value\"}'''",
                id="env-vars-with-dbt-vars-flag",
            ),
        ],
    )
    def test_dbt_execute_env_var_options(
        self, mock_connect, mock_cursor, runner, args, expected_query
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor

        result = runner.invoke(args)

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.get_query() == expected_query

    @pytest.mark.parametrize(
        "raw_value,expected_error",
        [
            pytest.param(
                '"just_a_string"',
                "must be a YAML/JSON object",
                id="non-mapping-string",
            ),
            pytest.param(
                "[1, 2, 3]",
                "must be a YAML/JSON object",
                id="non-mapping-list",
            ),
            pytest.param(
                '{"DBT_X": null}',
                "must not be null",
                id="null-value",
            ),
            pytest.param(
                '{"DBT_X": 1}',
                "must be a string",
                id="int-value",
            ),
            pytest.param(
                '{"DBT_X": 1.5}',
                "must be a string",
                id="float-value",
            ),
            pytest.param(
                '{"DBT_X": true}',
                "must be a string",
                id="bool-value",
            ),
            pytest.param(
                '{"DBT_X": {"nested": "1"}}',
                "must be a string",
                id="nested-object",
            ),
            pytest.param(
                '{"DBT_X": ["1", "2"]}',
                "must be a string",
                id="nested-array",
            ),
            pytest.param(
                "{not: valid: yaml: at: all",
                "must be valid YAML/JSON",
                id="malformed-yaml",
            ),
            pytest.param(
                '{"DBT_FOO": "1", "DBT_FOO": "2"}',
                "duplicate key",
                id="duplicate-key",
            ),
            pytest.param(
                '{"": "v"}',
                "must not be empty",
                id="empty-key",
            ),
            pytest.param(
                '{"FOO": "1"}',
                "must start with",
                id="key-missing-dbt-prefix",
            ),
            pytest.param(
                '{"DBT-FOO": "1"}',
                "ASCII letters",
                id="key-invalid-chars-hyphen",
            ),
            pytest.param(
                '{"DBT FOO": "1"}',
                "ASCII letters",
                id="key-invalid-chars-space",
            ),
            pytest.param(
                '{"DBT_FOO": "value\\nwith\\nnewlines"}',
                "must not contain control characters",
                id="value-control-char",
            ),
        ],
    )
    def test_dbt_execute_env_vars_invalid_input(
        self, mock_connect, runner, raw_value, expected_error
    ):
        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--env-vars",
                raw_value,
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 1
        assert expected_error in result.output

    def test_dbt_execute_env_with_control_char_rejected(self, mock_connect, runner):
        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--env=dev\nprod",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 1
        assert "must not contain control characters" in result.output

    def test_dbt_execute_env_vars_secret_prefix_warns(
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
                "--env-vars",
                '{"DBT_ENV_SECRET_TOKEN": "xyz"}',
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert "DBT_ENV_SECRET_" in result.output
        assert "DBT_ENV_SECRET_TOKEN" in result.output
        assert (
            mock_connect.mocked_ctx.get_query() == "EXECUTE DBT PROJECT pipeline_name "
            "ENV_VARS=('DBT_ENV_SECRET_TOKEN'='xyz') args='run'"
        )

    def test_dbt_execute_env_vars_async(self, mock_connect, runner):
        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--run-async",
                "--env=dev",
                "--env-vars",
                '{"DBT_FOO": "1"}',
                "pipeline_name",
                "compile",
            ]
        )

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.kwargs[0]["_exec_async"] is True
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name ENVIRONMENT='dev' "
            "ENV_VARS=('DBT_FOO'='1') args='compile'"
        )

    def test_use_shell_env_vars_basic(
        self, mock_connect, mock_cursor, runner, clean_dbt_env
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor
        clean_dbt_env.setenv("DBT_FOO", "1")
        clean_dbt_env.setenv("DBT_BAR", "2")
        clean_dbt_env.setenv("PATH_OVERRIDE", "should-not-appear")
        clean_dbt_env.setenv("AWS_ACCESS_KEY", "should-not-appear")

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--use-shell-env-vars",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query() == "EXECUTE DBT PROJECT pipeline_name "
            "ENV_VARS=('DBT_BAR'='2', 'DBT_FOO'='1') args='run'"
        )
        assert "forwarded 2 shell environment variable(s)" in result.output

    def test_use_shell_env_vars_drops_secret_prefix(
        self, mock_connect, mock_cursor, runner, clean_dbt_env
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor
        clean_dbt_env.setenv("DBT_FOO", "1")
        clean_dbt_env.setenv("DBT_ENV_SECRET_TOKEN", "should-not-appear")

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--use-shell-env-vars",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        query = mock_connect.mocked_ctx.get_query()
        assert query == (
            "EXECUTE DBT PROJECT pipeline_name " "ENV_VARS=('DBT_FOO'='1') args='run'"
        )
        assert "should-not-appear" not in query
        assert "should-not-appear" not in result.output
        assert "dropped 1 DBT_ENV_SECRET_* environment variable(s)" in result.output

    def test_use_shell_env_vars_only_secrets_present(
        self, mock_connect, mock_cursor, runner, clean_dbt_env
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor
        clean_dbt_env.setenv("DBT_ENV_SECRET_TOKEN", "should-not-appear")

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--use-shell-env-vars",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        # No ENV_VARS=() clause emitted; nothing to forward.
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name args='run'"
        )
        assert "dropped 1 DBT_ENV_SECRET_* environment variable(s)" in result.output
        assert "no DBT_* environment variables found in shell" in result.output
        assert "forwarded" not in result.output

    def test_use_shell_env_vars_empty_shell(
        self, mock_connect, mock_cursor, runner, clean_dbt_env
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
                "--use-shell-env-vars",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name args='run'"
        )
        assert "no DBT_* environment variables found in shell" in result.output
        assert "bash/zsh:" in result.output
        assert "fish:" in result.output
        assert "PowerShell:" in result.output
        assert "cmd.exe:" in result.output
        assert "sudo -E" in result.output

    def test_use_shell_env_vars_explicit_overrides_shell(
        self, mock_connect, mock_cursor, runner, clean_dbt_env
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor
        clean_dbt_env.setenv("DBT_FOO", "fromshell")

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--use-shell-env-vars",
                "--env-vars",
                '{"DBT_FOO": "explicit"}',
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query() == "EXECUTE DBT PROJECT pipeline_name "
            "ENV_VARS=('DBT_FOO'='explicit') args='run'"
        )

    def test_use_shell_env_vars_merge_with_explicit(
        self, mock_connect, mock_cursor, runner, clean_dbt_env
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor
        clean_dbt_env.setenv("DBT_FOO", "fromshell")
        clean_dbt_env.setenv("DBT_BAR", "fromshell")

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--use-shell-env-vars",
                "--env-vars",
                '{"DBT_BAR": "explicit", "DBT_NEW": "new"}',
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        # Shell side sorted (DBT_BAR, DBT_FOO); --env-vars merges on top:
        # DBT_BAR is overwritten in place; DBT_NEW appended at end.
        assert (
            mock_connect.mocked_ctx.get_query() == "EXECUTE DBT PROJECT pipeline_name "
            "ENV_VARS=('DBT_BAR'='explicit', 'DBT_FOO'='fromshell', "
            "'DBT_NEW'='new') args='run'"
        )

    def test_use_shell_env_vars_with_no_env(
        self, mock_connect, mock_cursor, runner, clean_dbt_env
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor
        clean_dbt_env.setenv("DBT_FOO", "1")

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--env=NO_ENV",
                "--use-shell-env-vars",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name ENVIRONMENT='NO_ENV' "
            "ENV_VARS=('DBT_FOO'='1') args='run'"
        )

    def test_use_shell_env_vars_value_with_single_quote(
        self, mock_connect, mock_cursor, runner, clean_dbt_env
    ):
        cursor = mock_cursor(
            rows=[(True, "very detailed logs")],
            columns=[RESULT_COLUMN_NAME, OUTPUT_COLUMN_NAME],
        )
        mock_connect.mocked_ctx.cs = cursor
        clean_dbt_env.setenv("DBT_MSG", "it's")

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--use-shell-env-vars",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query() == "EXECUTE DBT PROJECT pipeline_name "
            "ENV_VARS=('DBT_MSG'='it''s') args='run'"
        )

    def test_use_shell_env_vars_async(self, mock_connect, runner, clean_dbt_env):
        clean_dbt_env.setenv("DBT_FOO", "1")

        result = runner.invoke(
            [
                "dbt",
                "execute",
                "--run-async",
                "--use-shell-env-vars",
                "pipeline_name",
                "compile",
            ]
        )

        assert result.exit_code == 0, result.output
        assert mock_connect.mocked_ctx.kwargs[0]["_exec_async"] is True
        assert (
            mock_connect.mocked_ctx.get_query() == "EXECUTE DBT PROJECT pipeline_name "
            "ENV_VARS=('DBT_FOO'='1') args='compile'"
        )

    def test_dbt_execute_with_dotted_prerelease_version(
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
                "--dbt-version=2.0.0-preview.175",
                "pipeline_name",
                "run",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == "EXECUTE DBT PROJECT pipeline_name dbt_version='2.0.0-preview.175' args='run'"
        )
