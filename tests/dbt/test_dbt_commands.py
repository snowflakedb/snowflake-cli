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
from snowflake.cli._plugins.dbt.manager import DBTObjectEditableAttributes
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
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
    @staticmethod
    def _get_default_attribute_dict() -> DBTObjectEditableAttributes:
        return {"default_target": None}

    @pytest.fixture(autouse=True)
    def mock_validate_role(self):
        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager._validate_role",
            return_value=True,
        ) as _fixture:
            yield _fixture

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
                        "target": "local",
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
                            },
                            "prod": {
                                "account": "test_account",
                                "database": "testdb_prod",
                                "role": "test_role",
                                "schema": "test_schema",
                                "threads": 2,
                                "type": "snowflake",
                                "user": "test_user",
                                "warehouse": "test_warehouse",
                            },
                        },
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
    def mock_get_dbt_object_attributes(self):
        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager.get_dbt_object_attributes",
            return_value=None,
        ) as _fixture:
            yield _fixture

    @pytest.fixture
    def mock_from_resource(self):
        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.FQN.from_resource",
            return_value="@MockDatabase.MockSchema.DBT_PROJECT_TEST_PIPELINE_1757333281_STAGE",
        ) as _fixture:
            yield _fixture

    @pytest.fixture
    def mock_deploy(self):
        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager.deploy"
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
        mock_get_dbt_object_attributes,
        mock_from_resource,
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
            f"CREATE DBT PROJECT TEST_PIPELINE\nFROM {mock_from_resource()}"
            in mock_connect.mocked_ctx.get_query()
        )
        mock_create.assert_called_once_with(mock_from_resource(), temporary=True)
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
        mock_get_dbt_object_attributes,
        mock_from_resource,
    ):
        mock_get_dbt_object_attributes.return_value = self._get_default_attribute_dict()

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
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in mock_connect.mocked_ctx.get_query()
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli.api.identifiers.time.time", return_value=1234567890)
    def test_deploys_project_with_case_sensitive_name(
        self,
        mock_time,
        mock_create,
        mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_get_dbt_object_attributes,
    ):

        result = runner.invoke(
            [
                "dbt",
                "deploy",
                '"MockDaTaBaSe"."PuBlIc"."caseSenSITIVEnAME"',
                f"--source={dbt_project_path}",
            ]
        )

        assert result.exit_code == 0, result.output
        assert (
            mock_connect.mocked_ctx.get_query()
            == f"""CREATE DBT PROJECT "MockDaTaBaSe"."PuBlIc"."caseSenSITIVEnAME"
FROM @MockDatabase.MockSchema.DBT_PROJECT_caseSenSITIVEnAME_{mock_time()}_STAGE"""
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
        mock_get_dbt_object_attributes,
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
    @mock.patch("snowflake.cli.api.identifiers.time.time", return_value=1234567890)
    def test_deploys_project_with_fqn_uses_name_only_for_stage(
        self,
        mock_time,
        mock_create,
        mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_get_dbt_object_attributes,
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
        # Verify stage creation uses only the name part of the FQN
        assert (
            mock_connect.mocked_ctx.get_query()
            == f"""CREATE DBT PROJECT MockDatabase.MockSchema.test_dbt_project
FROM @MockDatabase.MockSchema.DBT_PROJECT_TEST_DBT_PROJECT_{mock_time()}_STAGE"""
        )

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
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
        mock_deploy.assert_called_once_with(
            FQN.from_string("TEST_PIPELINE"),
            SecurePath(dbt_project_path),
            SecurePath(dbt_project_path),
            force=False,
            default_target=None,
            unset_default_target=False,
            external_access_integrations=["google_apis_access_integration"],
        )

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
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
        mock_deploy.assert_called_once_with(
            FQN.from_string("TEST_PIPELINE"),
            SecurePath(dbt_project_path),
            SecurePath(dbt_project_path),
            force=False,
            default_target=None,
            unset_default_target=False,
            external_access_integrations=["google_apis_access_integration", "dbt_hub"],
        )

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

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploy_with_default_target(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_from_resource,
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
        assert (
            f"CREATE DBT PROJECT TEST_PIPELINE\nFROM {mock_from_resource()} DEFAULT_TARGET='prod'"
            in mock_connect.mocked_ctx.get_query()
        )

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploy_with_invalid_default_target(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_get_dbt_object_attributes,
    ):
        result = runner.invoke(
            [
                "dbt",
                "deploy",
                "TEST_PIPELINE",
                f"--source={dbt_project_path}",
                "--default-target=invalid",
            ]
        )

        assert result.exit_code == 1, result.output
        assert "Target 'invalid' is not defined" in result.output
        assert mock_connect.mocked_ctx.get_query() == ""

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploy_existing_project_with_default_target(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_from_resource,
    ):
        mock_get_dbt_object_attributes.return_value = {"default_target": "dev"}

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
        queries = mock_connect.mocked_ctx.get_queries()
        assert (
            len(queries) == 2
        )  # ADD VERSION and SET DEFAULT_TARGET (get_current_default_target is mocked)
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in queries[0]
        )
        assert "ALTER DBT PROJECT TEST_PIPELINE SET DEFAULT_TARGET='prod'" == queries[1]

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploy_existing_project_with_same_default_target(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_from_resource,
    ):
        mock_get_dbt_object_attributes.return_value = {"default_target": "prod"}

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
        # Should have only one query: ADD VERSION (no SET DEFAULT_TARGET because it's already correct)
        query = mock_connect.mocked_ctx.get_query()
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in query
        )
        assert "SET DEFAULT_TARGET" not in query

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploy_with_unset_default_target_when_project_exists_with_target(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_from_resource,
    ):
        mock_get_dbt_object_attributes.return_value = {"default_target": "prod"}

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
        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in queries[0]
        )
        assert "ALTER DBT PROJECT TEST_PIPELINE UNSET DEFAULT_TARGET" == queries[1]

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    def test_deploy_with_unset_default_target_when_project_exists_without_target(
        self,
        _mock_create,
        _mock_put_recursive,
        mock_connect,
        runner,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_from_resource,
    ):
        mock_get_dbt_object_attributes.return_value = {"default_target": None}

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
        query = mock_connect.mocked_ctx.get_query()
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in query
        )
        assert "UNSET DEFAULT_TARGET" not in query

    @with_feature_flags({FeatureFlag.ENABLE_DBT_GA_FEATURES: True})
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
