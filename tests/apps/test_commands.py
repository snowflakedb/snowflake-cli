# Copyright (c) 2026 Snowflake Inc.
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

from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.apps.generate import _generate_snowflake_yml
from snowflake.cli._plugins.apps.manager import (
    SNOWFLAKE_APP_ENTITY_TYPE,
    SnowflakeAppManager,
    _check_feature_enabled,
    _get_compute_pool,
    _get_entity,
    _get_external_access,
    _get_snowflake_app_entities,
    _object_exists,
    _resolve_entity_id,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag

from tests_common.feature_flag_utils import with_feature_flags

EXECUTE_QUERY = "snowflake.cli._plugins.apps.manager.SnowflakeAppManager.execute_query"
OBJECT_EXISTS = "snowflake.cli._plugins.apps.manager._object_exists"
GET_CLI_CONTEXT = "snowflake.cli._plugins.apps.manager.get_cli_context"
GET_ENV_USERNAME = "snowflake.cli._plugins.apps.generate.get_env_username"


# ── Feature flag tests ────────────────────────────────────────────────


class TestFeatureFlag:
    def test_feature_flag_disabled_by_default(self):
        assert FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled()

    def test_check_feature_enabled_raises_when_disabled(self):
        with pytest.raises(CliError, match="This feature is not available yet."):
            _check_feature_enabled()

    def test_check_feature_enabled_succeeds_when_enabled(self):
        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            _check_feature_enabled()  # Should not raise

    def test_apps_command_hidden_by_default(self, runner):
        result = runner.invoke(["--help"])
        assert result.exit_code == 0
        assert "apps" not in result.output


# ── Helper function tests ─────────────────────────────────────────────


class TestObjectExists:
    @patch("snowflake.cli._plugins.apps.manager.ObjectManager")
    def test_returns_true_when_exists(self, mock_object_manager):
        mock_object_manager().object_exists.return_value = True
        assert _object_exists("compute-pool", "MY_POOL") is True

    @patch("snowflake.cli._plugins.apps.manager.ObjectManager")
    def test_returns_false_when_not_exists(self, mock_object_manager):
        mock_object_manager().object_exists.return_value = False
        assert _object_exists("compute-pool", "MY_POOL") is False

    @patch("snowflake.cli._plugins.apps.manager.ObjectManager")
    def test_returns_false_on_exception(self, mock_object_manager):
        mock_object_manager().object_exists.side_effect = Exception("error")
        assert _object_exists("compute-pool", "MY_POOL") is False


class TestGetComputePool:
    @patch(OBJECT_EXISTS)
    def test_returns_default_pool_when_exists(self, mock_exists):
        mock_exists.return_value = True
        result = _get_compute_pool()
        assert result == "SNOW_APPS_DEFAULT_COMPUTE_POOL"

    @patch(OBJECT_EXISTS)
    def test_returns_none_when_no_pool_exists(self, mock_exists):
        mock_exists.return_value = False
        result = _get_compute_pool()
        assert result is None


class TestGetExternalAccess:
    @patch(OBJECT_EXISTS)
    def test_returns_default_eai_when_exists(self, mock_exists):
        mock_exists.return_value = True
        result = _get_external_access("my_app")
        assert result == "SNOW_APPS_DEFAULT_EXTERNAL_ACCESS"

    @patch(OBJECT_EXISTS)
    def test_returns_app_specific_eai_when_default_not_found(self, mock_exists):
        mock_exists.side_effect = [False, True]
        result = _get_external_access("my_app")
        assert result == "SNOW_APPS_MY_APP_EXTERNAL_ACCESS"

    @patch(OBJECT_EXISTS)
    def test_returns_none_when_no_eai_exists(self, mock_exists):
        mock_exists.return_value = False
        result = _get_external_access("my_app")
        assert result is None


class TestGetSnowflakeAppEntities:
    @patch(GET_CLI_CONTEXT)
    def test_raises_when_no_project_def(self, mock_ctx):
        mock_ctx().project_definition = None
        with pytest.raises(CliError, match="No snowflake.yml found"):
            _get_snowflake_app_entities()

    @patch(GET_CLI_CONTEXT)
    def test_returns_empty_dict_when_no_entities(self, mock_ctx):
        mock_project_def = Mock()
        mock_project_def.entities = {}
        mock_ctx().project_definition = mock_project_def
        result = _get_snowflake_app_entities()
        assert result == {}

    @patch(GET_CLI_CONTEXT)
    def test_returns_snowflake_app_entities_only(self, mock_ctx):
        app_entity = Mock()
        app_entity.type = SNOWFLAKE_APP_ENTITY_TYPE

        other_entity = Mock()
        other_entity.type = "streamlit"

        mock_project_def = Mock()
        mock_project_def.entities = {
            "my_app": app_entity,
            "my_streamlit": other_entity,
        }
        mock_ctx().project_definition = mock_project_def

        result = _get_snowflake_app_entities()
        assert "my_app" in result
        assert "my_streamlit" not in result


class TestResolveEntityId:
    def test_returns_provided_id(self):
        result = _resolve_entity_id("my_app")
        assert result == "my_app"

    @patch(
        "snowflake.cli._plugins.apps.manager._get_snowflake_app_entities",
        return_value={},
    )
    def test_raises_when_no_entities(self, _):
        with pytest.raises(CliError, match="No snowflake-app entities found"):
            _resolve_entity_id(None)

    @patch(
        "snowflake.cli._plugins.apps.manager._get_snowflake_app_entities",
        return_value={"my_app": Mock()},
    )
    def test_auto_resolves_single_entity(self, _):
        result = _resolve_entity_id(None)
        assert result == "my_app"

    @patch(
        "snowflake.cli._plugins.apps.manager._get_snowflake_app_entities",
        return_value={"app_1": Mock(), "app_2": Mock()},
    )
    def test_raises_when_multiple_entities(self, _):
        with pytest.raises(CliError, match="Multiple snowflake-app entities found"):
            _resolve_entity_id(None)


class TestGetEntity:
    @patch(
        "snowflake.cli._plugins.apps.manager._get_snowflake_app_entities",
    )
    def test_returns_entity(self, mock_get):
        entity = Mock()
        mock_get.return_value = {"my_app": entity}
        result = _get_entity("my_app")
        assert result is entity

    @patch(
        "snowflake.cli._plugins.apps.manager._get_snowflake_app_entities",
        return_value={},
    )
    def test_raises_when_not_found(self, _):
        with pytest.raises(CliError, match="Entity 'my_app' not found"):
            _get_entity("my_app")


# ── _generate_snowflake_yml tests ─────────────────────────────────────


class TestGenerateSnowflakeYml:
    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_generates_yml_no_compute_pool_no_eai(self, mock_user, mock_exists):
        result = _generate_snowflake_yml("my_app", "TEST_WH", "TEST_DB")
        assert "type: snowflake-app" in result
        assert "name: MY_APP" in result
        assert "database: TEST_DB" in result
        assert "schema: SNOW_APP_MY_APP_TESTUSER" in result
        assert "query_warehouse: TEST_WH" in result
        assert "build_compute_pool:" in result
        assert "name: null" in result
        assert "name: MY_APP_CODE" in result

    @patch(OBJECT_EXISTS, return_value=True)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_generates_yml_with_compute_pool_and_eai(self, mock_user, mock_exists):
        result = _generate_snowflake_yml("my_app", "TEST_WH", "TEST_DB")
        assert "name: SNOW_APPS_DEFAULT_COMPUTE_POOL" in result
        assert "name: SNOW_APPS_DEFAULT_EXTERNAL_ACCESS" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_generates_yml_default_database_template(self, mock_user, mock_exists):
        result = _generate_snowflake_yml("my_app", "TEST_WH")
        assert "database: <% ctx.connection.database %>" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_generates_yml_default_warehouse_template(self, mock_user, mock_exists):
        result = _generate_snowflake_yml("my_app", None, "TEST_DB")
        assert "query_warehouse: <% ctx.connection.warehouse %>" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_generated_yml_is_valid_project_definition(self, mock_user, mock_exists):
        """Generated YAML is parsable and produces a valid project definition."""
        import yaml
        from snowflake.cli.api.utils.definition_rendering import (
            render_definition_template,
        )

        # Use concrete values (no template placeholders) so parsing succeeds
        raw_yml = _generate_snowflake_yml("my_app", "TEST_WH", "TEST_DB")
        definition_input = yaml.safe_load(raw_yml)

        result = render_definition_template(definition_input, {})
        project = result.project_definition
        entity = project.entities["my_app"]

        assert entity.type == "snowflake-app"
        assert entity.query_warehouse == "TEST_WH"
        assert entity.code_stage.name == "MY_APP_CODE"
        assert entity.artifacts is not None


# ── SnowflakeAppManager tests ─────────────────────────────────────────


class TestSnowflakeAppManager:
    @patch(EXECUTE_QUERY)
    def test_create_schema_if_not_exists(self, mock_execute):
        SnowflakeAppManager().create_schema_if_not_exists("TEST_DB", "TEST_SCHEMA")
        mock_execute.assert_called_once_with(
            'CREATE SCHEMA IF NOT EXISTS "TEST_DB"."TEST_SCHEMA"'
        )

    @patch(EXECUTE_QUERY)
    def test_stage_exists_returns_true(self, mock_execute):
        assert SnowflakeAppManager().stage_exists("DB.SCHEMA.STAGE") is True
        mock_execute.assert_called_once_with("DESCRIBE STAGE DB.SCHEMA.STAGE")

    @patch(EXECUTE_QUERY, side_effect=Exception("not found"))
    def test_stage_exists_returns_false(self, mock_execute):
        assert SnowflakeAppManager().stage_exists("DB.SCHEMA.STAGE") is False

    @patch(EXECUTE_QUERY)
    def test_clear_stage(self, mock_execute):
        SnowflakeAppManager().clear_stage("DB.SCHEMA.STAGE")
        mock_execute.assert_called_once_with("REMOVE @DB.SCHEMA.STAGE")

    @patch(EXECUTE_QUERY)
    def test_create_stage(self, mock_execute):
        SnowflakeAppManager().create_stage("DB.SCHEMA.STAGE")
        mock_execute.assert_called_once_with(
            "CREATE STAGE IF NOT EXISTS DB.SCHEMA.STAGE ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
        )

    @patch(EXECUTE_QUERY)
    def test_create_stage_custom_encryption(self, mock_execute):
        SnowflakeAppManager().create_stage("DB.SCHEMA.STAGE", "SNOWFLAKE_FULL")
        mock_execute.assert_called_once_with(
            "CREATE STAGE IF NOT EXISTS DB.SCHEMA.STAGE ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL')"
        )

    @patch(EXECUTE_QUERY)
    def test_drop_service_if_exists(self, mock_execute):
        SnowflakeAppManager().drop_service_if_exists("DB.SCHEMA.SVC")
        mock_execute.assert_called_once_with("DROP SERVICE IF EXISTS DB.SCHEMA.SVC")

    @patch(EXECUTE_QUERY)
    def test_get_image_repo_url(self, mock_execute):
        cursor = Mock()
        cursor.rowcount = 1
        cursor.fetchall.return_value = [
            {
                "name": "MY_REPO",
                "repository_url": "host.registry.snowflakecomputing.com/db/schema/my_repo",
            }
        ]
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().get_image_repo_url("MY_REPO")
        assert result == "host.registry-local.snowflakecomputing.com/db/schema/my_repo"

    @patch(EXECUTE_QUERY)
    def test_get_image_repo_url_dev_registry(self, mock_execute):
        cursor = Mock()
        cursor.rowcount = 1
        cursor.fetchall.return_value = [
            {
                "name": "MY_REPO",
                "repository_url": "host.registry-dev.snowflakecomputing.com/db/schema/my_repo",
            }
        ]
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().get_image_repo_url("MY_REPO")
        assert result == "host.registry-local.snowflakecomputing.com/db/schema/my_repo"

    @patch(EXECUTE_QUERY)
    def test_get_image_repo_url_qa_environment(self, mock_execute):
        cursor = Mock()
        cursor.rowcount = 1
        cursor.fetchall.return_value = [
            {
                "name": "MY_REPO",
                "repository_url": "host.awsuswest2qa6.registry.snowflakecomputing.com/db/schema/my_repo",
            }
        ]
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().get_image_repo_url("MY_REPO")
        assert result == "host.registry-local.snowflakecomputing.com/db/schema/my_repo"

    @patch(EXECUTE_QUERY)
    def test_get_image_repo_url_not_found_empty(self, mock_execute):
        cursor = Mock()
        cursor.rowcount = 0
        mock_execute.return_value = cursor

        with pytest.raises(CliError, match="Image repository 'MY_REPO' not found"):
            SnowflakeAppManager().get_image_repo_url("MY_REPO")

    @patch(EXECUTE_QUERY)
    def test_get_image_repo_url_not_found_none_rowcount(self, mock_execute):
        cursor = Mock()
        cursor.rowcount = None
        mock_execute.return_value = cursor

        with pytest.raises(CliError, match="Image repository 'MY_REPO' not found"):
            SnowflakeAppManager().get_image_repo_url("MY_REPO")

    @patch(EXECUTE_QUERY)
    def test_execute_build_job_without_eai(self, mock_execute):
        SnowflakeAppManager().execute_build_job(
            job_service_name="BUILD_JOB",
            compute_pool="BUILD_POOL",
            code_stage="DB.SCHEMA.STAGE",
            image_repo_url="host/db/schema/repo",
            app_id="my_app",
        )
        mock_execute.assert_called_once()
        query = mock_execute.call_args[0][0]
        assert "EXECUTE JOB SERVICE IN COMPUTE POOL BUILD_POOL" in query
        assert "NAME = BUILD_JOB" in query
        assert "ASYNC = TRUE" in query
        assert 'IMAGE_REGISTRY_URL: "host/db/schema/repo"' in query
        assert 'IMAGE_NAME: "my_app"' in query
        assert "EXTERNAL_ACCESS_INTEGRATIONS" not in query

    @patch(EXECUTE_QUERY)
    def test_execute_build_job_with_eai(self, mock_execute):
        SnowflakeAppManager().execute_build_job(
            job_service_name="BUILD_JOB",
            compute_pool="BUILD_POOL",
            code_stage="DB.SCHEMA.STAGE",
            image_repo_url="host/db/schema/repo",
            app_id="my_app",
            external_access_integration="MY_EAI",
        )
        mock_execute.assert_called_once()
        query = mock_execute.call_args[0][0]
        assert "EXTERNAL_ACCESS_INTEGRATIONS = (MY_EAI)" in query

    @patch(EXECUTE_QUERY)
    def test_get_build_status_done(self, mock_execute):
        result_cursor = Mock()
        result_cursor.fetchone.return_value = (1, "DONE")
        mock_execute.return_value = result_cursor

        status = SnowflakeAppManager().get_build_status("DB", "SCHEMA", "BUILD_JOB")
        assert status == "DONE"
        assert mock_execute.call_count == 2  # SHOW SERVICES + SELECT

    @patch(EXECUTE_QUERY)
    def test_get_build_status_idle(self, mock_execute):
        result_cursor = Mock()
        result_cursor.fetchone.return_value = (0, None)
        mock_execute.return_value = result_cursor

        status = SnowflakeAppManager().get_build_status("DB", "SCHEMA", "BUILD_JOB")
        assert status == "IDLE"

    @patch(EXECUTE_QUERY)
    def test_get_build_status_no_row(self, mock_execute):
        result_cursor = Mock()
        result_cursor.fetchone.return_value = None
        mock_execute.return_value = result_cursor

        status = SnowflakeAppManager().get_build_status("DB", "SCHEMA", "BUILD_JOB")
        assert status == "IDLE"

    @patch(EXECUTE_QUERY)
    def test_create_service_basic(self, mock_execute):
        SnowflakeAppManager().create_service(
            service_name="DB.SCHEMA.SVC",
            compute_pool="SVC_POOL",
            query_warehouse="WH",
        )
        # Should call: CREATE SERVICE, ALTER SERVICE SUSPEND (no comment)
        assert mock_execute.call_count == 2
        create_query = mock_execute.call_args_list[0][0][0]
        assert "CREATE SERVICE IF NOT EXISTS DB.SCHEMA.SVC" in create_query
        assert "IN COMPUTE POOL SVC_POOL" in create_query
        assert "QUERY_WAREHOUSE = WH" in create_query
        suspend_query = mock_execute.call_args_list[1][0][0]
        assert "ALTER SERVICE DB.SCHEMA.SVC SUSPEND" in suspend_query

    @patch(EXECUTE_QUERY)
    def test_create_service_with_comment(self, mock_execute):
        SnowflakeAppManager().create_service(
            service_name="DB.SCHEMA.SVC",
            compute_pool="SVC_POOL",
            query_warehouse="WH",
            app_comment='{"appId": "MY_APP"}',
        )
        # Should call: CREATE SERVICE, ALTER SET COMMENT, ALTER SUSPEND
        assert mock_execute.call_count == 3
        comment_query = mock_execute.call_args_list[1][0][0]
        assert "ALTER SERVICE DB.SCHEMA.SVC SET COMMENT" in comment_query
        assert '{"appId": "MY_APP"}' in comment_query

    @patch(EXECUTE_QUERY)
    def test_create_service_escapes_comment_quotes(self, mock_execute):
        SnowflakeAppManager().create_service(
            service_name="DB.SCHEMA.SVC",
            compute_pool="SVC_POOL",
            query_warehouse="WH",
            app_comment="it's a test",
        )
        comment_query = mock_execute.call_args_list[1][0][0]
        assert "it''s a test" in comment_query

    @patch(EXECUTE_QUERY)
    def test_alter_service_spec(self, mock_execute):
        SnowflakeAppManager().alter_service_spec(
            service_name="DB.SCHEMA.SVC",
            image_url="/db/schema/repo/my_app:latest",
        )
        mock_execute.assert_called_once()
        query = mock_execute.call_args[0][0]
        assert "ALTER SERVICE DB.SCHEMA.SVC" in query
        assert "/db/schema/repo/my_app:latest" in query

    @patch(EXECUTE_QUERY)
    def test_resume_service(self, mock_execute):
        SnowflakeAppManager().resume_service("DB.SCHEMA.SVC")
        mock_execute.assert_called_once_with("ALTER SERVICE DB.SCHEMA.SVC RESUME")

    @patch(EXECUTE_QUERY)
    def test_get_service_status_running(self, mock_execute):
        result_cursor = Mock()
        result_cursor.fetchone.return_value = (1, "RUNNING")
        mock_execute.return_value = result_cursor

        status = SnowflakeAppManager().get_service_status("DB", "SCHEMA", "SVC")
        assert status == "RUNNING"

    @patch(EXECUTE_QUERY)
    def test_get_service_status_idle(self, mock_execute):
        result_cursor = Mock()
        result_cursor.fetchone.return_value = (0, None)
        mock_execute.return_value = result_cursor

        status = SnowflakeAppManager().get_service_status("DB", "SCHEMA", "SVC")
        assert status == "IDLE"

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url(self, mock_execute):
        result_cursor = Mock()
        result_cursor.fetchone.return_value = (
            "https://my-endpoint.snowflakecomputing.app",
        )
        mock_execute.return_value = result_cursor

        url = SnowflakeAppManager().get_service_endpoint_url("DB.SCHEMA.SVC")
        assert url == "https://my-endpoint.snowflakecomputing.app"
        # Should call SHOW ENDPOINTS then SELECT
        assert mock_execute.call_count == 2

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url_not_found(self, mock_execute):
        result_cursor = Mock()
        result_cursor.fetchone.return_value = None
        mock_execute.return_value = result_cursor

        url = SnowflakeAppManager().get_service_endpoint_url("DB.SCHEMA.SVC")
        assert url is None


# ── CLI command tests ─────────────────────────────────────────────────


class TestInitCommand:
    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    def test_init_creates_file(self, mock_gen, runner, tmp_path):
        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["apps", "init", "--app-name", "my_app"])
                assert result.exit_code == 0, result.output
                assert "Initialized Snowflake App project" in result.output
                assert (tmp_path / "snowflake.yml").exists()

    def test_init_skips_when_file_exists(self, runner, tmp_path):
        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            (tmp_path / "snowflake.yml").write_text("existing content")
            with change_directory(tmp_path):
                result = runner.invoke(["apps", "init", "--app-name", "my_app"])
                assert result.exit_code == 0, result.output
                assert "already exists" in result.output

    def test_init_fails_when_feature_disabled(self, runner, tmp_path):
        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(["apps", "init", "--app-name", "my_app"])
            assert result.exit_code == 1
            assert "not available" in result.output


class TestDeployCommand:
    def test_deploy_fails_when_feature_disabled(self, runner, tmp_path):
        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(["apps", "deploy"])
            assert result.exit_code == 1
            assert "not available" in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_fails_missing_build_compute_pool(
        self, mock_resolve, mock_get_entity, runner, tmp_path
    ):
        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA")
        entity.code_stage = None
        entity.artifacts = []
        entity.build_compute_pool = None
        entity.service_compute_pool = Mock()
        entity.service_compute_pool.name = "SVC_POOL"
        entity.query_warehouse = "WH"
        entity.build_eai = None
        entity.meta = None
        mock_get_entity.return_value = entity

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["apps", "deploy"])
                assert result.exit_code == 1
                assert "build_compute_pool is required" in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_fails_missing_service_compute_pool(
        self, mock_resolve, mock_get_entity, runner, tmp_path
    ):
        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA")
        entity.code_stage = None
        entity.artifacts = []
        entity.build_compute_pool = Mock()
        entity.build_compute_pool.name = "BUILD_POOL"
        entity.service_compute_pool = None
        entity.query_warehouse = "WH"
        entity.build_eai = None
        entity.meta = None
        mock_get_entity.return_value = entity

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["apps", "deploy"])
                assert result.exit_code == 1
                assert "service_compute_pool is required" in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_fails_missing_query_warehouse(
        self, mock_resolve, mock_get_entity, runner, tmp_path
    ):
        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA")
        entity.code_stage = None
        entity.artifacts = []
        entity.build_compute_pool = Mock()
        entity.build_compute_pool.name = "BUILD_POOL"
        entity.service_compute_pool = Mock()
        entity.service_compute_pool.name = "SVC_POOL"
        entity.query_warehouse = None
        entity.build_eai = None
        entity.meta = None
        mock_get_entity.return_value = entity

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["apps", "deploy"])
                assert result.exit_code == 1
                assert "query_warehouse is required" in result.output
