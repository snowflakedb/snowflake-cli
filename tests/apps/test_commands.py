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
from snowflake.cli._plugins.apps.generate import (
    _generate_snowflake_yml,
)
from snowflake.cli._plugins.apps.manager import (
    SNOWFLAKE_APP_ENTITY_TYPE,
    SnowflakeAppManager,
    _get_compute_pool,
    _get_entity,
    _get_external_access,
    _get_snowflake_app_entities,
    _object_exists,
    _poll_until,
    _resolve_entity_id,
    perform_bundle,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN

from tests_common.feature_flag_utils import with_feature_flags

EXECUTE_QUERY = "snowflake.cli._plugins.apps.manager.SnowflakeAppManager.execute_query"
OBJECT_EXISTS = "snowflake.cli._plugins.apps.manager._object_exists"
GET_CLI_CONTEXT = "snowflake.cli._plugins.apps.manager.get_cli_context"
GET_ENV_USERNAME = "snowflake.cli._plugins.apps.generate.get_env_username"


# ── Feature flag tests ────────────────────────────────────────────────


class TestFeatureFlag:
    def test_feature_flag_disabled_by_default(self):
        assert FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled()

    def test_apps_command_hidden_by_default(self, runner):
        result = runner.invoke(["--help"])
        assert result.exit_code == 0
        assert "__app" not in result.output


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
        from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
            SnowflakeAppEntityModel,
        )

        entity = Mock(spec=SnowflakeAppEntityModel)
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


# ── _poll_until tests ─────────────────────────────────────────────────


class TestPollUntilPredicateMode:
    """Tests for the predicate-based (is_done / is_error) mode of _poll_until."""

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_is_done_predicate_returns_value(self, mock_sleep):
        result = _poll_until(
            poll_fn=lambda: "https://my-app.snowflakecomputing.app",
            is_done=lambda url: url is not None
            and "provisioning in progress" not in url.lower(),
            timeout_message="timed out",
        )
        assert result == "https://my-app.snowflakecomputing.app"

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_is_done_with_none_then_provisioning_then_ready(self, mock_sleep):
        values = iter(
            [
                None,
                "Provisioning in progress",
                "https://my-app.snowflakecomputing.app",
            ]
        )
        result = _poll_until(
            poll_fn=lambda: next(values),
            is_done=lambda url: url is not None
            and "provisioning in progress" not in url.lower(),
            format_status=lambda url: url or "not yet available",
            timeout_message="timed out",
        )
        assert result == "https://my-app.snowflakecomputing.app"
        assert mock_sleep.call_count == 3

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_is_error_predicate_raises_cli_error(self, mock_sleep):
        with pytest.raises(CliError, match="something failed"):
            _poll_until(
                poll_fn=lambda: "ERROR_STATE",
                is_done=lambda v: v == "READY",
                is_error=lambda v: v.startswith("ERROR"),
                timeout_message="something failed",
            )

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_predicate_mode_timeout(self, mock_sleep):
        with pytest.raises(CliError, match="timed out"):
            _poll_until(
                poll_fn=lambda: None,
                is_done=lambda v: v is not None,
                max_attempts=3,
                interval_seconds=1,
                timeout_message="timed out",
            )
        assert mock_sleep.call_count == 3

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_format_status_used_in_predicate_mode(self, mock_sleep, capsys):
        values = iter([None, "https://ready.app"])
        _poll_until(
            poll_fn=lambda: next(values),
            is_done=lambda url: url is not None and url.startswith("https://"),
            format_status=lambda url: url or "waiting...",
            max_attempts=5,
            timeout_message="timed out",
        )
        # Just verify it completes without error; the format_status lambda
        # is exercised by the cli_console.step call inside _poll_until.
        assert mock_sleep.call_count == 2


class TestPollUntilStateSetMode:
    """Verify the original state-set mode still works after refactoring."""

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_done_state_returns(self, mock_sleep):
        result = _poll_until(
            poll_fn=lambda: "DONE",
            done_states={"DONE"},
            error_states={"FAILED"},
            known_pending_states={"PENDING"},
            timeout_message="timed out",
        )
        assert result == "DONE"

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_error_state_raises(self, mock_sleep):
        with pytest.raises(CliError, match="timed out"):
            _poll_until(
                poll_fn=lambda: "FAILED",
                done_states={"DONE"},
                error_states={"FAILED"},
                known_pending_states={"PENDING"},
                timeout_message="timed out",
            )

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_unknown_state_returns_early(self, mock_sleep):
        result = _poll_until(
            poll_fn=lambda: "UNKNOWN",
            done_states={"DONE"},
            error_states={"FAILED"},
            known_pending_states={"PENDING"},
            timeout_message="timed out",
        )
        assert result == "UNKNOWN"

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_pending_then_done(self, mock_sleep):
        values = iter(["PENDING", "PENDING", "DONE"])
        result = _poll_until(
            poll_fn=lambda: next(values),
            done_states={"DONE"},
            error_states={"FAILED"},
            known_pending_states={"PENDING"},
            timeout_message="timed out",
        )
        assert result == "DONE"
        assert mock_sleep.call_count == 3

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_state_set_timeout(self, mock_sleep):
        with pytest.raises(CliError, match="timed out"):
            _poll_until(
                poll_fn=lambda: "PENDING",
                done_states={"DONE"},
                error_states={"FAILED"},
                known_pending_states={"PENDING"},
                max_attempts=2,
                timeout_message="timed out",
            )
        assert mock_sleep.call_count == 2


# ── _generate_snowflake_yml tests ─────────────────────────────────────


class TestGenerateSnowflakeYml:
    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_generates_yml_no_compute_pool_no_eai(self, mock_user, mock_exists):
        result = _generate_snowflake_yml("my_app", "TEST_WH", "TEST_DB")
        assert "type: snowflake-app" in result
        assert "name: MY_APP" in result
        assert "database: TEST_DB" in result
        assert "schema: SNOW_APPS" in result
        assert "query_warehouse: TEST_WH" in result
        assert "build_compute_pool:" in result
        assert "name: null" in result
        assert "name: MY_APP_CODE" in result
        assert "artifact_repository" not in result
        assert "image_repository" not in result

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
        assert "database: null" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_generates_yml_default_warehouse_none(self, mock_user, mock_exists):
        result = _generate_snowflake_yml("my_app", None, "TEST_DB")
        assert "query_warehouse: null" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_config_overrides_fill_missing_database(self, mock_user, mock_exists):
        result = _generate_snowflake_yml(
            "my_app", "TEST_WH", config_overrides={"database": "CFG_DB"}
        )
        assert "database: CFG_DB" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_config_overrides_fill_missing_warehouse(self, mock_user, mock_exists):
        result = _generate_snowflake_yml(
            "my_app", None, config_overrides={"warehouse": "CFG_WH"}
        )
        assert "query_warehouse: CFG_WH" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_explicit_values_beat_config_overrides(self, mock_user, mock_exists):
        result = _generate_snowflake_yml(
            "my_app",
            "EXPLICIT_WH",
            "EXPLICIT_DB",
            config_overrides={"warehouse": "CFG_WH", "database": "CFG_DB"},
        )
        assert "database: EXPLICIT_DB" in result
        assert "query_warehouse: EXPLICIT_WH" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_config_overrides_compute_pool_beats_builtin(self, mock_user, mock_exists):
        result = _generate_snowflake_yml(
            "my_app", "WH", "DB", config_overrides={"compute_pool": "CFG_POOL"}
        )
        assert "name: CFG_POOL" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_config_overrides_schema(self, mock_user, mock_exists):
        result = _generate_snowflake_yml(
            "my_app", "WH", "DB", config_overrides={"schema": "CFG_SCHEMA"}
        )
        assert "schema: CFG_SCHEMA" in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_repos_omitted_without_config_overrides(self, mock_user, mock_exists):
        result = _generate_snowflake_yml("my_app", "WH", "DB")
        assert "artifact_repository" not in result
        assert "image_repository" not in result

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(GET_ENV_USERNAME, return_value="testuser")
    def test_config_overrides_set_repos(self, mock_user, mock_exists):
        result = _generate_snowflake_yml(
            "my_app",
            "WH",
            "DB",
            config_overrides={
                "artifact_repository": "MY_AR",
                "image_repository": "MY_IR",
            },
        )
        assert "artifact_repository:" in result
        assert "name: MY_AR" in result
        assert "image_repository:" in result
        assert "name: MY_IR" in result

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
            "CREATE SCHEMA IF NOT EXISTS IDENTIFIER('TEST_DB.TEST_SCHEMA')"
        )

    @patch(EXECUTE_QUERY)
    def test_stage_exists_returns_true(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        assert SnowflakeAppManager().stage_exists(fqn) is True
        mock_execute.assert_called_once_with(
            "DESCRIBE STAGE IDENTIFIER('DB.SCHEMA.STAGE')"
        )

    @patch(EXECUTE_QUERY, side_effect=Exception("not found"))
    def test_stage_exists_returns_false(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        assert SnowflakeAppManager().stage_exists(fqn) is False

    @patch(EXECUTE_QUERY)
    def test_clear_stage(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().clear_stage(fqn)
        mock_execute.assert_called_once_with("REMOVE @DB.SCHEMA.STAGE")

    @patch(EXECUTE_QUERY)
    def test_create_stage(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().create_stage(fqn)
        mock_execute.assert_called_once_with(
            "CREATE STAGE IF NOT EXISTS IDENTIFIER('DB.SCHEMA.STAGE') ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
        )

    @patch(EXECUTE_QUERY)
    def test_create_stage_custom_encryption(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().create_stage(fqn, "SNOWFLAKE_FULL")
        mock_execute.assert_called_once_with(
            "CREATE STAGE IF NOT EXISTS IDENTIFIER('DB.SCHEMA.STAGE') ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL')"
        )

    @patch(EXECUTE_QUERY)
    def test_drop_service_if_exists(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().drop_service_if_exists(fqn)
        mock_execute.assert_called_once_with(
            "DROP SERVICE IF EXISTS IDENTIFIER('DB.SCHEMA.SVC')"
        )

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
    def test_get_image_repo_url_with_database_and_schema(self, mock_execute):
        cursor = Mock()
        cursor.rowcount = 1
        cursor.fetchall.return_value = [
            {
                "name": "MY_REPO",
                "repository_url": "host.registry.snowflakecomputing.com/db/schema/my_repo",
            }
        ]
        mock_execute.return_value = cursor

        SnowflakeAppManager().get_image_repo_url(
            "MY_REPO", database="CUSTOM_DB", schema="CUSTOM_SCHEMA"
        )
        query = mock_execute.call_args[0][0]
        assert "in schema IDENTIFIER('CUSTOM_DB.CUSTOM_SCHEMA')" in query

    @patch(EXECUTE_QUERY)
    def test_get_image_repo_url_with_database_only(self, mock_execute):
        cursor = Mock()
        cursor.rowcount = 1
        cursor.fetchall.return_value = [
            {
                "name": "MY_REPO",
                "repository_url": "host.registry.snowflakecomputing.com/db/schema/my_repo",
            }
        ]
        mock_execute.return_value = cursor

        SnowflakeAppManager().get_image_repo_url("MY_REPO", database="CUSTOM_DB")
        query = mock_execute.call_args[0][0]
        assert "in database IDENTIFIER('CUSTOM_DB')" in query

    def test_get_image_repo_url_schema_without_database_raises(self):
        with pytest.raises(CliError, match="image_repository.schema requires"):
            SnowflakeAppManager().get_image_repo_url("MY_REPO", schema="CUSTOM_SCHEMA")

    @patch(EXECUTE_QUERY)
    def test_execute_build_job_without_eai(self, mock_execute):
        job_fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().execute_build_job(
            job_service_name=job_fqn,
            compute_pool="BUILD_POOL",
            code_stage=stage_fqn,
            image_repo_url="host/db/schema/repo",
            app_id="my_app",
        )
        mock_execute.assert_called_once()
        query = mock_execute.call_args[0][0]
        assert "EXECUTE JOB SERVICE IN COMPUTE POOL BUILD_POOL" in query
        assert "NAME = IDENTIFIER('DB.SCHEMA.BUILD_JOB')" in query
        assert "ASYNC = TRUE" in query
        assert 'IMAGE_REGISTRY_URL: "host/db/schema/repo"' in query
        assert 'IMAGE_NAME: "my_app"' in query
        assert "@DB.SCHEMA.STAGE" in query
        assert "EXTERNAL_ACCESS_INTEGRATIONS" not in query

    @patch(EXECUTE_QUERY)
    def test_execute_build_job_with_eai(self, mock_execute):
        job_fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().execute_build_job(
            job_service_name=job_fqn,
            compute_pool="BUILD_POOL",
            code_stage=stage_fqn,
            image_repo_url="host/db/schema/repo",
            app_id="my_app",
            external_access_integration="MY_EAI",
        )
        mock_execute.assert_called_once()
        query = mock_execute.call_args[0][0]
        assert "EXTERNAL_ACCESS_INTEGRATIONS = (MY_EAI)" in query

    @patch(EXECUTE_QUERY)
    def test_execute_build_job_uses_default_image(self, mock_execute):
        job_fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().execute_build_job(
            job_service_name=job_fqn,
            compute_pool="BUILD_POOL",
            code_stage=stage_fqn,
            image_repo_url="host/db/schema/repo",
            app_id="my_app",
        )
        query = mock_execute.call_args[0][0]
        assert "sf-image-build:0.0.1" in query

    @patch(EXECUTE_QUERY)
    def test_execute_build_job_with_custom_build_image(self, mock_execute):
        job_fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().execute_build_job(
            job_service_name=job_fqn,
            compute_pool="BUILD_POOL",
            code_stage=stage_fqn,
            image_repo_url="host/db/schema/repo",
            app_id="my_app",
            build_image="/my/custom/builder:2.0",
        )
        query = mock_execute.call_args[0][0]
        assert '"/my/custom/builder:2.0"' in query
        assert "sf-image-build:0.0.1" not in query

    @patch(EXECUTE_QUERY)
    def test_get_build_status_done(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter([{"name": "BUILD_JOB", "status": "DONE"}])
        )
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        status = SnowflakeAppManager().get_build_status(fqn)
        assert status == "DONE"
        mock_execute.assert_called_once()

    @patch(EXECUTE_QUERY)
    def test_get_build_status_idle(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([]))
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        status = SnowflakeAppManager().get_build_status(fqn)
        assert status == "IDLE"

    @patch(EXECUTE_QUERY)
    def test_get_build_status_filters_by_name(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {"name": "OTHER_SERVICE", "status": "RUNNING"},
                    {"name": "BUILD_JOB", "status": "DONE"},
                ]
            )
        )
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        status = SnowflakeAppManager().get_build_status(fqn)
        assert status == "DONE"

    @patch(EXECUTE_QUERY)
    def test_create_service_basic(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().create_service(
            service_name=fqn,
            compute_pool="SVC_POOL",
            query_warehouse="WH",
        )
        # Should call: CREATE SERVICE, ALTER SERVICE SUSPEND (no comment)
        assert mock_execute.call_count == 2
        create_query = mock_execute.call_args_list[0][0][0]
        assert (
            "CREATE SERVICE IF NOT EXISTS IDENTIFIER('DB.SCHEMA.SVC')" in create_query
        )
        assert "IN COMPUTE POOL SVC_POOL" in create_query
        assert "QUERY_WAREHOUSE = WH" in create_query
        suspend_query = mock_execute.call_args_list[1][0][0]
        assert "ALTER SERVICE IDENTIFIER('DB.SCHEMA.SVC') SUSPEND" in suspend_query

    @patch(EXECUTE_QUERY)
    def test_create_service_default_no_execute_as_caller(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().create_service(
            service_name=fqn,
            compute_pool="SVC_POOL",
            query_warehouse="WH",
        )
        create_query = mock_execute.call_args_list[0][0][0]
        assert "executeAsCaller" not in create_query

    @patch(EXECUTE_QUERY)
    def test_create_service_with_execute_as_caller(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().create_service(
            service_name=fqn,
            compute_pool="SVC_POOL",
            query_warehouse="WH",
            execute_as_caller=True,
        )
        create_query = mock_execute.call_args_list[0][0][0]
        assert "executeAsCaller: true" in create_query

    @patch(EXECUTE_QUERY)
    def test_create_service_with_comment(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().create_service(
            service_name=fqn,
            compute_pool="SVC_POOL",
            query_warehouse="WH",
            app_comment='{"appId": "MY_APP"}',
        )
        # Should call: CREATE SERVICE, ALTER SET COMMENT, ALTER SUSPEND
        assert mock_execute.call_count == 3
        comment_query = mock_execute.call_args_list[1][0][0]
        assert "ALTER SERVICE IDENTIFIER('DB.SCHEMA.SVC') SET COMMENT" in comment_query
        assert '{"appId": "MY_APP"}' in comment_query

    @patch(EXECUTE_QUERY)
    def test_create_service_escapes_comment_quotes(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().create_service(
            service_name=fqn,
            compute_pool="SVC_POOL",
            query_warehouse="WH",
            app_comment="it's a test",
        )
        comment_query = mock_execute.call_args_list[1][0][0]
        assert "it''s a test" in comment_query

    @patch(EXECUTE_QUERY)
    def test_alter_service_spec(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().alter_service_spec(
            service_name=fqn,
            image_url="/db/schema/repo/my_app:latest",
        )
        mock_execute.assert_called_once()
        query = mock_execute.call_args[0][0]
        assert "ALTER SERVICE IDENTIFIER('DB.SCHEMA.SVC')" in query
        assert "/db/schema/repo/my_app:latest" in query

    @patch(EXECUTE_QUERY)
    def test_alter_service_spec_default_no_execute_as_caller(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().alter_service_spec(
            service_name=fqn,
            image_url="/db/schema/repo/my_app:latest",
        )
        query = mock_execute.call_args[0][0]
        assert "executeAsCaller" not in query

    @patch(EXECUTE_QUERY)
    def test_alter_service_spec_with_execute_as_caller(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().alter_service_spec(
            service_name=fqn,
            image_url="/db/schema/repo/my_app:latest",
            execute_as_caller=True,
        )
        query = mock_execute.call_args[0][0]
        assert "executeAsCaller: true" in query

    @patch(EXECUTE_QUERY)
    def test_resume_service(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        SnowflakeAppManager().resume_service(fqn)
        mock_execute.assert_called_once_with(
            "ALTER SERVICE IDENTIFIER('DB.SCHEMA.SVC') RESUME"
        )

    @patch(EXECUTE_QUERY)
    def test_get_service_status_running(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter([{"name": "SVC", "status": "RUNNING"}])
        )
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        status = SnowflakeAppManager().get_service_status(fqn)
        assert status == "RUNNING"

    @patch(EXECUTE_QUERY)
    def test_get_service_status_idle(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([]))
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        status = SnowflakeAppManager().get_service_status(fqn)
        assert status == "IDLE"

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "name": "app-endpoint",
                        "ingress_url": "https://my-endpoint.snowflakecomputing.app",
                    }
                ]
            )
        )
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url == "https://my-endpoint.snowflakecomputing.app"
        mock_execute.assert_called_once()

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url_adds_https_prefix(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "name": "app-endpoint",
                        "ingress_url": "my-endpoint.snowflakecomputing.app",
                    }
                ]
            )
        )
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url == "https://my-endpoint.snowflakecomputing.app"

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url_not_found(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([]))
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url is None

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url_provisioning_in_progress(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "name": "app-endpoint",
                        "ingress_url": "Provisioning in progress... check back later",
                    }
                ]
            )
        )
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url == "Provisioning in progress... check back later"
        assert not url.startswith("https://")


# ── fetch_config_table_defaults tests ─────────────────────────────────


class TestFetchConfigTableDefaults:
    @patch(EXECUTE_QUERY)
    def test_returns_defaults_from_table(self, mock_execute):
        import json

        cursor = Mock()
        cursor.fetchone.return_value = {
            "DEFAULTS": json.dumps(
                {
                    "warehouse": "SNOWADHOC",
                    "compute_pool": "ENG_COMPUTE_POOL",
                    "eai": "ALLOW_ALL_EAI",
                    "database": "SNOW_APPS",
                    "schema": "APPS",
                }
            )
        }
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().fetch_config_table_defaults("ENGINEER")
        assert result == {
            "warehouse": "SNOWADHOC",
            "compute_pool": "ENG_COMPUTE_POOL",
            "eai": "ALLOW_ALL_EAI",
            "database": "SNOW_APPS",
            "schema": "APPS",
        }
        query = mock_execute.call_args[0][0]
        assert "SNOW_APPS.CONFIG.APP_DEFAULTS" in query
        assert "'ENGINEER'" in query

    @patch(EXECUTE_QUERY)
    def test_returns_empty_dict_when_no_rows(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().fetch_config_table_defaults("ENGINEER")
        assert result == {}

    @patch(EXECUTE_QUERY, side_effect=Exception("table does not exist"))
    def test_returns_empty_dict_on_error(self, mock_execute):
        result = SnowflakeAppManager().fetch_config_table_defaults("ENGINEER")
        assert result == {}

    @patch(EXECUTE_QUERY)
    def test_handles_lowercase_column_name(self, mock_execute):
        import json

        cursor = Mock()
        cursor.fetchone.return_value = {"defaults": json.dumps({"warehouse": "MY_WH"})}
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().fetch_config_table_defaults("ENGINEER")
        assert result == {"warehouse": "MY_WH"}

    @patch(EXECUTE_QUERY)
    def test_filters_none_values(self, mock_execute):
        import json

        cursor = Mock()
        cursor.fetchone.return_value = {
            "DEFAULTS": json.dumps({"warehouse": "MY_WH", "eai": None})
        }
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().fetch_config_table_defaults("ENGINEER")
        assert result == {"warehouse": "MY_WH"}
        assert "eai" not in result

    @patch(EXECUTE_QUERY)
    def test_returns_empty_dict_for_non_dict_defaults(self, mock_execute):
        import json

        cursor = Mock()
        cursor.fetchone.return_value = {"DEFAULTS": json.dumps("not a dict")}
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().fetch_config_table_defaults("ENGINEER")
        assert result == {}

    @patch(EXECUTE_QUERY)
    def test_uses_custom_integration(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        SnowflakeAppManager().fetch_config_table_defaults(
            "ENGINEER", integration="custom-int"
        )
        query = mock_execute.call_args[0][0]
        assert "'custom-int'" in query

    @patch(EXECUTE_QUERY)
    def test_uppercases_role(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        SnowflakeAppManager().fetch_config_table_defaults("engineer")
        query = mock_execute.call_args[0][0]
        assert "'ENGINEER'" in query


# ── _resolve_deploy_defaults tests ────────────────────────────────────


CURRENT_ROLE = "snowflake.cli._plugins.apps.manager.SnowflakeAppManager.current_role"
FETCH_CONFIG_DEFAULTS = (
    "snowflake.cli._plugins.apps.manager.SnowflakeAppManager"
    ".fetch_config_table_defaults"
)


GET_CLI_CONTEXT = "snowflake.cli._plugins.apps.manager.get_cli_context"


def _mock_connection_context(warehouse=None, database=None, schema=None):
    ctx = Mock()
    ctx.connection_context.warehouse = warehouse
    ctx.connection_context.database = database
    ctx.connection_context.schema = schema
    return ctx


class TestResolveDeployDefaults:
    def _make_entity(
        self,
        *,
        query_warehouse=None,
        build_compute_pool=None,
        service_compute_pool=None,
        build_eai=None,
        database="TEST_DB",
        schema="TEST_SCHEMA",
        app_name="MY_APP",
    ):
        entity = Mock()
        entity.fqn = Mock(database=database, schema=schema, name=app_name)
        entity.query_warehouse = query_warehouse
        entity.build_compute_pool = (
            Mock(name_attr=build_compute_pool) if build_compute_pool else None
        )
        if build_compute_pool:
            entity.build_compute_pool.name = build_compute_pool
        entity.service_compute_pool = (
            Mock(name_attr=service_compute_pool) if service_compute_pool else None
        )
        if service_compute_pool:
            entity.service_compute_pool.name = service_compute_pool
        entity.build_eai = Mock(name_attr=build_eai) if build_eai else None
        if build_eai:
            entity.build_eai.name = build_eai
        return entity

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(FETCH_CONFIG_DEFAULTS, return_value={})
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_yml_values_take_precedence(
        self, mock_ctx, mock_role, mock_fetch, mock_exists
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(
            query_warehouse="YML_WH",
            build_compute_pool="YML_POOL",
            service_compute_pool="YML_SVC_POOL",
            build_eai="YML_EAI",
        )
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "YML_WH"
        assert result["build_compute_pool"] == "YML_POOL"
        assert result["service_compute_pool"] == "YML_SVC_POOL"
        assert result["build_eai"] == "YML_EAI"

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(
        FETCH_CONFIG_DEFAULTS,
        return_value={
            "warehouse": "TABLE_WH",
            "compute_pool": "TABLE_POOL",
            "eai": "TABLE_EAI",
            "database": "TABLE_DB",
            "schema": "TABLE_SCHEMA",
        },
    )
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_config_table_fills_gaps(
        self, mock_ctx, mock_role, mock_fetch, mock_exists
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database=None, schema=None)
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "TABLE_WH"
        assert result["build_compute_pool"] == "TABLE_POOL"
        assert result["service_compute_pool"] == "TABLE_POOL"
        assert result["build_eai"] == "TABLE_EAI"
        assert result["database"] == "TABLE_DB"
        assert result["schema"] == "TABLE_SCHEMA"

    @patch(OBJECT_EXISTS, return_value=True)
    @patch(FETCH_CONFIG_DEFAULTS, return_value={})
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_builtin_defaults_fill_remaining_gaps(
        self, mock_ctx, mock_role, mock_fetch, mock_exists
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity()
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["build_compute_pool"] == "SNOW_APPS_DEFAULT_COMPUTE_POOL"
        assert result["service_compute_pool"] == "SNOW_APPS_DEFAULT_COMPUTE_POOL"
        assert result["build_eai"] == "SNOW_APPS_DEFAULT_EXTERNAL_ACCESS"

    @patch(OBJECT_EXISTS, return_value=True)
    @patch(
        FETCH_CONFIG_DEFAULTS,
        return_value={"compute_pool": "TABLE_POOL", "warehouse": "TABLE_WH"},
    )
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_yml_beats_conn_beats_table_beats_builtin(
        self, mock_ctx, mock_role, mock_fetch, mock_exists
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(
            query_warehouse="YML_WH",
        )
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "YML_WH"
        assert result["build_compute_pool"] == "TABLE_POOL"
        assert result["service_compute_pool"] == "TABLE_POOL"
        assert result["build_eai"] == "SNOW_APPS_DEFAULT_EXTERNAL_ACCESS"

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(FETCH_CONFIG_DEFAULTS, return_value={})
    @patch(CURRENT_ROLE, return_value=None)
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_no_role_skips_config_table(
        self, mock_ctx, mock_role, mock_fetch, mock_exists
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity()
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        mock_fetch.assert_not_called()
        assert result["query_warehouse"] is None

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(FETCH_CONFIG_DEFAULTS, return_value={})
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_preserves_yml_database_and_schema(
        self, mock_ctx, mock_role, mock_fetch, mock_exists
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database="MY_DB", schema="MY_SCHEMA")
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["database"] == "MY_DB"
        assert result["schema"] == "MY_SCHEMA"

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(FETCH_CONFIG_DEFAULTS, return_value={})
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    @patch(
        GET_CLI_CONTEXT,
        return_value=_mock_connection_context(
            warehouse="CONN_WH", database="CONN_DB", schema="CONN_SCHEMA"
        ),
    )
    def test_connection_fills_gaps_before_table(
        self, mock_ctx, mock_role, mock_fetch, mock_exists
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database=None, schema=None)
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "CONN_WH"
        assert result["database"] == "CONN_DB"
        assert result["schema"] == "CONN_SCHEMA"

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(
        FETCH_CONFIG_DEFAULTS,
        return_value={"warehouse": "TABLE_WH", "database": "TABLE_DB"},
    )
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    @patch(
        GET_CLI_CONTEXT,
        return_value=_mock_connection_context(warehouse="CONN_WH"),
    )
    def test_connection_beats_table(self, mock_ctx, mock_role, mock_fetch, mock_exists):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database=None, schema=None)
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "CONN_WH"
        assert result["database"] == "TABLE_DB"


# ── CLI command tests ─────────────────────────────────────────────────


class TestSetupCommand:
    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_init_creates_file(self, mock_mgr_cls, mock_gen, runner, tmp_path):
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.current_role.return_value = "TEST_ROLE"
        mock_mgr.fetch_config_table_defaults.return_value = {"database": "CFG_DB"}

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "setup", "--app-name", "my_app"])
                assert result.exit_code == 0, result.output
                assert "Initialized Snowflake App project" in result.output
                assert (tmp_path / "snowflake.yml").exists()

        mock_mgr.current_role.assert_called_once()
        mock_mgr.fetch_config_table_defaults.assert_called_once_with("TEST_ROLE")
        call_kwargs = mock_gen.call_args
        assert call_kwargs[1].get("config_overrides") == {"database": "CFG_DB"} or (
            len(call_kwargs[0]) >= 4 and call_kwargs[0][3] == {"database": "CFG_DB"}
        )

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_init_skips_config_table_when_no_role(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.current_role.return_value = None

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "setup", "--app-name", "my_app"])
                assert result.exit_code == 0, result.output

        mock_mgr.fetch_config_table_defaults.assert_not_called()

    def test_init_skips_when_file_exists(self, runner, tmp_path):
        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            (tmp_path / "snowflake.yml").write_text("existing content")
            with change_directory(tmp_path):
                result = runner.invoke(["__app", "setup", "--app-name", "my_app"])
                assert result.exit_code == 0, result.output
                assert "already exists" in result.output


# ── perform_bundle tests ──────────────────────────────────────────────


class TestPerformBundle:
    @patch("snowflake.cli._plugins.apps.manager.get_cli_context")
    @patch("snowflake.cli._plugins.apps.manager.bundle_artifacts")
    def test_creates_bundle_root_and_calls_bundle_artifacts(
        self, mock_bundle, mock_ctx, tmp_path
    ):
        mock_ctx().project_root = tmp_path

        entity = Mock()
        entity.artifacts = [Mock(), Mock()]

        result = perform_bundle("my_app", entity)

        assert result.project_root == tmp_path
        assert result.bundle_root.exists()
        mock_bundle.assert_called_once_with(result, entity.artifacts)

    @patch("snowflake.cli._plugins.apps.manager.get_cli_context")
    @patch("snowflake.cli._plugins.apps.manager.bundle_artifacts")
    def test_removes_existing_bundle_root(self, mock_bundle, mock_ctx, tmp_path):
        mock_ctx().project_root = tmp_path

        # Pre-create a stale bundle root with a file in it
        stale_bundle = tmp_path / "output" / "bundle"
        stale_bundle.mkdir(parents=True)
        (stale_bundle / "old_file.txt").write_text("stale")

        entity = Mock()
        entity.artifacts = []

        result = perform_bundle("my_app", entity)

        assert result.bundle_root.exists()
        assert not (result.bundle_root / "old_file.txt").exists()

    @patch("snowflake.cli._plugins.apps.manager.get_cli_context")
    @patch("snowflake.cli._plugins.apps.manager.bundle_artifacts")
    def test_returns_project_paths(self, mock_bundle, mock_ctx, tmp_path):
        mock_ctx().project_root = tmp_path

        entity = Mock()
        entity.artifacts = []

        result = perform_bundle("my_app", entity)

        assert result.project_root == tmp_path
        assert result.bundle_root == tmp_path / "output" / "bundle"


# ── Bundle CLI command tests ──────────────────────────────────────────


class TestBundleCommand:
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_bundle_succeeds(
        self, mock_resolve, mock_get_entity, mock_perform_bundle, runner, tmp_path
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        mock_get_entity.return_value = entity

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "bundle"])
                assert result.exit_code == 0, result.output
                assert "Bundle generated at" in result.output
                assert "output" in result.output
                mock_perform_bundle.assert_called_once_with("my_app", entity)

    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_bundle_with_entity_id(
        self, mock_resolve, mock_get_entity, mock_perform_bundle, runner, tmp_path
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        mock_get_entity.return_value = entity

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "bundle", "--entity-id", "custom_app"])
                assert result.exit_code == 0, result.output
                mock_resolve.assert_called_once_with("custom_app")


# ── _find_dockerfile_expose_port tests ─────────────────────────────────


class TestFindDockerfileExposePort:
    def test_returns_port_from_expose(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import _find_dockerfile_expose_port

        (tmp_path / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 3000\n")
        assert _find_dockerfile_expose_port(tmp_path) == 3000

    def test_returns_port_with_tcp_suffix(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import _find_dockerfile_expose_port

        (tmp_path / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 8080/tcp\n")
        assert _find_dockerfile_expose_port(tmp_path) == 8080

    def test_returns_port_with_udp_suffix(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import _find_dockerfile_expose_port

        (tmp_path / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 5000/udp\n")
        assert _find_dockerfile_expose_port(tmp_path) == 5000

    def test_returns_first_port_when_multiple(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import _find_dockerfile_expose_port

        (tmp_path / "Dockerfile").write_text(
            "FROM python:3.11\nEXPOSE 3000\nEXPOSE 8080\n"
        )
        assert _find_dockerfile_expose_port(tmp_path) == 3000

    def test_returns_none_when_no_dockerfile(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import _find_dockerfile_expose_port

        assert _find_dockerfile_expose_port(tmp_path) is None

    def test_returns_none_when_no_expose(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import _find_dockerfile_expose_port

        (tmp_path / "Dockerfile").write_text("FROM python:3.11\nCMD ['python']\n")
        assert _find_dockerfile_expose_port(tmp_path) is None

    def test_case_insensitive(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import _find_dockerfile_expose_port

        (tmp_path / "Dockerfile").write_text("FROM python:3.11\nexpose 9090\n")
        assert _find_dockerfile_expose_port(tmp_path) == 9090

    def test_returns_unsupported_for_multi_port(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import (
            EXPOSE_UNSUPPORTED_SYNTAX,
            _find_dockerfile_expose_port,
        )

        (tmp_path / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 3000 8080\n")
        assert _find_dockerfile_expose_port(tmp_path) == EXPOSE_UNSUPPORTED_SYNTAX

    def test_returns_unsupported_for_port_range(self, tmp_path):
        from snowflake.cli._plugins.apps.manager import (
            EXPOSE_UNSUPPORTED_SYNTAX,
            _find_dockerfile_expose_port,
        )

        (tmp_path / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 3000-3005\n")
        assert _find_dockerfile_expose_port(tmp_path) == EXPOSE_UNSUPPORTED_SYNTAX


# ── Validate CLI command tests ────────────────────────────────────────


class TestValidateCommand:
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_succeeds(
        self,
        mock_resolve,
        mock_get_entity,
        mock_perform_bundle,
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.app_port = 3000
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        (bundle_dir / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 3000\n")

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.current_role.return_value = "ACCOUNTADMIN"

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "validate"])
                assert result.exit_code == 0, result.output
                assert "Valid Snowflake App project" in result.output

    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_fails_no_dockerfile(
        self,
        mock_resolve,
        mock_get_entity,
        mock_perform_bundle,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.app_port = 3000
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "validate"])
                assert result.exit_code == 1
                assert "No Dockerfile found" in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_warns_no_expose(
        self,
        mock_resolve,
        mock_get_entity,
        mock_perform_bundle,
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.app_port = 3000
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.current_role.return_value = "ACCOUNTADMIN"

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        (bundle_dir / "Dockerfile").write_text("FROM python:3.11\nCMD ['python']\n")

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "validate"])
                assert result.exit_code == 0, result.output
                assert "EXPOSE" in result.output
                assert "warning" in result.output.lower()

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_warns_unsupported_expose_syntax(
        self,
        mock_resolve,
        mock_get_entity,
        mock_perform_bundle,
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.app_port = 3000
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.current_role.return_value = "ACCOUNTADMIN"

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        (bundle_dir / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 3000 8080\n")

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "validate"])
                assert result.exit_code == 0, result.output
                assert "multi-port" in result.output.lower()
                assert "warning" in result.output.lower()

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_warns_port_mismatch(
        self,
        mock_resolve,
        mock_get_entity,
        mock_perform_bundle,
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.app_port = 3000
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        (bundle_dir / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 8080\n")

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.current_role.return_value = "ACCOUNTADMIN"

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "validate"])
                assert result.exit_code == 0, result.output
                assert "Validation passed with 1 warning(s)" in result.output
                assert "8080" in result.output
                assert "3000" in result.output

    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_cleans_up_bundle_on_error(
        self,
        mock_resolve,
        mock_get_entity,
        mock_perform_bundle,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.app_port = 3000
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "validate"])
                assert result.exit_code == 1
                assert not bundle_dir.exists()

    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_handles_perform_bundle_exception(
        self,
        mock_resolve,
        mock_get_entity,
        mock_perform_bundle,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.app_port = 3000
        mock_get_entity.return_value = entity

        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.side_effect = CliError("bundle failed")

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "validate"])
                assert result.exit_code == 1
                assert "bundle failed" in result.output


# ── role_has_bind_service_endpoint tests ──────────────────────────────


class TestRoleHasBindServiceEndpoint:
    @patch(EXECUTE_QUERY)
    def test_returns_true_when_privilege_granted(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "privilege": "BIND SERVICE ENDPOINT",
                        "granted_on": "ACCOUNT",
                    }
                ]
            )
        )
        mock_execute.return_value = cursor
        assert SnowflakeAppManager().role_has_bind_service_endpoint("DEV_ROLE") is True

    @patch(EXECUTE_QUERY)
    def test_returns_false_when_no_matching_privilege(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "privilege": "CREATE DATABASE",
                        "granted_on": "ACCOUNT",
                    }
                ]
            )
        )
        mock_execute.return_value = cursor
        assert SnowflakeAppManager().role_has_bind_service_endpoint("DEV_ROLE") is False

    @patch(EXECUTE_QUERY)
    def test_returns_false_when_no_grants(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([]))
        mock_execute.return_value = cursor
        assert SnowflakeAppManager().role_has_bind_service_endpoint("DEV_ROLE") is False

    @patch(EXECUTE_QUERY)
    def test_escapes_role_with_single_quote(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([]))
        mock_execute.return_value = cursor
        SnowflakeAppManager().role_has_bind_service_endpoint("BAD'ROLE")
        query = mock_execute.call_args[0][0]
        assert "BAD\\'ROLE" in query
        assert "BAD'ROLE" not in query


# ── Open CLI command tests ────────────────────────────────────────────


class TestOpenCommand:
    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_launches_browser(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
        mock_launch,
        runner,
        tmp_path,
    ):
        entity = Mock()
        entity.fqn = Mock(database="DB", schema="SCHEMA", name="MY_APP")
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.get_service_endpoint_url.return_value = (
            "https://my-app.snowflakecomputing.app"
        )

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "open"])
                assert result.exit_code == 0, result.output
                assert "https://my-app.snowflakecomputing.app" in result.output
                mock_launch.assert_called_once_with(
                    "https://my-app.snowflakecomputing.app"
                )

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_print_only(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
        mock_launch,
        runner,
        tmp_path,
    ):
        entity = Mock()
        entity.fqn = Mock(database="DB", schema="SCHEMA", name="MY_APP")
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.get_service_endpoint_url.return_value = (
            "https://my-app.snowflakecomputing.app"
        )

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "open", "--print-only"])
                assert result.exit_code == 0, result.output
                assert "https://my-app.snowflakecomputing.app" in result.output
                mock_launch.assert_not_called()

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_fails_when_no_endpoint(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        entity = Mock()
        entity.fqn = Mock(database="DB", schema="SCHEMA", name="MY_APP")
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.get_service_endpoint_url.return_value = None

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "open"])
                assert result.exit_code == 1
                assert "No endpoint URL found" in result.output


# ── Deploy CLI command tests ──────────────────────────────────────────


RESOLVE_DEPLOY_DEFAULTS = (
    "snowflake.cli._plugins.apps.commands._resolve_deploy_defaults"
)


class TestDeployCommand:
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": None,
            "service_compute_pool": "SVC_POOL",
            "build_eai": None,
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
        },
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_fails_missing_build_compute_pool(
        self, mock_resolve, mock_get_entity, mock_defaults, runner, tmp_path
    ):
        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        entity.code_stage = None
        entity.artifacts = []
        entity.meta = None
        entity.image_repository = Mock()
        entity.image_repository.name = "MY_REPO"
        mock_get_entity.return_value = entity

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "deploy"])
                assert result.exit_code == 1
                assert "build_compute_pool is required" in result.output

    @patch("snowflake.cli._plugins.apps.commands._poll_until")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": None,
            "service_compute_pool": "SVC_POOL",
            "build_eai": None,
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_skip_build_skips_build_phase(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
        mock_poll,
        runner,
        tmp_path,
    ):
        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = None
        entity.artifacts = []
        entity.meta = None
        entity.image_repository = Mock()
        entity.image_repository.name = "MY_REPO"
        entity.image_repository.database = None
        entity.image_repository.schema_ = None
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.get_image_repo_url.return_value = (
            "host.registry-local.snowflakecomputing.com/TEST_DB/TEST_SCHEMA/MY_REPO"
        )
        mock_poll.return_value = "https://my-app.snowflakecomputing.app"

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "deploy", "--skip-build"])
                assert result.exit_code == 0, result.output
                assert "Skipping build phase" in result.output
                mock_mgr.get_image_repo_url.assert_called_once_with(
                    "MY_REPO", database="TEST_DB", schema="TEST_SCHEMA"
                )
                mock_mgr.create_schema_if_not_exists.assert_not_called()
                mock_mgr.execute_build_job.assert_not_called()
                mock_mgr.create_service.assert_called_once()
                mock_mgr.alter_service_spec.assert_called_once()
                mock_mgr.resume_service.assert_called_once()

    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": None,
            "service_compute_pool": None,
            "build_eai": None,
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
        },
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_skip_build_allows_missing_build_compute_pool(
        self, mock_resolve, mock_get_entity, mock_defaults, runner, tmp_path
    ):
        """--skip-build should not require build_compute_pool."""
        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        entity.code_stage = None
        entity.artifacts = []
        entity.meta = None
        entity.image_repository = None
        mock_get_entity.return_value = entity

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "deploy", "--skip-build"])
                assert result.exit_code == 1
                assert "build_compute_pool is required" not in result.output
                assert "service_compute_pool is required" in result.output

    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": None,
            "build_eai": None,
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
        },
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_fails_missing_service_compute_pool(
        self, mock_resolve, mock_get_entity, mock_defaults, runner, tmp_path
    ):
        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        entity.code_stage = None
        entity.artifacts = []
        entity.meta = None
        entity.image_repository = Mock()
        entity.image_repository.name = "MY_REPO"
        mock_get_entity.return_value = entity

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "deploy"])
                assert result.exit_code == 1
                assert "service_compute_pool is required" in result.output

    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": None,
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": "SVC_POOL",
            "build_eai": None,
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
        },
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._get_entity",
    )
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_fails_missing_query_warehouse(
        self, mock_resolve, mock_get_entity, mock_defaults, runner, tmp_path
    ):
        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        entity.code_stage = None
        entity.artifacts = []
        entity.meta = None
        entity.image_repository = Mock()
        entity.image_repository.name = "MY_REPO"
        mock_get_entity.return_value = entity

        with with_feature_flags({FeatureFlag.ENABLE_SNOWFLAKE_APPS: True}):
            from tests_common import change_directory

            with change_directory(tmp_path):
                result = runner.invoke(["__app", "deploy"])
                assert result.exit_code == 1
                assert "query_warehouse is required" in result.output
