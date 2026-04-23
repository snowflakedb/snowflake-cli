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
    _get_entity,
    _get_snowflake_app_entities,
    _object_exists,
    _poll_until,
    _resolve_entity_id,
    perform_bundle,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.connector.errors import ProgrammingError

from tests_common import change_directory

EXECUTE_QUERY = "snowflake.cli._plugins.apps.manager.SnowflakeAppManager.execute_query"
OBJECT_EXISTS = "snowflake.cli._plugins.apps.manager._object_exists"
GET_CLI_CONTEXT = "snowflake.cli._plugins.apps.manager.get_cli_context"
GET_ENV_USERNAME = "snowflake.cli._plugins.apps.commands.get_env_username"
FETCH_SNOW_APPS_PARAMS = (
    "snowflake.cli._plugins.apps.manager.SnowflakeAppManager"
    ".fetch_snow_apps_parameters"
)


_SNOWFLAKE_APP_YML = """definition_version: '2'
entities:
  my_app:
    type: snowflake-app
    identifier: my_app
    artifacts:
      - src: "*"
        dest: ./
"""


def _write_snowflake_app_yml(path):
    """Write a minimal ``snowflake.yml`` containing a single ``snowflake-app``
    entity so that ``@with_app_flow_routing()`` can detect the Snowflake Apps Deploy
    flow when the CLI is invoked from ``path``.
    """
    (path / "snowflake.yml").write_text(_SNOWFLAKE_APP_YML)


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
    _BASE_RESOLVED = {
        "database": "TEST_DB",
        "schema": "SNOW_APPS",
        "warehouse": "TEST_WH",
        "build_compute_pool": "MY_POOL",
        "service_compute_pool": "MY_POOL",
        "build_eai": "MY_EAI",
    }

    def test_generates_yml_with_all_required_values(self):
        result = _generate_snowflake_yml("my_app", self._BASE_RESOLVED)
        assert "type: snowflake-app" in result
        assert "name: MY_APP" in result
        assert "database: TEST_DB" in result
        assert "schema: SNOW_APPS" in result
        assert "query_warehouse: TEST_WH" in result
        assert "name: MY_POOL" in result
        assert "name: MY_EAI" in result
        # code_workspace is written as a fully-qualified identifier.
        assert "code_workspace: TEST_DB.SNOW_APPS.MY_APP_CODE" in result
        assert "code_stage:" not in result
        assert "image_repository" not in result
        assert "artifact_repository" not in result

    def test_no_null_values_in_output(self):
        result = _generate_snowflake_yml("my_app", self._BASE_RESOLVED)
        assert "null" not in result

    def test_build_eai_omitted_when_missing(self):
        """When ``build_eai`` is missing, the generated YAML has no
        ``build_eai`` block — the field is optional."""
        resolved = {**self._BASE_RESOLVED, "build_eai": None}
        result = _generate_snowflake_yml("my_app", resolved)
        assert "build_eai" not in result
        assert "None" not in result

    def test_build_eai_omitted_when_missing_key(self):
        """When ``build_eai`` is not in the resolved dict at all, the
        generated YAML still works and omits the block."""
        resolved = {k: v for k, v in self._BASE_RESOLVED.items() if k != "build_eai"}
        result = _generate_snowflake_yml("my_app", resolved)
        assert "build_eai" not in result

    def test_custom_schema(self):
        resolved = {**self._BASE_RESOLVED, "schema": "CFG_SCHEMA"}
        result = _generate_snowflake_yml("my_app", resolved)
        assert "schema: CFG_SCHEMA" in result

    def test_generated_yml_is_valid_project_definition(self):
        """Generated YAML is parsable and produces a valid project definition."""
        import yaml
        from snowflake.cli.api.utils.definition_rendering import (
            render_definition_template,
        )

        raw_yml = _generate_snowflake_yml("my_app", self._BASE_RESOLVED)
        definition_input = yaml.safe_load(raw_yml)

        result = render_definition_template(definition_input, {})
        project = result.project_definition
        entity = project.entities["my_app"]

        assert entity.type == "snowflake-app"
        assert entity.query_warehouse == "TEST_WH"
        # code_workspace is emitted as ``DB.SCHEMA.WORKSPACE`` and the
        # validator parses it back into a ``CodeWorkspaceReference`` with
        # db/schema set.
        assert entity.code_workspace.name == "MY_APP_CODE"
        assert entity.code_workspace.database == "TEST_DB"
        assert entity.code_workspace.schema_ == "SNOW_APPS"
        assert entity.code_stage is None
        assert entity.artifacts is not None


# ── SnowflakeAppManager tests ─────────────────────────────────────────


class TestDatabaseExists:
    @patch(EXECUTE_QUERY)
    def test_returns_true_when_database_found(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = {"name": "MY_DB"}
        mock_execute.return_value = cursor

        assert SnowflakeAppManager().database_exists("MY_DB") is True
        query = mock_execute.call_args[0][0]
        assert "SHOW DATABASES LIKE" in query
        assert "'MY_DB'" in query

    @patch(EXECUTE_QUERY)
    def test_returns_false_when_database_not_found(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        assert SnowflakeAppManager().database_exists("NO_SUCH_DB") is False

    @patch(EXECUTE_QUERY)
    def test_escapes_database_name(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        SnowflakeAppManager().database_exists("BAD'DB")
        query = mock_execute.call_args[0][0]
        assert "BAD\\'DB" in query
        assert "BAD'DB" not in query


class TestSchemaExists:
    @patch(EXECUTE_QUERY)
    def test_returns_true_when_schema_found(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = {"name": "MY_SCHEMA"}
        mock_execute.return_value = cursor

        assert SnowflakeAppManager().schema_exists("MY_DB", "MY_SCHEMA") is True
        query = mock_execute.call_args[0][0]
        assert "SHOW SCHEMAS LIKE" in query
        assert "'MY_SCHEMA'" in query
        assert "MY_DB" in query

    @patch(EXECUTE_QUERY)
    def test_returns_false_when_schema_not_found(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        assert SnowflakeAppManager().schema_exists("MY_DB", "NO_SUCH") is False


class TestSnowflakeAppManager:
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

    @staticmethod
    def _find_query(call_args_list, substr):
        for call in call_args_list:
            if substr in call[0][0]:
                return call[0][0]
        raise AssertionError(f"No query containing '{substr}' found")

    @patch(EXECUTE_QUERY)
    def test_build_app_artifact_repo_sanitizes_inputs(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ("Build job submitted: DB.SCHEMA.JOB",)
        mock_execute.return_value = cursor

        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().build_app_artifact_repo(
            stage_fqn=stage_fqn,
            artifact_repo_fqn="DB.SCHEMA.REPO",
            app_id="my_app",
            compute_pool="BUILD_POOL",
            database="DB",
            schema="SCHEMA",
            runtime_image="runtime:latest",
        )
        build_query = self._find_query(
            mock_execute.call_args_list, "SPCS_TEST_BUILD_APP_ARTIFACT_REPO"
        )
        assert "'DB.SCHEMA.REPO'" in build_query
        assert "'my_app'" in build_query
        assert "'BUILD_POOL'" in build_query

    @patch(EXECUTE_QUERY)
    def test_build_app_artifact_repo_escapes_single_quotes(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ("Build job submitted: DB.SCHEMA.JOB",)
        mock_execute.return_value = cursor

        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().build_app_artifact_repo(
            stage_fqn=stage_fqn,
            artifact_repo_fqn="DB.SCHEMA.REPO",
            app_id="app'injection",
            compute_pool="BUILD_POOL",
            database="DB",
            schema="SCHEMA",
            runtime_image="runtime:latest",
        )
        build_query = self._find_query(
            mock_execute.call_args_list, "SPCS_TEST_BUILD_APP_ARTIFACT_REPO"
        )
        assert "app'injection" not in build_query
        assert "app\\'injection" in build_query

    @patch(EXECUTE_QUERY)
    def test_build_app_artifact_repo_restores_session(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.side_effect = [
            ("PREV_DB",),
            ("PREV_SCHEMA",),
            None,
            None,
            ("Build job submitted: DB.SCHEMA.JOB",),
            None,
            None,
        ]
        mock_execute.return_value = cursor

        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        SnowflakeAppManager().build_app_artifact_repo(
            stage_fqn=stage_fqn,
            artifact_repo_fqn="DB.SCHEMA.REPO",
            app_id="my_app",
            compute_pool="BUILD_POOL",
            database="DB",
            schema="SCHEMA",
            runtime_image="runtime:latest",
        )
        queries = [c[0][0] for c in mock_execute.call_args_list]
        spcs_idx = next(i for i, q in enumerate(queries) if "SPCS_TEST_BUILD" in q)
        restore_db_idx = queries.index("USE DATABASE PREV_DB")
        restore_schema_idx = queries.index("USE SCHEMA PREV_SCHEMA")
        assert restore_db_idx > spcs_idx
        assert restore_schema_idx > restore_db_idx

    @patch(EXECUTE_QUERY)
    def test_create_app_service_generates_correct_sql(self, mock_execute):
        cursor = Mock()
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        SnowflakeAppManager().create_app_service(
            service_fqn=fqn,
            artifact_repo_fqn="DB.SCHEMA.REPO",
            package_name="my_app",
            compute_pool="SVC_POOL",
            version="LATEST",
            query_warehouse="WH",
            external_access_integrations=["MY_EAI"],
        )
        create_query = self._find_query(
            mock_execute.call_args_list, "CREATE APPLICATION SERVICE"
        )
        assert "DB.SCHEMA.my_app" in create_query
        assert "ARTIFACT REPOSITORY DB.SCHEMA.REPO" in create_query
        assert "PACKAGE my_app" in create_query
        assert "VERSION LATEST" in create_query
        assert "IN COMPUTE POOL SVC_POOL" in create_query
        assert "QUERY_WAREHOUSE = WH" in create_query
        assert "EXTERNAL_ACCESS_INTEGRATIONS = (MY_EAI)" in create_query

    @patch(EXECUTE_QUERY)
    def test_create_app_service_with_comment(self, mock_execute):
        cursor = Mock()
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        SnowflakeAppManager().create_app_service(
            service_fqn=fqn,
            artifact_repo_fqn="DB.SCHEMA.REPO",
            package_name="my_app",
            compute_pool="SVC_POOL",
            comment="it's a test",
        )
        create_query = self._find_query(
            mock_execute.call_args_list, "CREATE APPLICATION SERVICE"
        )
        assert "COMMENT = 'it''s a test'" in create_query

    @patch(EXECUTE_QUERY)
    def test_upgrade_app_service_generates_correct_sql(self, mock_execute):
        cursor = Mock()
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        SnowflakeAppManager().upgrade_app_service(
            service_fqn=fqn,
            version="LATEST",
        )
        upgrade_query = self._find_query(
            mock_execute.call_args_list, "ALTER APPLICATION SERVICE"
        )
        assert "DB.SCHEMA.my_app" in upgrade_query
        assert "UPGRADE" in upgrade_query
        assert "TO VERSION LATEST" in upgrade_query

    @patch(EXECUTE_QUERY)
    def test_get_app_service_logs(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ("log output here",)
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().get_app_service_logs("my_app")
        assert result == "log output here"
        mock_execute.assert_called_once_with(
            "CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('my_app')"
        )

    @patch(EXECUTE_QUERY)
    def test_describe_app_service_normalises_keys(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = {
            "URL": "my-app.snowflakecomputing.app",
            "IS_UPGRADING": "false",
        }
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        desc = SnowflakeAppManager().describe_app_service(fqn)
        assert desc["url"] == "my-app.snowflakecomputing.app"
        assert desc["is_upgrading"] == "false"
        query = mock_execute.call_args[0][0]
        assert "DESCRIBE APPLICATION SERVICE DB.SCHEMA.my_app" in query

    @patch(EXECUTE_QUERY)
    def test_describe_app_service_empty(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        desc = SnowflakeAppManager().describe_app_service(fqn)
        assert desc == {}

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
    def test_artifact_repo_exists_returns_true(self, mock_execute):
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([{"name": "MY_REPO"}]))
        mock_execute.return_value = mock_cursor
        assert (
            SnowflakeAppManager().artifact_repo_exists(
                database="DB", schema="SCHEMA", repo_name="MY_REPO"
            )
            is True
        )
        query = mock_execute.call_args[0][0]
        assert "SHOW ARTIFACT REPOSITORIES LIKE" in query
        assert "IN SCHEMA" in query
        assert "DB.SCHEMA" in query

    @patch(EXECUTE_QUERY)
    def test_artifact_repo_exists_returns_false(self, mock_execute):
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([]))
        mock_execute.return_value = mock_cursor
        assert (
            SnowflakeAppManager().artifact_repo_exists(
                database="DB", schema="SCHEMA", repo_name="MY_REPO"
            )
            is False
        )
        query = mock_execute.call_args[0][0]
        assert "SHOW ARTIFACT REPOSITORIES LIKE" in query
        assert "IN SCHEMA" in query
        assert "DB.SCHEMA" in query

    @patch(EXECUTE_QUERY)
    def test_create_artifact_repo(self, mock_execute):
        SnowflakeAppManager().create_artifact_repo(
            database="DB", schema="SCHEMA", repo_name="MY_REPO"
        )
        mock_execute.assert_called_once_with(
            "CREATE ARTIFACT REPOSITORY IF NOT EXISTS IDENTIFIER('DB.SCHEMA.MY_REPO') TYPE=APPLICATION"
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
    def test_get_service_logs(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ("INFO: app started\nINFO: listening",)
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="MY_APP")
        logs = SnowflakeAppManager().get_service_logs(fqn)
        assert logs == "INFO: app started\nINFO: listening"
        mock_execute.assert_called_once_with(
            "CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('DB.SCHEMA.MY_APP', 500)"
        )

    @patch(EXECUTE_QUERY)
    def test_get_service_logs_custom_last(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ("line1\nline2",)
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="MY_APP")
        logs = SnowflakeAppManager().get_service_logs(fqn, last=100)
        assert logs == "line1\nline2"
        mock_execute.assert_called_once_with(
            "CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('DB.SCHEMA.MY_APP', 100)"
        )

    @patch(EXECUTE_QUERY)
    def test_get_service_logs_empty_result(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="MY_APP")
        logs = SnowflakeAppManager().get_service_logs(fqn)
        assert logs == ""

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


# ── fetch_snow_apps_parameters tests ──────────────────────────────────


class TestFetchSnowAppsParameters:
    @patch(EXECUTE_QUERY)
    def test_returns_mapped_parameters(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {"key": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE", "value": "MY_WH"},
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL",
                        "value": "MY_POOL",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_SERVICE_COMPUTE_POOL",
                        "value": "SVC_POOL",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION",
                        "value": "MY_EAI",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE",
                        "value": "MY_DB",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA",
                        "value": "MY_SCHEMA",
                    },
                ]
            )
        )
        mock_execute.return_value = cursor
        result = SnowflakeAppManager().fetch_snow_apps_parameters()
        assert result == {
            "query_warehouse": "MY_WH",
            "build_compute_pool": "MY_POOL",
            "service_compute_pool": "SVC_POOL",
            "build_eai": "MY_EAI",
            "database": "MY_DB",
            "schema": "MY_SCHEMA",
        }
        query = mock_execute.call_args[0][0]
        assert "SHOW PARAMETERS LIKE 'DEFAULT_SNOWFLAKE_APPS_%' IN USER" in query

    @patch(EXECUTE_QUERY)
    def test_ignores_empty_string_values(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {"key": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE", "value": "MY_WH"},
                    {"key": "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL", "value": ""},
                ]
            )
        )
        mock_execute.return_value = cursor
        result = SnowflakeAppManager().fetch_snow_apps_parameters()
        assert result == {"query_warehouse": "MY_WH"}
        assert "build_compute_pool" not in result

    @patch(EXECUTE_QUERY)
    def test_ignores_unknown_parameters(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {"key": "DEFAULT_SNOWFLAKE_APPS_UNKNOWN_PARAM", "value": "FOO"},
                    {"key": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE", "value": "MY_WH"},
                ]
            )
        )
        mock_execute.return_value = cursor
        result = SnowflakeAppManager().fetch_snow_apps_parameters()
        assert result == {"query_warehouse": "MY_WH"}

    @patch(
        EXECUTE_QUERY,
        side_effect=ProgrammingError("permission denied"),
    )
    def test_returns_empty_dict_on_error(self, mock_execute):
        result = SnowflakeAppManager().fetch_snow_apps_parameters()
        assert result == {}

    @patch(EXECUTE_QUERY)
    def test_returns_empty_dict_when_no_params_set(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter([]))
        mock_execute.return_value = cursor
        result = SnowflakeAppManager().fetch_snow_apps_parameters()
        assert result == {}

    @patch(EXECUTE_QUERY)
    def test_handles_uppercase_column_names(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [{"KEY": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE", "VALUE": "MY_WH"}]
            )
        )
        mock_execute.return_value = cursor
        result = SnowflakeAppManager().fetch_snow_apps_parameters()
        assert result == {"query_warehouse": "MY_WH"}


# ── _resolve_deploy_defaults tests ────────────────────────────────────


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
        artifact_repository=None,
        database="TEST_DB",
        schema="TEST_SCHEMA",
        app_name="MY_APP",
    ):
        entity = Mock()
        fqn = Mock(database=database, schema=schema)
        fqn.name = app_name
        entity.fqn = fqn
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
        entity.artifact_repository = None
        if artifact_repository:
            entity.artifact_repository = Mock()
            entity.artifact_repository.name = artifact_repository
            entity.artifact_repository.database = None
            entity.artifact_repository.schema_ = None
        return entity

    @patch(FETCH_SNOW_APPS_PARAMS, return_value={})
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_yml_values_take_precedence(self, mock_ctx, mock_params):
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

    @patch(
        FETCH_SNOW_APPS_PARAMS,
        return_value={
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
        },
    )
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_parameters_fill_gaps(self, mock_ctx, mock_params):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database=None, schema=None)
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "PARAM_WH"
        assert result["build_compute_pool"] == "PARAM_POOL"
        assert result["service_compute_pool"] == "PARAM_SVC_POOL"
        assert result["build_eai"] == "PARAM_EAI"
        assert result["database"] == "PARAM_DB"
        assert result["schema"] == "PARAM_SCHEMA"

    @patch(
        FETCH_SNOW_APPS_PARAMS,
        return_value={"query_warehouse": "PARAM_WH", "build_eai": "PARAM_EAI"},
    )
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_yml_beats_params_beats_session(self, mock_ctx, mock_params):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(
            query_warehouse="YML_WH",
        )
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "YML_WH"  # yml wins over param
        assert result["build_eai"] == "PARAM_EAI"  # param fills gap

    @patch(FETCH_SNOW_APPS_PARAMS, return_value={})
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_preserves_yml_database_and_schema(self, mock_ctx, mock_params):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database="MY_DB", schema="MY_SCHEMA")
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["database"] == "MY_DB"
        assert result["schema"] == "MY_SCHEMA"

    @patch(FETCH_SNOW_APPS_PARAMS, return_value={})
    @patch(
        GET_CLI_CONTEXT,
        return_value=_mock_connection_context(
            warehouse="CONN_WH", database="CONN_DB", schema="CONN_SCHEMA"
        ),
    )
    def test_session_fills_gaps_after_params(self, mock_ctx, mock_params):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database=None, schema=None)
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "CONN_WH"
        assert result["database"] == "CONN_DB"
        assert result["schema"] == "CONN_SCHEMA"

    @patch(
        FETCH_SNOW_APPS_PARAMS,
        return_value={"query_warehouse": "PARAM_WH", "database": "PARAM_DB"},
    )
    @patch(
        GET_CLI_CONTEXT,
        return_value=_mock_connection_context(warehouse="CONN_WH"),
    )
    def test_params_beat_session(self, mock_ctx, mock_params):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database=None, schema=None)
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["query_warehouse"] == "PARAM_WH"  # param beats session
        assert result["database"] == "PARAM_DB"

    @patch(FETCH_SNOW_APPS_PARAMS, return_value={})
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_returns_none_when_no_source_provides_value(self, mock_ctx, mock_params):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity()
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["build_compute_pool"] is None
        assert result["service_compute_pool"] is None
        assert result["build_eai"] is None

    @patch(FETCH_SNOW_APPS_PARAMS, return_value={})
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_artifact_repository_defaults_to_app_name_repo(self, mock_ctx, mock_params):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(app_name="MY_APP")
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["artifact_repository"] == "MY_APP_REPO"

    @patch(FETCH_SNOW_APPS_PARAMS, return_value={})
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_explicit_artifact_repository_takes_precedence(self, mock_ctx, mock_params):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(artifact_repository="CUSTOM_REPO")
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["artifact_repository"] == "CUSTOM_REPO"

    @patch(FETCH_SNOW_APPS_PARAMS, return_value={})
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    def test_explicit_app_name_overrides_fqn_for_default_repo(
        self, mock_ctx, mock_params
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(app_name="ENTITY_NAME")
        result = _resolve_deploy_defaults(
            entity, SnowflakeAppManager(), app_name="OVERRIDE_NAME"
        )
        assert result["artifact_repository"] == "OVERRIDE_NAME_REPO"


# ── CLI command tests ─────────────────────────────────────────────────


class TestSetupCommand:
    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_init_creates_file(self, mock_mgr_cls, mock_gen, runner, tmp_path):
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
        }

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output
            assert "Initialized Snowflake Apps Deploy project" in result.output
            assert (tmp_path / "snowflake.yml").exists()

        resolved = mock_gen.call_args[0][1]
        assert resolved["database"] == "PARAM_DB"
        assert resolved["warehouse"] == "PARAM_WH"
        assert resolved["build_compute_pool"] == "PARAM_POOL"
        assert resolved["build_eai"] == "PARAM_EAI"

    def test_init_skips_when_file_exists(self, runner, tmp_path):
        (tmp_path / "snowflake.yml").write_text("existing content")
        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output
            assert "already exists" in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_dry_run_does_not_create_file(self, mock_mgr_cls, runner, tmp_path):
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(
                ["app", "setup", "--app-name", "my_app", "--dry-run"]
            )
            assert result.exit_code == 0, result.output
            assert not (tmp_path / "snowflake.yml").exists()
            assert "PARAM_DB" in result.output
            assert "PARAM_WH" in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_dry_run_omits_missing_build_eai(self, mock_mgr_cls, runner, tmp_path):
        """``--build-eai`` is optional: when no value is resolved the dry-run
        output should not emit the ``build_eai`` line (which would otherwise
        display ``build_eai: None  (missing)`` and imply it is required)."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            # no build_eai
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(
                ["app", "setup", "--app-name", "my_app", "--dry-run"]
            )
            assert result.exit_code == 0, result.output
            assert "build_eai" not in result.output
            assert "missing" not in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_dry_run_json_output(self, mock_mgr_cls, runner, tmp_path):
        import json as json_mod

        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(
                [
                    "app",
                    "setup",
                    "--app-name",
                    "my_app",
                    "--dry-run",
                    "--format",
                    "json",
                ]
            )
            assert result.exit_code == 0, result.output
            parsed = json_mod.loads(result.output)
            assert parsed["success"] is False
            assert parsed["database"] == "PARAM_DB"
            assert parsed["warehouse"] == "PARAM_WH"
            assert parsed["build_compute_pool"] == "PARAM_POOL"
            assert parsed["build_eai"] == "PARAM_EAI"

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_success_json_includes_resolved_values(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        import json as json_mod

        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(
                [
                    "app",
                    "setup",
                    "--app-name",
                    "my_app",
                    "--format",
                    "json",
                ]
            )
            assert result.exit_code == 0, result.output
            parsed = json_mod.loads(result.output)
            assert parsed["success"] is True
            assert parsed["database"] == "PARAM_DB"
            assert parsed["warehouse"] == "PARAM_WH"

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_success_prints_resolved_values_in_default_format(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output
            assert "database: PARAM_DB" in result.output
            assert "warehouse: PARAM_WH" in result.output
            assert "build_compute_pool: PARAM_POOL" in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_flags_beat_parameters(self, mock_mgr_cls, mock_gen, runner, tmp_path):
        """CLI flags should override SnowApps parameters."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(
                [
                    "app",
                    "setup",
                    "--app-name",
                    "my_app",
                    "--compute-pool",
                    "FLAG_POOL",
                    "--build-eai",
                    "FLAG_EAI",
                ]
            )
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["build_compute_pool"] == "FLAG_POOL"
        assert resolved["build_eai"] == "FLAG_EAI"
        # These come from params since no flag overrides them
        assert resolved["database"] == "PARAM_DB"
        assert resolved["warehouse"] == "PARAM_WH"

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_setup_shows_parameter_provenance(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """Resolved values from SnowApps parameters should show 'account parameter' provenance."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output
            assert "account parameter" in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_personal_db_default_and_public_schema(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """When no param/session db is set, fall back to the personal DB and PUBLIC schema."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
        }
        mock_mgr.get_personal_database.return_value = "USER$MYUSER"

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["database"] == "USER$MYUSER"
        assert resolved["schema"] == "PUBLIC"


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

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "bundle"])
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

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "bundle", "--entity-id", "custom_app"])
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
    @staticmethod
    def _make_validate_entity(app_port=3000):
        entity = Mock()
        entity.app_port = app_port
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        return entity

    @staticmethod
    def _configure_manager_mock(mock_manager_cls):
        mock_mgr = mock_manager_cls.return_value
        mock_mgr.database_exists.return_value = True
        mock_mgr.schema_exists.return_value = True
        return mock_mgr

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

        mock_get_entity.return_value = self._make_validate_entity()
        self._configure_manager_mock(mock_manager_cls)

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        (bundle_dir / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 3000\n")

        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
            assert result.exit_code == 0, result.output
            assert "Valid Snowflake Apps Deploy project" in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_fails_database_not_found(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        mock_get_entity.return_value = self._make_validate_entity()
        mock_mgr = self._configure_manager_mock(mock_manager_cls)
        mock_mgr.database_exists.return_value = False

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
            assert result.exit_code == 1
            assert "Database 'TEST_DB' does not exist" in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_fails_schema_not_found(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        mock_get_entity.return_value = self._make_validate_entity()
        mock_mgr = self._configure_manager_mock(mock_manager_cls)
        mock_mgr.schema_exists.return_value = False

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
            assert result.exit_code == 1
            assert "Schema 'TEST_DB.TEST_SCHEMA' does not exist" in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
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
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        mock_get_entity.return_value = self._make_validate_entity()
        self._configure_manager_mock(mock_manager_cls)

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)

        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
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

        mock_get_entity.return_value = self._make_validate_entity()
        self._configure_manager_mock(mock_manager_cls)

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        (bundle_dir / "Dockerfile").write_text("FROM python:3.11\nCMD ['python']\n")

        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
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

        mock_get_entity.return_value = self._make_validate_entity()
        self._configure_manager_mock(mock_manager_cls)

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        (bundle_dir / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 3000 8080\n")

        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
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

        mock_get_entity.return_value = self._make_validate_entity()
        self._configure_manager_mock(mock_manager_cls)

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        (bundle_dir / "Dockerfile").write_text("FROM python:3.11\nEXPOSE 8080\n")

        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
            assert result.exit_code == 0, result.output
            assert "Validation passed with 1 warning(s)" in result.output
            assert "8080" in result.output
            assert "3000" in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
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
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        mock_get_entity.return_value = self._make_validate_entity()
        self._configure_manager_mock(mock_manager_cls)

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)

        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
            assert result.exit_code == 1
            assert not bundle_dir.exists()

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
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
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        mock_get_entity.return_value = self._make_validate_entity()
        self._configure_manager_mock(mock_manager_cls)

        mock_perform_bundle.side_effect = CliError("bundle failed")

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "validate"])
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

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open"])
            assert result.exit_code == 0, result.output
            assert result.output.strip() == "https://my-app.snowflakecomputing.app"
            mock_launch.assert_called_once_with("https://my-app.snowflakecomputing.app")

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

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--print-only"])
            assert result.exit_code == 0, result.output
            assert result.output.strip() == "https://my-app.snowflakecomputing.app"
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

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open"])
            assert result.exit_code == 1
            assert "No endpoint URL found" in result.output

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_falls_back_to_connection_context(
        self,
        mock_resolve,
        mock_get_entity,
        mock_ctx,
        mock_manager_cls,
        mock_launch,
        runner,
        tmp_path,
    ):
        """Non-settings path uses connection_context when fqn has no db/schema."""
        entity = Mock()
        fqn = Mock(database=None, schema=None)
        fqn.name = "MY_APP"
        entity.fqn = fqn
        mock_get_entity.return_value = entity
        mock_ctx.return_value.connection_context = Mock(
            database="CONN_DB", schema="CONN_SCHEMA"
        )

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.get_service_endpoint_url.return_value = (
            "https://my-app.snowflakecomputing.app"
        )

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open"])
            assert result.exit_code == 0, result.output
            call_args = mock_mgr.get_service_endpoint_url.call_args[0][0]
            assert str(call_args).startswith("CONN_DB")

    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_fails_when_db_schema_unresolvable(
        self,
        mock_resolve,
        mock_get_entity,
        mock_ctx,
        runner,
        tmp_path,
    ):
        """Non-settings path errors when neither fqn nor connection has db/schema."""
        entity = Mock()
        fqn = Mock(database=None, schema=None)
        fqn.name = "MY_APP"
        entity.fqn = fqn
        mock_get_entity.return_value = entity
        mock_ctx.return_value.connection_context = Mock(database=None, schema=None)

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open"])
            assert result.exit_code == 1
            assert "Cannot resolve" in result.output

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch(
        "snowflake.cli._plugins.apps.commands.make_snowsight_url",
        return_value="https://app.snowflake.com/org/acct/#/apps/service/DB.SCHEMA.MY_APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_launches_snowsight(
        self,
        mock_resolve,
        mock_get_entity,
        mock_ctx,
        mock_snowsight,
        mock_launch,
        runner,
        tmp_path,
    ):
        entity = Mock()
        fqn = Mock(database="DB", schema="SCHEMA")
        fqn.name = "MY_APP"
        entity.fqn = fqn
        mock_get_entity.return_value = entity
        mock_ctx.return_value.connection = Mock()
        mock_ctx.return_value.connection_context = Mock(database="DB", schema="SCHEMA")

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings"])
            assert result.exit_code == 0, result.output
            assert "#/apps/service/DB.SCHEMA.MY_APP/details" in result.output
            mock_launch.assert_called_once()
            mock_snowsight.assert_called_once()
            path_arg = mock_snowsight.call_args[0][1]
            assert path_arg == "#/apps/service/DB.SCHEMA.MY_APP/details"

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch(
        "snowflake.cli._plugins.apps.commands.make_snowsight_url",
        return_value="https://app.snowflake.com/org/acct/#/apps/service/DB.SCHEMA.MY_APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_print_only(
        self,
        mock_resolve,
        mock_get_entity,
        mock_ctx,
        mock_snowsight,
        mock_launch,
        runner,
        tmp_path,
    ):
        entity = Mock()
        fqn = Mock(database="DB", schema="SCHEMA")
        fqn.name = "MY_APP"
        entity.fqn = fqn
        mock_get_entity.return_value = entity
        mock_ctx.return_value.connection = Mock()
        mock_ctx.return_value.connection_context = Mock(database="DB", schema="SCHEMA")

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings", "--print-only"])
            assert result.exit_code == 0, result.output
            assert "#/apps/service/DB.SCHEMA.MY_APP/details" in result.output
            mock_launch.assert_not_called()

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch(
        "snowflake.cli._plugins.apps.commands.make_snowsight_url",
        return_value="https://app.snowflake.com/org/acct/#/apps/service/CONN_DB.CONN_SCHEMA.MY_APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_falls_back_to_connection_context(
        self,
        mock_resolve,
        mock_get_entity,
        mock_ctx,
        mock_snowsight,
        mock_launch,
        runner,
        tmp_path,
    ):
        """When fqn.database/schema are None, fall back to connection_context."""
        entity = Mock()
        fqn = Mock(database=None, schema=None)
        fqn.name = "MY_APP"
        entity.fqn = fqn
        mock_get_entity.return_value = entity
        mock_ctx.return_value.connection = Mock()
        mock_ctx.return_value.connection_context = Mock(
            database="CONN_DB", schema="CONN_SCHEMA"
        )

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings"])
            assert result.exit_code == 0, result.output
            path_arg = mock_snowsight.call_args[0][1]
            assert path_arg == "#/apps/service/CONN_DB.CONN_SCHEMA.MY_APP/details"

    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_fails_when_database_and_schema_unresolvable(
        self,
        mock_resolve,
        mock_get_entity,
        mock_ctx,
        runner,
        tmp_path,
    ):
        """Error when neither fqn nor connection_context provides database/schema."""
        entity = Mock()
        fqn = Mock(database=None, schema=None)
        fqn.name = "MY_APP"
        entity.fqn = fqn
        mock_get_entity.return_value = entity
        mock_ctx.return_value.connection_context = Mock(database=None, schema=None)

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings"])
            assert result.exit_code == 1
            assert "Cannot resolve" in result.output
            assert "database" in result.output
            assert "schema" in result.output

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch(
        "snowflake.cli._plugins.apps.commands.make_snowsight_url",
        return_value="https://app.snowflake.com/org/acct/#/apps/service/MY%20DB.MY%20SCHEMA.MY%20APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_url_encodes_identifiers(
        self,
        mock_resolve,
        mock_get_entity,
        mock_ctx,
        mock_snowsight,
        mock_launch,
        runner,
        tmp_path,
    ):
        """Identifiers with special characters are URL-encoded in the settings URL."""
        entity = Mock()
        fqn = Mock(database='"MY DB"', schema='"MY SCHEMA"')
        fqn.name = '"MY APP"'
        entity.fqn = fqn
        mock_get_entity.return_value = entity
        mock_ctx.return_value.connection = Mock()
        mock_ctx.return_value.connection_context = Mock(
            database='"MY DB"', schema='"MY SCHEMA"'
        )

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings"])
            assert result.exit_code == 0, result.output
            path_arg = mock_snowsight.call_args[0][1]
            assert "MY%20DB" in path_arg
            assert "MY%20SCHEMA" in path_arg
            assert "MY%20APP" in path_arg


# ── Events CLI command tests ──────────────────────────────────────────


class TestEventsCommand:
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_events_returns_logs(
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
        mock_mgr.get_service_logs.return_value = "INFO: app started\nINFO: listening"

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "events"])
            assert result.exit_code == 0, result.output
            assert "app started" in result.output

        mock_mgr.get_service_logs.assert_called_once()

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_events_with_entity_id(
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
        mock_mgr.get_service_logs.return_value = ""

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "events", "--entity-id", "custom_app"])
            assert result.exit_code == 0, result.output
            mock_resolve.assert_called_once_with("custom_app")

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_events_last_flag(
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
        mock_mgr.get_service_logs.return_value = "line1"

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "events", "--last", "100"])
            assert result.exit_code == 0, result.output

        mock_mgr.get_service_logs.assert_called_once()
        _, kwargs = mock_mgr.get_service_logs.call_args
        assert kwargs["last"] == 100

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_events_service_not_found(
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
        mock_mgr.get_service_logs.side_effect = ProgrammingError("does not exist")

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "events"])
            assert result.exit_code == 1
            assert "Could not retrieve logs" in result.output
            assert "Verify that the app is deployed" in result.output


# ── Deploy CLI command tests ──────────────────────────────────────────


RESOLVE_DEPLOY_DEFAULTS = (
    "snowflake.cli._plugins.apps.commands._resolve_deploy_defaults"
)


class TestDeployCommand:
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
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_only_skips_upload_and_build_phase(
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
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_poll.return_value = {
            "url": "my-app.snowflakecomputing.app",
            "is_upgrading": "false",
        }

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy", "--deploy-only"])
            assert result.exit_code == 0, result.output
            mock_mgr.build_app_artifact_repo.assert_not_called()
            mock_mgr.artifact_repo_exists.assert_not_called()
            mock_mgr.create_app_service.assert_called_once()

    @patch("snowflake.cli._plugins.apps.commands._poll_until")
    @patch("snowflake.cli._plugins.apps.commands.StageManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": "SVC_POOL",
            "build_eai": "MY_EAI",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_artifact_repo_path(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
        mock_perform_bundle,
        mock_stage_manager_cls,
        mock_poll,
        runner,
        tmp_path,
    ):
        """Deploy uses build/run artifact repo APIs."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
        entity.build_image = None
        entity.execute_as_caller = False
        entity.artifact_repository = None
        entity.build_compute_pool = None
        entity.service_compute_pool = None
        entity.build_eai = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.artifact_repo_exists.return_value = False
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        mock_poll.side_effect = [
            "DONE",  # build status poll
            {
                "url": "my-app.snowflakecomputing.app",
                "is_upgrading": "false",
            },  # describe poll
        ]

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy"])
            assert result.exit_code == 0, result.output
            assert "my-app.snowflakecomputing.app" in result.output

        mock_mgr.artifact_repo_exists.assert_called_once()
        mock_mgr.create_artifact_repo.assert_called_once()
        mock_mgr.create_workspace.assert_called_once()
        mock_mgr.create_stage.assert_not_called()
        mock_mgr.build_app_artifact_repo.assert_called_once_with(
            source_uri=mock_mgr.workspace_uri.return_value,
            artifact_repo_fqn="TEST_DB.TEST_SCHEMA.MY_APP_REPO",
            app_id="MY_APP",
            compute_pool="BUILD_POOL",
            database="TEST_DB",
            schema="TEST_SCHEMA",
            runtime_image="runtime:latest",
            build_eai="MY_EAI",
        )
        mock_mgr.create_app_service.assert_called_once_with(
            service_fqn=FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP"),
            artifact_repo_fqn="TEST_DB.TEST_SCHEMA.MY_APP_REPO",
            package_name="MY_APP",
            compute_pool="SVC_POOL",
            version="LATEST",
            query_warehouse="WH",
            external_access_integrations=["MY_EAI"],
            comment='{"appId": "MY_APP"}',
        )
        assert mock_poll.call_count == 2

    @patch("snowflake.cli._plugins.apps.commands._poll_until")
    @patch("snowflake.cli._plugins.apps.commands.StageManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": "SVC_POOL",
            "build_eai": "MY_EAI",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_legacy_stage_flow_when_code_stage_set(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
        mock_perform_bundle,
        mock_stage_manager_cls,
        mock_poll,
        runner,
        tmp_path,
    ):
        """When only code_stage is set (and code_workspace is not), use the legacy stage flow."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = Mock(
            name="MY_STAGE",
            encryption_type="SNOWFLAKE_SSE",
            database=None,
            schema_=None,
        )
        entity.code_stage.name = "MY_STAGE"
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
        entity.build_image = None
        entity.execute_as_caller = False
        entity.artifact_repository = None
        entity.build_compute_pool = None
        entity.service_compute_pool = None
        entity.build_eai = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.stage_exists.return_value = False
        mock_mgr.artifact_repo_exists.return_value = False
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        mock_poll.side_effect = [
            "DONE",
            {
                "url": "my-app.snowflakecomputing.app",
                "is_upgrading": "false",
            },
        ]

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy"])
            assert result.exit_code == 0, result.output

        mock_mgr.create_stage.assert_called_once()
        mock_mgr.create_workspace.assert_not_called()
        mock_mgr.build_app_artifact_repo.assert_called_once_with(
            stage_fqn=FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_STAGE"),
            artifact_repo_fqn="TEST_DB.TEST_SCHEMA.MY_APP_REPO",
            app_id="MY_APP",
            compute_pool="BUILD_POOL",
            database="TEST_DB",
            schema="TEST_SCHEMA",
            runtime_image="runtime:latest",
            build_eai="MY_EAI",
        )

    @patch("snowflake.cli._plugins.apps.commands._poll_until")
    @patch("snowflake.cli._plugins.apps.commands.StageManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": "SVC_POOL",
            "build_eai": "MY_EAI",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_skips_create_when_artifact_repo_exists(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
        mock_perform_bundle,
        mock_stage_manager_cls,
        mock_poll,
        runner,
        tmp_path,
    ):
        """Deploy skips CREATE ARTIFACT REPOSITORY when repo already exists."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
        entity.build_image = None
        entity.execute_as_caller = False
        entity.artifact_repository = None
        entity.build_compute_pool = None
        entity.service_compute_pool = None
        entity.build_eai = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.artifact_repo_exists.return_value = True
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        mock_poll.side_effect = [
            "DONE",
            {
                "url": "my-app.snowflakecomputing.app",
                "is_upgrading": "false",
            },
        ]

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy"])
            assert result.exit_code == 0, result.output

        mock_mgr.artifact_repo_exists.assert_called_once()
        mock_mgr.create_artifact_repo.assert_not_called()
        mock_mgr.build_app_artifact_repo.assert_called_once()

    # ── Phase flag tests ──────────────────────────────────────────────

    def test_mutually_exclusive_phase_flags(self, runner, tmp_path):
        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy", "--upload-only", "--build-only"])
            assert result.exit_code == 1
            assert "Only one of" in result.output

            result = runner.invoke(["app", "deploy", "--upload-only", "--deploy-only"])
            assert result.exit_code == 1
            assert "Only one of" in result.output

            result = runner.invoke(["app", "deploy", "--build-only", "--deploy-only"])
            assert result.exit_code == 1
            assert "Only one of" in result.output

    @patch("snowflake.cli._plugins.apps.commands._poll_until")
    @patch("snowflake.cli._plugins.apps.commands.StageManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": "SVC_POOL",
            "build_eai": "MY_EAI",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_no_phase_flags_runs_all_phases(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
        mock_perform_bundle,
        mock_stage_manager_cls,
        mock_poll,
        runner,
        tmp_path,
    ):
        """Deploy with no phase flags runs upload, build, and deploy."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        entity.runtime_image = ""
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.stage_exists.return_value = False
        mock_mgr.artifact_repo_exists.return_value = False
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        mock_poll.side_effect = [
            "DONE",  # build status poll
            {
                "url": "my-app.snowflakecomputing.app",
                "is_upgrading": "false",
            },  # describe poll
        ]

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy"])
            assert result.exit_code == 0, result.output
            assert "App ready at" in result.output

        mock_mgr.create_workspace.assert_called_once()
        mock_perform_bundle.assert_called_once()
        mock_mgr.artifact_repo_exists.assert_called_once()
        mock_mgr.create_artifact_repo.assert_called_once()
        mock_mgr.build_app_artifact_repo.assert_called_once()
        mock_mgr.create_app_service.assert_called_once()

    @patch("snowflake.cli._plugins.apps.commands.StageManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": "SVC_POOL",
            "build_eai": "MY_EAI",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_upload_only_runs_upload_and_stops(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
        mock_perform_bundle,
        mock_stage_manager_cls,
        runner,
        tmp_path,
    ):
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        entity.code_stage = None
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        project_paths = ProjectPaths(project_root=tmp_path)
        mock_perform_bundle.return_value = project_paths

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.stage_exists.return_value = False

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy", "--upload-only"])
            assert result.exit_code == 0, result.output
            assert "Artifacts uploaded" in result.output

        mock_mgr.create_workspace.assert_called_once()
        mock_perform_bundle.assert_called_once()
        mock_mgr.build_app_artifact_repo.assert_not_called()
        mock_mgr.create_app_service.assert_not_called()

    @patch("snowflake.cli._plugins.apps.commands._poll_until")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": "SVC_POOL",
            "build_eai": "MY_EAI",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_build_only_runs_build_and_stops(
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
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        entity.runtime_image = ""
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.artifact_repo_exists.return_value = False
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        mock_poll.return_value = "DONE"

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy", "--build-only"])
            assert result.exit_code == 0, result.output
            assert "Build completed successfully" in result.output

        mock_mgr.artifact_repo_exists.assert_called_once()
        mock_mgr.create_artifact_repo.assert_called_once()
        mock_mgr.build_app_artifact_repo.assert_called_once()
        mock_mgr.create_app_service.assert_not_called()
        mock_mgr.stage_exists.assert_not_called()

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
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_only_runs_deploy_and_stops(
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
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_poll.return_value = {
            "url": "my-app.snowflakecomputing.app",
            "is_upgrading": "false",
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy", "--deploy-only"])
            assert result.exit_code == 0, result.output
            assert "App ready at" in result.output

        mock_mgr.create_app_service.assert_called_once()
        mock_mgr.build_app_artifact_repo.assert_not_called()
        mock_mgr.stage_exists.assert_not_called()

    @patch("snowflake.cli._plugins.apps.commands.StageManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": None,
            "build_compute_pool": None,
            "service_compute_pool": None,
            "build_eai": None,
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_upload_only_skips_build_and_deploy_validation(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
        mock_perform_bundle,
        mock_stage_manager_cls,
        runner,
        tmp_path,
    ):
        """--upload-only should not require build_compute_pool, service_compute_pool, or query_warehouse."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        entity.code_stage = None
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)
        mock_manager_cls.return_value.stage_exists.return_value = False

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy", "--upload-only"])
            assert result.exit_code == 0, result.output
            assert "build_compute_pool is required" not in result.output
            assert "service_compute_pool is required" not in result.output
            assert "query_warehouse is required" not in result.output

    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": None,
            "build_compute_pool": "BUILD_POOL",
            "service_compute_pool": None,
            "build_eai": None,
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "TEST_DB",
            "artifact_repo_schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_build_only_skips_deploy_validation(
        self, mock_resolve, mock_get_entity, mock_defaults, runner, tmp_path
    ):
        """--build-only should not require service_compute_pool or query_warehouse."""
        entity = Mock()
        entity.fqn = Mock(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        entity.code_stage = None
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy", "--build-only"])
            assert "service_compute_pool is required" not in result.output
            assert "query_warehouse is required" not in result.output
