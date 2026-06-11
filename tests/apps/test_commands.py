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

import io
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.apps.commands import (
    _CodeStorage,
    _log_service_logs,
    _make_build_log_streamer,
    _resolve_code_storage,
)
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
    _ts,
    app_fqn,
    is_personal_database,
    perform_bundle,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.metrics import CLIMetrics, CLIMetricsSpan
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.connector.cursor import DictCursor
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
CURRENT_ROLE = "snowflake.cli._plugins.apps.manager.SnowflakeAppManager.current_role"
GET_MISSING_PRIVILEGES = (
    "snowflake.cli._plugins.apps.manager.SnowflakeAppManager.get_missing_privileges"
)
GET_PERSONAL_DATABASE = (
    "snowflake.cli._plugins.apps.manager.SnowflakeAppManager.get_personal_database"
)
MANAGER_CLI_CONSOLE = "snowflake.cli._plugins.apps.manager.cli_console"


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
    entity so that ``@with_app_flow_routing()`` can detect the Snowflake App Runtime
    flow when the CLI is invoked from ``path``.
    """
    (path / "snowflake.yml").write_text(_SNOWFLAKE_APP_YML)


def _reset_command_metrics():
    get_cli_context_manager().metrics = CLIMetrics()


def _get_completed_span(span_name: str) -> dict:
    spans = get_cli_context_manager().metrics.completed_spans
    for span in spans:
        if span[CLIMetricsSpan.NAME_KEY] == span_name:
            return span
    raise AssertionError(
        f"Span {span_name!r} not found. Recorded spans: "
        f"{[span[CLIMetricsSpan.NAME_KEY] for span in spans]}"
    )


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


# ── _ts tests ─────────────────────────────────────────────────────────


class TestTs:
    """Tests for the _ts() timestamp helper used in polling messages."""

    @patch("snowflake.cli._plugins.apps.manager.time.strftime", return_value="12:34:56")
    def test_returns_formatted_time(self, mock_strftime):
        assert _ts() == "12:34:56"
        mock_strftime.assert_called_once_with("%H:%M:%S")

    @patch("snowflake.cli._plugins.apps.manager.time.strftime", return_value="00:00:00")
    def test_midnight_format(self, mock_strftime):
        assert _ts() == "00:00:00"


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
    def test_is_error_predicate_rewrites_timeout_wording(self, mock_sleep):
        with pytest.raises(CliError, match="Upgrade failed"):
            _poll_until(
                poll_fn=lambda: "FAILED",
                is_done=lambda v: v == "READY",
                is_error=lambda v: v == "FAILED",
                timeout_message="Upgrade timed out. Check logs:",
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
        with pytest.raises(CliError, match="failed"):
            _poll_until(
                poll_fn=lambda: "FAILED",
                done_states={"DONE"},
                error_states={"FAILED"},
                known_pending_states={"PENDING"},
                timeout_message="timed out",
            )

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_unknown_state_raises(self, mock_sleep):
        with pytest.raises(CliError, match="unexpected status=UNKNOWN"):
            _poll_until(
                poll_fn=lambda: "UNKNOWN",
                done_states={"DONE"},
                error_states={"FAILED"},
                known_pending_states={"PENDING"},
                timeout_message="timed out",
            )

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


class TestPollUntilOnPoll:
    """Tests for the ``on_poll`` callback used to stream log lines."""

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_on_poll_invoked_each_second(self, mock_sleep):
        """``on_poll`` runs once per second within each ``interval_seconds``."""
        on_poll = Mock()
        values = iter(["PENDING", "DONE"])
        _poll_until(
            poll_fn=lambda: next(values),
            done_states={"DONE"},
            error_states={"FAILED"},
            known_pending_states={"PENDING"},
            interval_seconds=3,
            timeout_message="timed out",
            on_poll=on_poll,
        )
        # 2 polling iterations × 3 seconds = 6 calls
        assert on_poll.call_count == 6
        assert mock_sleep.call_count == 6

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_on_poll_exception_swallowed(self, mock_sleep):
        """Exceptions raised by ``on_poll`` must not interrupt polling."""
        on_poll = Mock(side_effect=RuntimeError("transient"))
        values = iter(["PENDING", "DONE"])
        result = _poll_until(
            poll_fn=lambda: next(values),
            done_states={"DONE"},
            error_states={"FAILED"},
            known_pending_states={"PENDING"},
            interval_seconds=2,
            timeout_message="timed out",
            on_poll=on_poll,
        )
        assert result == "DONE"
        assert on_poll.call_count == 4

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    def test_on_poll_omitted_uses_single_sleep(self, mock_sleep):
        """Without ``on_poll``, ``_poll_until`` sleeps once per iteration."""
        values = iter(["PENDING", "DONE"])
        _poll_until(
            poll_fn=lambda: next(values),
            done_states={"DONE"},
            error_states={"FAILED"},
            known_pending_states={"PENDING"},
            interval_seconds=3,
            timeout_message="timed out",
        )
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(3)


class TestPollUntilStatusMessage:
    """Verify the per-iteration status step includes a ``[HH:MM:SS]`` prefix."""

    @patch("snowflake.cli._plugins.apps.manager.cli_console")
    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    @patch("snowflake.cli._plugins.apps.manager.time.strftime", return_value="10:00:00")
    def test_status_step_includes_timestamp(self, _mock_strftime, _mock_sleep, mock_cc):
        _poll_until(
            poll_fn=lambda: "DONE",
            done_states={"DONE"},
            error_states={"FAILED"},
            known_pending_states={"PENDING"},
            timeout_message="timed out",
        )
        mock_cc.step.assert_called_once_with("[10:00:00] Status: DONE")

    @patch("snowflake.cli._plugins.apps.manager.cli_console")
    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
    @patch("snowflake.cli._plugins.apps.manager.time.strftime", return_value="23:59:59")
    def test_status_step_timestamp_format(self, _mock_strftime, _mock_sleep, mock_cc):
        """Status line uses [HH:MM:SS] bracket format."""
        _poll_until(
            poll_fn=lambda: "DONE",
            done_states={"DONE"},
            error_states={"FAILED"},
            known_pending_states={"PENDING"},
            timeout_message="timed out",
        )
        step_arg = mock_cc.step.call_args[0][0]
        assert step_arg.startswith("[23:59:59] Status: ")


# ── Build log streamer tests ──────────────────────────────────────────


class TestMakeBuildLogStreamer:
    """Tests for ``_make_build_log_streamer`` incremental log diffing.

    The streamer is wired into ``snow app deploy`` as a ``_poll_until``
    ``on_poll`` callback so build logs stream to the user when they
    pass ``--verbose``.
    """

    def test_first_call_prints_all_lines(self):
        manager = Mock()
        manager.get_build_job_logs.return_value = ["line1", "line2", "line3"]
        fqn = FQN.from_string("DB.SCHEMA.BUILD_JOB")

        streamer = _make_build_log_streamer(manager, fqn)
        with patch("snowflake.cli._plugins.apps.commands.log") as mock_log:
            streamer()

        assert mock_log.info.call_count == 3
        mock_log.info.assert_any_call("line1")
        mock_log.info.assert_any_call("line2")
        mock_log.info.assert_any_call("line3")

    def test_subsequent_call_prints_only_new_lines(self):
        manager = Mock()
        fqn = FQN.from_string("DB.SCHEMA.BUILD_JOB")
        streamer = _make_build_log_streamer(manager, fqn)

        with patch("snowflake.cli._plugins.apps.commands.log") as mock_log:
            manager.get_build_job_logs.return_value = ["line1", "line2"]
            streamer()
            assert mock_log.info.call_count == 2

            mock_log.info.reset_mock()
            manager.get_build_job_logs.return_value = [
                "line1",
                "line2",
                "line3",
                "line4",
            ]
            streamer()
            assert mock_log.info.call_count == 2
            mock_log.info.assert_any_call("line3")
            mock_log.info.assert_any_call("line4")

    def test_no_new_lines_prints_nothing(self):
        manager = Mock()
        fqn = FQN.from_string("DB.SCHEMA.BUILD_JOB")
        streamer = _make_build_log_streamer(manager, fqn)

        with patch("snowflake.cli._plugins.apps.commands.log") as mock_log:
            manager.get_build_job_logs.return_value = ["line1"]
            streamer()
            mock_log.info.reset_mock()

            manager.get_build_job_logs.return_value = ["line1"]
            streamer()
            mock_log.info.assert_not_called()

    def test_empty_logs_prints_nothing(self):
        manager = Mock()
        manager.get_build_job_logs.return_value = []
        fqn = FQN.from_string("DB.SCHEMA.BUILD_JOB")
        streamer = _make_build_log_streamer(manager, fqn)

        with patch("snowflake.cli._plugins.apps.commands.log") as mock_log:
            streamer()
            mock_log.info.assert_not_called()

    def test_exception_is_swallowed(self):
        manager = Mock()
        manager.get_build_job_logs.side_effect = RuntimeError("connection lost")
        fqn = FQN.from_string("DB.SCHEMA.BUILD_JOB")
        streamer = _make_build_log_streamer(manager, fqn)

        streamer()  # should not raise

    def test_exception_does_not_reset_seen_count(self):
        manager = Mock()
        fqn = FQN.from_string("DB.SCHEMA.BUILD_JOB")
        streamer = _make_build_log_streamer(manager, fqn)

        with patch("snowflake.cli._plugins.apps.commands.log") as mock_log:
            manager.get_build_job_logs.return_value = ["line1", "line2"]
            streamer()
            assert mock_log.info.call_count == 2

            mock_log.info.reset_mock()
            manager.get_build_job_logs.side_effect = RuntimeError("transient")
            streamer()
            mock_log.info.assert_not_called()

            manager.get_build_job_logs.side_effect = None
            manager.get_build_job_logs.return_value = ["line1", "line2", "line3"]
            streamer()
            assert mock_log.info.call_count == 1
            mock_log.info.assert_called_with("line3")


class TestLogServiceLogs:
    """Tests for ``_log_service_logs`` line-by-line emission."""

    def test_logs_all_lines(self):
        manager = Mock()
        manager.get_service_logs.return_value = "line1\nline2\nline3"
        fqn = FQN.from_string("DB.SCHEMA.MY_APP")

        with patch("snowflake.cli._plugins.apps.commands.log") as mock_log:
            _log_service_logs(manager, fqn)

        assert mock_log.info.call_count == 3
        mock_log.info.assert_any_call("line1")
        mock_log.info.assert_any_call("line2")
        mock_log.info.assert_any_call("line3")

    def test_empty_logs_print_nothing(self):
        manager = Mock()
        manager.get_service_logs.return_value = ""
        fqn = FQN.from_string("DB.SCHEMA.MY_APP")

        with patch("snowflake.cli._plugins.apps.commands.log") as mock_log:
            _log_service_logs(manager, fqn)
            mock_log.info.assert_not_called()

    def test_exception_is_swallowed(self):
        manager = Mock()
        manager.get_service_logs.side_effect = RuntimeError("connection lost")
        fqn = FQN.from_string("DB.SCHEMA.MY_APP")

        _log_service_logs(manager, fqn)  # should not raise


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
        result = _generate_snowflake_yml(
            "my_app", self._BASE_RESOLVED, use_workspace=True
        )
        assert "type: snowflake-app" in result
        assert "name: MY_APP" in result
        assert "database: TEST_DB" in result
        assert "schema: SNOW_APPS" in result
        assert "query_warehouse: TEST_WH" in result
        assert "name: MY_POOL" in result
        assert "name: MY_EAI" in result
        # code_workspace is a shared workspace, fully-qualified.
        assert "code_workspace: TEST_DB.SNOW_APPS.SNOWFLAKE_APPS" in result
        assert "code_stage:" not in result
        assert "artifact_repository" not in result

    def test_generates_yml_with_code_stage_when_not_using_workspace(self):
        result = _generate_snowflake_yml(
            "my_app", self._BASE_RESOLVED, use_workspace=False
        )
        assert "code_stage: MY_APP_CODE" in result
        assert "code_workspace" not in result

    def test_no_null_values_in_output(self):
        result = _generate_snowflake_yml(
            "my_app", self._BASE_RESOLVED, use_workspace=True
        )
        assert "null" not in result

    def test_build_eai_omitted_when_missing(self):
        """When ``build_eai`` is missing, the generated YAML has no
        ``build_eai`` block — the field is optional."""
        resolved = {**self._BASE_RESOLVED, "build_eai": None}
        result = _generate_snowflake_yml("my_app", resolved, use_workspace=False)
        assert "build_eai" not in result
        assert "None" not in result

    def test_build_eai_omitted_when_missing_key(self):
        """When ``build_eai`` is not in the resolved dict at all, the
        generated YAML still works and omits the block."""
        resolved = {k: v for k, v in self._BASE_RESOLVED.items() if k != "build_eai"}
        result = _generate_snowflake_yml("my_app", resolved, use_workspace=False)
        assert "build_eai" not in result

    def test_build_compute_pool_omitted_when_none(self):
        """When ``build_compute_pool`` is None (e.g. account opted into a
        managed build compute pool), the generated YAML omits the
        ``build_compute_pool`` block but still emits
        ``service_compute_pool``."""
        resolved = {**self._BASE_RESOLVED, "build_compute_pool": None}
        result = _generate_snowflake_yml("my_app", resolved, use_workspace=False)
        assert "build_compute_pool" not in result
        assert "service_compute_pool" in result
        assert "None" not in result

    def test_build_compute_pool_omitted_when_missing_key(self):
        """``build_compute_pool`` may be omitted from the resolved dict
        entirely; the generated YAML still produces a valid project."""
        resolved = {
            k: v for k, v in self._BASE_RESOLVED.items() if k != "build_compute_pool"
        }
        result = _generate_snowflake_yml("my_app", resolved, use_workspace=False)
        assert "build_compute_pool" not in result
        assert "service_compute_pool" in result

    def test_service_compute_pool_omitted_when_none(self):
        """When ``service_compute_pool`` is None the generated YAML omits
        the ``service_compute_pool`` block."""
        resolved = {**self._BASE_RESOLVED, "service_compute_pool": None}
        result = _generate_snowflake_yml("my_app", resolved, use_workspace=False)
        assert "service_compute_pool" not in result
        assert "None" not in result

    def test_both_compute_pools_omitted_when_none(self):
        """The managed-compute-pool flow omits both pool blocks while still
        producing a valid YAML body."""
        resolved = {
            **self._BASE_RESOLVED,
            "build_compute_pool": None,
            "service_compute_pool": None,
        }
        result = _generate_snowflake_yml("my_app", resolved, use_workspace=False)
        assert "build_compute_pool" not in result
        assert "service_compute_pool" not in result
        # Ensure neighbouring blocks are still emitted correctly.
        assert "query_warehouse: TEST_WH" in result
        assert "build_eai" in result

    def test_yml_without_compute_pools_is_valid_project_definition(self):
        """A generated YAML without either compute-pool block still parses
        cleanly into a project definition."""
        import yaml
        from snowflake.cli.api.utils.definition_rendering import (
            render_definition_template,
        )

        resolved = {
            **self._BASE_RESOLVED,
            "build_compute_pool": None,
            "service_compute_pool": None,
        }
        raw_yml = _generate_snowflake_yml("my_app", resolved, use_workspace=False)
        definition_input = yaml.safe_load(raw_yml)
        result = render_definition_template(definition_input, {})
        entity = result.project_definition.entities["my_app"]

        assert entity.type == "snowflake-app"
        assert entity.build_compute_pool is None
        assert entity.service_compute_pool is None

    def test_custom_schema(self):
        resolved = {**self._BASE_RESOLVED, "schema": "CFG_SCHEMA"}
        result = _generate_snowflake_yml("my_app", resolved, use_workspace=True)
        assert "schema: CFG_SCHEMA" in result

    def test_generated_yml_is_valid_project_definition(self):
        """Generated YAML is parsable and produces a valid project definition."""
        import yaml
        from snowflake.cli.api.utils.definition_rendering import (
            render_definition_template,
        )

        raw_yml = _generate_snowflake_yml(
            "my_app", self._BASE_RESOLVED, use_workspace=True
        )
        definition_input = yaml.safe_load(raw_yml)

        result = render_definition_template(definition_input, {})
        project = result.project_definition
        entity = project.entities["my_app"]

        assert entity.type == "snowflake-app"
        assert entity.query_warehouse == "TEST_WH"
        # code_workspace points at the shared SNOWFLAKE_APPS workspace and
        # the validator parses it back into a ``CodeWorkspaceReference`` with
        # db/schema set.
        assert entity.code_workspace.name == "SNOWFLAKE_APPS"
        assert entity.code_workspace.database == "TEST_DB"
        assert entity.code_workspace.schema_ == "SNOW_APPS"
        assert entity.code_stage is None
        assert entity.artifacts is not None

    def test_generated_yml_with_stage_is_valid_project_definition(self):
        """When ``use_workspace`` is false, ``code_stage`` is a bare name."""
        import yaml
        from snowflake.cli.api.utils.definition_rendering import (
            render_definition_template,
        )

        raw_yml = _generate_snowflake_yml(
            "my_app", self._BASE_RESOLVED, use_workspace=False
        )
        definition_input = yaml.safe_load(raw_yml)

        result = render_definition_template(definition_input, {})
        entity = result.project_definition.entities["my_app"]

        assert entity.code_stage.name == "MY_APP_CODE"
        assert entity.code_stage.database is None
        assert entity.code_stage.schema_ is None
        assert entity.code_workspace is None


# ── SnowflakeAppManager tests ─────────────────────────────────────────


class TestSnowflakeAppManagerQuerySpinner:
    def test_execute_query_wraps_query_with_spinner_when_interactive(self):
        manager = SnowflakeAppManager(interactive=True)
        with patch(
            "snowflake.cli._plugins.apps.manager.cli_console.spinner"
        ) as mock_spinner, patch(
            "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"
        ) as mock_super_execute:
            cursor = Mock()
            spinner = Mock()
            mock_super_execute.return_value = cursor
            mock_spinner.return_value.__enter__.return_value = spinner

            result = manager.execute_query("SELECT 1", cursor_class=DictCursor)

            assert result is cursor
            mock_spinner.assert_called_once_with()
            spinner.add_task.assert_called_once_with(
                description="",
                total=None,
            )
            mock_super_execute.assert_called_once_with(
                "SELECT 1", cursor_class=DictCursor
            )

    def test_execute_query_skips_spinner_when_non_interactive(self):
        manager = SnowflakeAppManager(interactive=False)
        with patch(
            "snowflake.cli._plugins.apps.manager.cli_console.spinner"
        ) as mock_spinner, patch(
            "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"
        ) as mock_super_execute:
            cursor = Mock()
            mock_super_execute.return_value = cursor

            result = manager.execute_query("SELECT 1", cursor_class=DictCursor)

            assert result is cursor
            mock_spinner.assert_not_called()
            mock_super_execute.assert_called_once_with(
                "SELECT 1", cursor_class=DictCursor
            )

    def test_execute_query_falls_back_to_tty_detection_when_unset(self):
        manager = SnowflakeAppManager()
        with patch(
            "snowflake.cli._plugins.apps.manager.is_tty_interactive",
            return_value=False,
        ), patch(
            "snowflake.cli._plugins.apps.manager.cli_console.spinner"
        ) as mock_spinner, patch(
            "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"
        ) as mock_super_execute:
            cursor = Mock()
            mock_super_execute.return_value = cursor

            result = manager.execute_query("SELECT 1", cursor_class=DictCursor)

            assert result is cursor
            mock_spinner.assert_not_called()
            mock_super_execute.assert_called_once_with(
                "SELECT 1", cursor_class=DictCursor
            )


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
        # Single quotes are escaped by doubling them (''), not by backslash prefix.
        assert "'BAD''DB'" in query
        assert "BAD\\'DB" not in query


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


class TestCurrentRole:
    @patch(EXECUTE_QUERY)
    def test_returns_role(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ("ENGINEER",)
        mock_execute.return_value = cursor

        assert SnowflakeAppManager().current_role() == "ENGINEER"
        assert mock_execute.call_args[0][0] == "SELECT CURRENT_ROLE()"

    @patch(EXECUTE_QUERY)
    def test_returns_none_when_unset(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = (None,)
        mock_execute.return_value = cursor

        assert SnowflakeAppManager().current_role() is None

    @patch(EXECUTE_QUERY, side_effect=ProgrammingError("boom"))
    def test_returns_none_on_error(self, mock_execute):
        assert SnowflakeAppManager().current_role() is None


class TestGetMissingPrivileges:
    @patch(EXECUTE_QUERY)
    def test_authorized_returns_empty(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ('{"authorized": true}',)
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().get_missing_privileges(
            "CREATE STAGE APPS.PUBLIC.x", "ENGINEER"
        )
        assert result == []
        query = mock_execute.call_args[0][0]
        assert "CALL EXPLAIN_PRIVILEGES(" in query
        assert "statement => 'CREATE STAGE APPS.PUBLIC.x'" in query
        assert "missing_only => true" in query
        assert "for_role => 'ENGINEER'" in query

    @patch(EXECUTE_QUERY)
    def test_returns_flattened_missing_nodes(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = (
            '{"allOf": [{"privilege": "USAGE", "objectType": "DATABASE", '
            '"objectName": "APPS"}]}',
        )
        mock_execute.return_value = cursor

        result = SnowflakeAppManager().get_missing_privileges(
            "CREATE STAGE APPS.PUBLIC.x", "ENGINEER"
        )
        assert result == [
            {"privilege": "USAGE", "objectType": "DATABASE", "objectName": "APPS"}
        ]

    @patch(EXECUTE_QUERY)
    def test_omits_for_role_when_role_is_none(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ('{"authorized": true}',)
        mock_execute.return_value = cursor

        SnowflakeAppManager().get_missing_privileges("CREATE STAGE APPS.PUBLIC.x")
        query = mock_execute.call_args[0][0]
        assert "for_role" not in query

    @patch(EXECUTE_QUERY)
    def test_escapes_statement(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = ('{"authorized": true}',)
        mock_execute.return_value = cursor

        SnowflakeAppManager().get_missing_privileges(
            'CREATE STAGE "USER$x".PUBLIC.y', "ENGINEER"
        )
        query = mock_execute.call_args[0][0]
        # Quoted personal-database identifiers survive inside the SQL literal.
        assert '"USER$x".PUBLIC.y' in query

    @patch(EXECUTE_QUERY)
    def test_empty_response_returns_empty(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        assert (
            SnowflakeAppManager().get_missing_privileges("CREATE STAGE A.B.C", "R")
            == []
        )


class TestAppFqn:
    """``app_fqn`` is the apps-plugin FQN factory: it routes each component
    through :func:`to_identifier` so the resulting FQN's ``identifier`` /
    ``sql_identifier`` / ``prefix`` produce valid SQL even for personal
    databases (``USER$first.last@domain.com``) and other components with
    characters illegal in unquoted Snowflake identifiers.

    The shared :class:`FQN` API is intentionally untouched, so the apps
    plugin opts in by constructing FQNs through this factory at the
    handful of entry points where database / schema / name strings cross
    into apps code.
    """

    @pytest.mark.parametrize(
        "database, schema, name, expected_identifier",
        [
            # No-regression: plain unquoted names pass through unchanged.
            ("DB", "SCHEMA", "OBJ", "DB.SCHEMA.OBJ"),
            ("USER$ADMIN", "APPS", "MY_APP", "USER$ADMIN.APPS.MY_APP"),
            # Personal databases — dotted + ``@``.
            (
                "USER$GUY.BLOOM@SNOWFLAKE.COM",
                "APPS",
                "MY_APP",
                '"USER$GUY.BLOOM@SNOWFLAKE.COM".APPS.MY_APP',
            ),
            (
                "USER$guy.bloom@snowflake.com",
                "APPS",
                "MY_APP",
                '"USER$guy.bloom@snowflake.com".APPS.MY_APP',
            ),
            # Dotted schema and dotted name should also be quoted.
            ("DB", "dotted.schema", "MY_APP", 'DB."dotted.schema".MY_APP'),
            ("DB", "APPS", "dotted.name", 'DB.APPS."dotted.name"'),
            # Already-quoted components are passed through unchanged.
            ('"already.quoted"', "APPS", "MY_APP", '"already.quoted".APPS.MY_APP'),
            # Optional database / schema.
            (None, "SCHEMA", "MY_APP", "SCHEMA.MY_APP"),
            (None, None, "MY_APP", "MY_APP"),
        ],
    )
    def test_identifier(self, database, schema, name, expected_identifier):
        fqn = app_fqn(database=database, schema=schema, name=name)
        assert fqn.identifier == expected_identifier

    def test_sql_identifier_quotes_personal_database(self):
        fqn = app_fqn(
            database="USER$GUY.BLOOM@SNOWFLAKE.COM",
            schema="APPS",
            name="SNOWFLAKE_APPS",
        )
        assert fqn.sql_identifier == (
            "IDENTIFIER('\"USER$GUY.BLOOM@SNOWFLAKE.COM\".APPS.SNOWFLAKE_APPS')"
        )

    def test_sql_identifier_no_regression_for_simple_names(self):
        fqn = app_fqn(database="DB", schema="SCHEMA", name="OBJ")
        assert fqn.sql_identifier == "IDENTIFIER('DB.SCHEMA.OBJ')"

    def test_prefix_quotes_personal_database(self):
        fqn = app_fqn(
            database="USER$guy.bloom@snowflake.com",
            schema="APPS",
            name="X",
        )
        assert fqn.prefix == '"USER$guy.bloom@snowflake.com".APPS'


class TestSnowflakeAppManager:
    @patch(EXECUTE_QUERY)
    def test_get_personal_database_preserves_case(self, mock_execute):
        """Snowflake users created as quoted identifiers (e.g.
        ``"first.last@domain.com"``) keep their original case, and so do
        their personal databases (``USER$first.last@domain.com``). Because
        :func:`app_fqn` later wraps the database in a *case-sensitive*
        quoted identifier, ``get_personal_database`` must return the
        value from ``CURRENT_USER()`` verbatim — folding it to upper case
        would silently target a non-existent database for these users.
        """
        cursor = Mock()
        cursor.fetchone.return_value = ("USER$guy.bloom@snowflake.com",)
        mock_execute.return_value = cursor

        assert (
            SnowflakeAppManager().get_personal_database()
            == "USER$guy.bloom@snowflake.com"
        )

    @patch(EXECUTE_QUERY)
    def test_get_personal_database_returns_uppercase_users_unchanged(
        self, mock_execute
    ):
        """Unquoted Snowflake usernames are folded to upper case at
        creation, so ``CURRENT_USER()`` already returns them in upper
        case. Verify the normal path still works after dropping the
        defensive ``.upper()`` call.
        """
        cursor = Mock()
        cursor.fetchone.return_value = ("USER$ADMIN",)
        mock_execute.return_value = cursor

        assert SnowflakeAppManager().get_personal_database() == "USER$ADMIN"

    @patch(EXECUTE_QUERY)
    def test_get_personal_database_returns_none_when_user_missing(self, mock_execute):
        """``CURRENT_USER()`` returns an empty string in unauthenticated
        contexts, producing a bare ``USER$`` which is not a real database.
        """
        cursor = Mock()
        cursor.fetchone.return_value = ("USER$",)
        mock_execute.return_value = cursor

        assert SnowflakeAppManager().get_personal_database() is None

    @pytest.mark.parametrize(
        "database, expected",
        [
            ("USER$ADMIN", True),
            ("user$admin", True),  # prefix match is case-insensitive
            ("USER$guy.bloom@snowflake.com", True),
            ('"USER$guy.bloom@snowflake.com"', True),  # quoted identifier
            ('"USER$first.last@x.com"', True),
            ("TEST_DB", False),
            ("MY_USER_DB", False),  # USER$ prefix only, not substring
            ("DB$USER", False),
            ("", False),
            (None, False),
        ],
    )
    def test_is_personal_database(self, database, expected):
        """Personal databases are named ``USER$<user>`` and must be detected
        regardless of identifier quoting so the deploy flow routes them to a
        workspace (stages are unsupported in personal databases)."""
        assert is_personal_database(database) is expected

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
    def test_create_workspace_ensures_live_version(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="WORKSPACE")
        SnowflakeAppManager().create_workspace(fqn)
        assert [c[0][0] for c in mock_execute.call_args_list] == [
            "CREATE WORKSPACE IF NOT EXISTS IDENTIFIER('DB.SCHEMA.WORKSPACE')",
            (
                "ALTER WORKSPACE IDENTIFIER('DB.SCHEMA.WORKSPACE') "
                "ADD LIVE VERSION FROM LAST"
            ),
        ]

    @patch(EXECUTE_QUERY)
    def test_commit_workspace_live_version(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="WORKSPACE")
        SnowflakeAppManager().commit_workspace_live_version(fqn)
        mock_execute.assert_called_once_with(
            "ALTER WORKSPACE IDENTIFIER('DB.SCHEMA.WORKSPACE') COMMIT"
        )

    @patch(EXECUTE_QUERY)
    def test_ensure_workspace_live_version(self, mock_execute):
        fqn = FQN(database="DB", schema="SCHEMA", name="WORKSPACE")
        SnowflakeAppManager().ensure_workspace_live_version(fqn)
        mock_execute.assert_called_once_with(
            "ALTER WORKSPACE IDENTIFIER('DB.SCHEMA.WORKSPACE') "
            "ADD LIVE VERSION FROM LAST"
        )

    @patch(EXECUTE_QUERY)
    def test_ensure_workspace_live_version_ignores_duplicate_live_version_error(
        self, mock_execute
    ):
        fqn = FQN(database="DB", schema="SCHEMA", name="WORKSPACE")
        mock_execute.side_effect = ProgrammingError(
            "099106 (42710): There is already a live version"
        )
        SnowflakeAppManager().ensure_workspace_live_version(fqn)

    @patch(EXECUTE_QUERY)
    def test_ensure_workspace_live_version_raises_unexpected_programming_error(
        self, mock_execute
    ):
        fqn = FQN(database="DB", schema="SCHEMA", name="WORKSPACE")
        mock_execute.side_effect = ProgrammingError("some other error")
        with pytest.raises(ProgrammingError):
            SnowflakeAppManager().ensure_workspace_live_version(fqn)

    def test_workspace_last_uri(self):
        fqn = FQN(database="DB", schema="SCHEMA", name="WORKSPACE")
        assert (
            SnowflakeAppManager().workspace_last_uri(fqn)
            == "snow://workspace/DB.SCHEMA.WORKSPACE/versions/last"
        )

    def test_workspace_last_subdirectory_uri_normalizes_directory_name(self):
        fqn = FQN(database="DB", schema="SCHEMA", name="WORKSPACE")
        assert (
            SnowflakeAppManager().workspace_last_subdirectory_uri(fqn, "/MY_APP/")
            == "snow://workspace/DB.SCHEMA.WORKSPACE/versions/last/MY_APP"
        )

    @patch(EXECUTE_QUERY)
    def test_upload_to_workspace_builds_native_file_uri(self, mock_execute, tmp_path):
        """The PUT source must come from the native local path via
        ``_local_path_to_file_uri`` and be embedded without an extra layer of
        quoting. Using ``Path.as_posix()`` here produced ``file://C:/...`` on
        Windows, which the connector rejects with error 253006."""
        from snowflake.cli._plugins.apps.manager import _local_path_to_file_uri

        (tmp_path / "app.py").write_text("print('hi')")
        fqn = FQN(database="DB", schema="SCHEMA", name="WORKSPACE")

        results = list(
            SnowflakeAppManager().upload_to_workspace(
                local_root=tmp_path,
                workspace_fqn=fqn,
                target_subdirectory="MY_APP",
                overwrite=True,
            )
        )

        assert [r["source"] for r in results] == ["app.py"]
        expected_uri = _local_path_to_file_uri(str((tmp_path / "app.py").resolve()))
        put_query = mock_execute.call_args_list[0][0][0]
        assert put_query == (
            f"PUT {expected_uri} "
            f"'snow://workspace/DB.SCHEMA.WORKSPACE/versions/live/MY_APP/' "
            f"auto_compress=false overwrite=true"
        )
        # The helper output is embedded directly, never re-wrapped in another
        # string literal (which would yield an invalid ``PUT ''file://...''``).
        assert "''file://" not in put_query

    @pytest.mark.parametrize(
        "native_path,expected_uri",
        [
            # Windows drive path keeps native backslashes; allowed unquoted so
            # returned bare. The previous as_posix() form ``file://C:/...`` was
            # the bug.
            ("C:\\Users\\dev\\bundle\\app.py", "file://C:\\Users\\dev\\bundle\\app.py"),
            # A space forces a quoted literal with doubled backslashes.
            (
                "C:\\My Apps\\bundle\\app.py",
                "'file://C:\\\\My Apps\\\\bundle\\\\app.py'",
            ),
            # POSIX absolute path yields the valid three-slash form.
            ("/home/dev/bundle/app.py", "file:///home/dev/bundle/app.py"),
        ],
    )
    def test_local_path_to_file_uri(self, native_path, expected_uri):
        from snowflake.cli._plugins.apps.manager import _local_path_to_file_uri

        assert _local_path_to_file_uri(native_path) == expected_uri

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
    def test_build_app_artifact_repo_defaults_project_type_to_empty_string(
        self, mock_execute
    ):
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
        assert ", '', '{}')" in build_query
        assert "'nodejs'" not in build_query

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
        # Single quotes are escaped by doubling (Snowflake's native escape mechanism);
        # the raw payload 'app\'injection' must not appear as a literal unescaped quote.
        assert "'app''injection'" in build_query
        assert "app'injection'," not in build_query
        assert "app'injection)" not in build_query

    def test_build_app_artifact_repo_requires_repo_and_app_id(self):
        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        mgr = SnowflakeAppManager()
        with pytest.raises(ValueError, match="artifact_repo_fqn"):
            mgr.build_app_artifact_repo(
                stage_fqn=stage_fqn,
                artifact_repo_fqn="",
                app_id="my_app",
                database="DB",
                schema="SCHEMA",
            )
        with pytest.raises(ValueError, match="app_id"):
            mgr.build_app_artifact_repo(
                stage_fqn=stage_fqn,
                artifact_repo_fqn="DB.SCHEMA.REPO",
                app_id="  ",
                database="DB",
                schema="SCHEMA",
            )

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
    def test_use_database_and_schema_quotes_personal_database(self, mock_execute):
        """Personal databases like ``USER$guy.bloom@snowflake.com`` contain
        dots and ``@`` so the ``USE DATABASE`` / ``USE SCHEMA`` statements
        emitted by ``_use_database_and_schema`` must wrap them in double
        quotes. Without the quoting Snowflake parses the value as multiple
        identifier parts and fails with ``invalid identifier`` /
        ``syntax error``.
        """
        cursor = Mock()
        cursor.fetchone.side_effect = [
            (None,),  # CURRENT_DATABASE() — no previous session DB
            (None,),  # CURRENT_SCHEMA()
            None,  # USE DATABASE
            None,  # USE SCHEMA
            ("Build job submitted: DB.SCHEMA.JOB",),
        ]
        mock_execute.return_value = cursor

        stage_fqn = FQN(database="DB", schema="APPS", name="STAGE")
        SnowflakeAppManager().build_app_artifact_repo(
            stage_fqn=stage_fqn,
            artifact_repo_fqn='"USER$guy.bloom@snowflake.com".APPS.IMAGE_REPO',
            app_id="my_app",
            compute_pool="BUILD_POOL",
            database="USER$guy.bloom@snowflake.com",
            schema="APPS",
            runtime_image="runtime:latest",
        )
        queries = [c[0][0] for c in mock_execute.call_args_list]
        assert (
            'USE DATABASE "USER$guy.bloom@snowflake.com"' in queries
        ), f"Expected quoted USE DATABASE, got queries: {queries}"
        assert "USE SCHEMA APPS" in queries

    @patch(EXECUTE_QUERY)
    def test_use_database_and_schema_quotes_personal_database_on_restore(
        self, mock_execute
    ):
        """``CURRENT_DATABASE()`` / ``CURRENT_SCHEMA()`` return the raw
        (unquoted) identifier, so the restore-session ``USE DATABASE`` /
        ``USE SCHEMA`` must quote the previous values too — otherwise we
        crash *after* the build with the same parser error.
        """
        cursor = Mock()
        cursor.fetchone.side_effect = [
            ("USER$guy.bloom@snowflake.com",),  # CURRENT_DATABASE()
            ("dotted.schema",),  # CURRENT_SCHEMA()
            None,  # USE DATABASE
            None,  # USE SCHEMA
            ("Build job submitted: DB.SCHEMA.JOB",),
            None,  # restore USE DATABASE
            None,  # restore USE SCHEMA
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
        assert 'USE DATABASE "USER$guy.bloom@snowflake.com"' in queries
        assert 'USE SCHEMA "dotted.schema"' in queries

    @patch(EXECUTE_QUERY)
    def test_build_app_artifact_repo_logs_arguments(self, mock_execute, caplog):
        """``--verbose`` users need to see the resolved arguments handed to
        ``SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO`` so they can diagnose
        quoting / escaping / config issues without re-running with
        ``--debug`` and reading raw connector logs."""
        import logging

        cursor = Mock()
        cursor.fetchone.side_effect = [
            (None,),
            (None,),
            None,
            None,
            ("Build job submitted: DB.SCHEMA.JOB",),
        ]
        mock_execute.return_value = cursor

        stage_fqn = FQN(database="DB", schema="SCHEMA", name="STAGE")
        with caplog.at_level(
            logging.INFO, logger="snowflake.cli._plugins.apps.manager"
        ):
            SnowflakeAppManager().build_app_artifact_repo(
                stage_fqn=stage_fqn,
                artifact_repo_fqn="DB.SCHEMA.REPO",
                app_id="my_app",
                compute_pool="BUILD_POOL",
                database="DB",
                schema="SCHEMA",
                runtime_image="runtime:latest",
                build_eai="MY_EAI",
                project_type="nodejs",
            )

        log_record = next(
            (
                r
                for r in caplog.records
                if "SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO" in r.getMessage()
            ),
            None,
        )
        assert log_record is not None, (
            f"No SPCS_TEST_BUILD_APP_ARTIFACT_REPO log emitted; "
            f"got: {[r.getMessage() for r in caplog.records]}"
        )
        assert log_record.levelno == logging.INFO
        msg = log_record.getMessage()
        assert "source_uri='@DB.SCHEMA.STAGE'" in msg
        assert "artifact_repo_fqn='DB.SCHEMA.REPO'" in msg
        assert "app_id='my_app'" in msg
        assert "compute_pool='BUILD_POOL'" in msg
        assert "runtime_image='runtime:latest'" in msg
        assert "project_type='nodejs'" in msg
        assert "MY_EAI" in msg
        assert "database='DB'" in msg
        assert "schema='SCHEMA'" in msg

    @patch(EXECUTE_QUERY)
    def test_create_artifact_repo_quotes_personal_database(self, mock_execute):
        """``create_artifact_repo`` builds its FQN via :func:`app_fqn` so a
        personal database name is wrapped in double quotes before being
        embedded in ``IDENTIFIER('...')``."""
        SnowflakeAppManager().create_artifact_repo(
            database="USER$guy.bloom@snowflake.com",
            schema="APPS",
            repo_name="MY_APP_REPO",
        )
        mock_execute.assert_called_once_with(
            "CREATE ARTIFACT REPOSITORY IF NOT EXISTS "
            "IDENTIFIER('\"USER$guy.bloom@snowflake.com\".APPS.MY_APP_REPO') "
            "TYPE=APPLICATION"
        )

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

    @patch(OBJECT_EXISTS)
    @patch(EXECUTE_QUERY)
    def test_is_application_service_true_when_describe_app_service_succeeds(
        self, mock_execute, mock_object_exists
    ):
        cursor = Mock()
        cursor.fetchone.return_value = {"URL": "my-app.snowflakecomputing.app"}
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        assert SnowflakeAppManager().is_application_service(fqn) is True
        mock_object_exists.assert_not_called()

    @patch(OBJECT_EXISTS, return_value=True)
    @patch(EXECUTE_QUERY)
    def test_is_application_service_false_when_legacy_service_exists(
        self, mock_execute, mock_object_exists
    ):
        mock_execute.side_effect = ProgrammingError("object does not exist")

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        assert SnowflakeAppManager().is_application_service(fqn) is False
        mock_object_exists.assert_called_once_with("service", "DB.SCHEMA.my_app")

    @patch(OBJECT_EXISTS, return_value=True)
    @patch(EXECUTE_QUERY)
    def test_is_application_service_false_when_describe_returns_no_rows(
        self, mock_execute, mock_object_exists
    ):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        assert SnowflakeAppManager().is_application_service(fqn) is False
        mock_object_exists.assert_called_once_with("service", "DB.SCHEMA.my_app")

    @patch(OBJECT_EXISTS, return_value=False)
    @patch(EXECUTE_QUERY)
    def test_is_application_service_defaults_to_true_on_check_failure(
        self, mock_execute, mock_object_exists
    ):
        mock_execute.side_effect = ProgrammingError("permission denied")

        fqn = FQN(database="DB", schema="SCHEMA", name="my_app")
        assert SnowflakeAppManager().is_application_service(fqn) is True
        mock_object_exists.assert_called_once_with("service", "DB.SCHEMA.my_app")

    def test_resolve_application_service_url_from_describe(self):
        mgr = SnowflakeAppManager()
        assert mgr.resolve_application_service_url_from_describe({}) is None
        assert (
            mgr.resolve_application_service_url_from_describe(
                {"url": "x.snowflakecomputing.app", "is_upgrading": "false"}
            )
            == "https://x.snowflakecomputing.app"
        )
        assert (
            mgr.resolve_application_service_url_from_describe(
                {"url": "https://x.snowflakecomputing.app", "is_upgrading": "false"}
            )
            == "https://x.snowflakecomputing.app"
        )
        assert (
            mgr.resolve_application_service_url_from_describe(
                {"url": "", "is_upgrading": "false"}
            )
            is None
        )
        assert (
            mgr.resolve_application_service_url_from_describe(
                {"url": "x.app", "is_upgrading": "true"}
            )
            is None
        )

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
    def test_artifact_repo_exists_quotes_database_and_schema_independently(
        self, mock_execute
    ):
        mock_cursor = Mock()
        mock_cursor.__iter__ = Mock(return_value=iter([]))
        mock_execute.return_value = mock_cursor

        SnowflakeAppManager().artifact_repo_exists(
            database="my db", schema='my "schema"', repo_name="MY_REPO"
        )

        query = mock_execute.call_args[0][0]
        assert 'IN SCHEMA "my db"."my ""schema"""' in query
        assert "IDENTIFIER(" not in query
        assert mock_execute.call_args.kwargs["cursor_class"] is DictCursor

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

    @staticmethod
    def _build_show_containers_cursor(rows):
        cursor = Mock()
        cursor.__iter__ = Mock(return_value=iter(rows))
        return cursor

    @staticmethod
    def _build_logs_cursor(value):
        cursor = Mock()
        cursor.fetchone.return_value = value
        return cursor

    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs(self, mock_execute):
        show_cursor = self._build_show_containers_cursor(
            [{"instance_id": 0, "container_name": "builder"}]
        )
        logs_cursor = self._build_logs_cursor(("step 1\nstep 2\nstep 3",))
        mock_execute.side_effect = [show_cursor, logs_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        logs = SnowflakeAppManager().get_build_job_logs(fqn)
        assert logs == ["step 1", "step 2", "step 3"]

        assert mock_execute.call_count == 2
        show_call = mock_execute.call_args_list[0]
        assert (
            show_call.args[0]
            == "SHOW SERVICE CONTAINERS IN SERVICE DB.SCHEMA.BUILD_JOB"
        )
        assert show_call.kwargs["cursor_class"] is DictCursor
        assert mock_execute.call_args_list[1].args[0] == (
            "CALL SYSTEM$GET_SERVICE_LOGS('DB.SCHEMA.BUILD_JOB', '0', 'builder', 500)"
        )

    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_custom_last(self, mock_execute):
        show_cursor = self._build_show_containers_cursor(
            [{"instance_id": 0, "container_name": "builder"}]
        )
        logs_cursor = self._build_logs_cursor(("step 1",))
        mock_execute.side_effect = [show_cursor, logs_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        logs = SnowflakeAppManager().get_build_job_logs(fqn, last=100)
        assert logs == ["step 1"]
        assert mock_execute.call_args_list[1].args[0] == (
            "CALL SYSTEM$GET_SERVICE_LOGS('DB.SCHEMA.BUILD_JOB', '0', 'builder', 100)"
        )

    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_empty_result(self, mock_execute):
        show_cursor = self._build_show_containers_cursor(
            [{"instance_id": 0, "container_name": "builder"}]
        )
        logs_cursor = self._build_logs_cursor(None)
        mock_execute.side_effect = [show_cursor, logs_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        logs = SnowflakeAppManager().get_build_job_logs(fqn)
        assert logs == []

    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_blank_lines_skipped(self, mock_execute):
        show_cursor = self._build_show_containers_cursor(
            [{"instance_id": 0, "container_name": "builder"}]
        )
        logs_cursor = self._build_logs_cursor(("step 1\n\nstep 2\n",))
        mock_execute.side_effect = [show_cursor, logs_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        logs = SnowflakeAppManager().get_build_job_logs(fqn)
        assert logs == ["step 1", "step 2"]

    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_no_running_containers(self, mock_execute):
        # SUSPENDED/PENDING service reports no usable containers.
        show_cursor = self._build_show_containers_cursor([])
        mock_execute.side_effect = [show_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        logs = SnowflakeAppManager().get_build_job_logs(fqn)
        assert logs == []
        # No SYSTEM$GET_SERVICE_LOGS call when there is no container.
        mock_execute.assert_called_once()

    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_skips_null_container_rows(self, mock_execute):
        show_cursor = self._build_show_containers_cursor(
            [{"instance_id": None, "container_name": None}]
        )
        mock_execute.side_effect = [show_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        logs = SnowflakeAppManager().get_build_job_logs(fqn)
        assert logs == []
        mock_execute.assert_called_once()

    @patch("snowflake.cli._plugins.apps.manager.cli_console")
    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_multiple_containers_prefers_builder(
        self, mock_execute, mock_console
    ):
        show_cursor = self._build_show_containers_cursor(
            [
                {"instance_id": 0, "container_name": "sidecar"},
                {"instance_id": 0, "container_name": "builder"},
            ]
        )
        logs_cursor = self._build_logs_cursor(("ok",))
        mock_execute.side_effect = [show_cursor, logs_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        logs = SnowflakeAppManager().get_build_job_logs(fqn)
        assert logs == ["ok"]
        mock_console.warning.assert_called_once()
        assert mock_execute.call_args_list[1].args[0] == (
            "CALL SYSTEM$GET_SERVICE_LOGS('DB.SCHEMA.BUILD_JOB', '0', 'builder', 500)"
        )

    @patch("snowflake.cli._plugins.apps.manager.cli_console")
    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_multiple_containers_falls_back_to_first(
        self, mock_execute, mock_console
    ):
        show_cursor = self._build_show_containers_cursor(
            [
                {"instance_id": 0, "container_name": "foo"},
                {"instance_id": 1, "container_name": "bar"},
            ]
        )
        logs_cursor = self._build_logs_cursor(("ok",))
        mock_execute.side_effect = [show_cursor, logs_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        logs = SnowflakeAppManager().get_build_job_logs(fqn)
        assert logs == ["ok"]
        mock_console.warning.assert_called_once()
        assert mock_execute.call_args_list[1].args[0] == (
            "CALL SYSTEM$GET_SERVICE_LOGS('DB.SCHEMA.BUILD_JOB', '0', 'foo', 500)"
        )

    @patch("snowflake.cli._plugins.apps.manager.log")
    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_logs_show_result(self, mock_execute, mock_log):
        show_cursor = self._build_show_containers_cursor(
            [{"instance_id": 0, "container_name": "builder", "status": "READY"}]
        )
        logs_cursor = self._build_logs_cursor(("ok",))
        mock_execute.side_effect = [show_cursor, logs_cursor]

        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        SnowflakeAppManager().get_build_job_logs(fqn)

        info_messages = [call.args[0] for call in mock_log.info.call_args_list]
        assert any(
            "SHOW SERVICE CONTAINERS IN SERVICE" in message for message in info_messages
        )
        # The container row itself is emitted at INFO for verbose visibility.
        logged = " ".join(
            str(arg) for call in mock_log.info.call_args_list for arg in call.args
        )
        assert "builder" in logged

    @patch(EXECUTE_QUERY)
    def test_get_build_job_logs_caches_container_resolution(self, mock_execute):
        show_cursor = self._build_show_containers_cursor(
            [{"instance_id": 0, "container_name": "builder"}]
        )
        mock_execute.side_effect = [
            show_cursor,
            self._build_logs_cursor(("a",)),
            self._build_logs_cursor(("a\nb",)),
        ]

        manager = SnowflakeAppManager()
        fqn = FQN(database="DB", schema="SCHEMA", name="BUILD_JOB")
        assert manager.get_build_job_logs(fqn) == ["a"]
        assert manager.get_build_job_logs(fqn) == ["a", "b"]

        # SHOW SERVICE CONTAINERS runs once; only the log fetch repeats.
        show_calls = [
            call
            for call in mock_execute.call_args_list
            if call.args[0].startswith("SHOW SERVICE CONTAINERS")
        ]
        assert len(show_calls) == 1
        assert mock_execute.call_count == 3

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = {
            "url": "https://my-endpoint.snowflakecomputing.app",
            "is_upgrading": "false",
        }
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url == "https://my-endpoint.snowflakecomputing.app"
        mock_execute.assert_called_once()
        assert (
            mock_execute.call_args[0][0] == "DESCRIBE APPLICATION SERVICE DB.SCHEMA.SVC"
        )

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url_adds_https_prefix(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = {
            "url": "my-endpoint.snowflakecomputing.app",
            "is_upgrading": "false",
        }
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url == "https://my-endpoint.snowflakecomputing.app"

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url_not_found(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = None
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url is None
        mock_execute.assert_called_once()
        assert (
            "DESCRIBE APPLICATION SERVICE DB.SCHEMA.SVC" in mock_execute.call_args[0][0]
        )

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url_provisioning_in_progress(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = {
            "url": "Provisioning in progress... check back later",
            "is_upgrading": "false",
        }
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url is None

    @patch(EXECUTE_QUERY)
    def test_get_service_endpoint_url_while_upgrading(self, mock_execute):
        cursor = Mock()
        cursor.fetchone.return_value = {
            "url": "my-app.snowflakecomputing.app",
            "is_upgrading": "true",
        }
        mock_execute.return_value = cursor

        fqn = FQN(database="DB", schema="SCHEMA", name="SVC")
        url = SnowflakeAppManager().get_service_endpoint_url(fqn)
        assert url is None


# ── fetch_snow_apps_parameters tests ──────────────────────────────────


class TestFetchSnowAppsParameters:
    @patch(EXECUTE_QUERY)
    def test_returns_mapped_parameters(self, mock_execute):
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE",
                        "value": "MY_WH",
                        "level": "ACCOUNT",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL",
                        "value": "MY_POOL",
                        "level": "ACCOUNT",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_SERVICE_COMPUTE_POOL",
                        "value": "SVC_POOL",
                        "level": "ACCOUNT",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION",
                        "value": "MY_EAI",
                        "level": "ACCOUNT",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE",
                        "value": "MY_DB",
                        "level": "ACCOUNT",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA",
                        "value": "MY_SCHEMA",
                        "level": "ACCOUNT",
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
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE",
                        "value": "MY_WH",
                        "level": "ACCOUNT",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL",
                        "value": "",
                        "level": "ACCOUNT",
                    },
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
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_UNKNOWN_PARAM",
                        "value": "FOO",
                        "level": "ACCOUNT",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE",
                        "value": "MY_WH",
                        "level": "ACCOUNT",
                    },
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
                [
                    {
                        "KEY": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE",
                        "VALUE": "MY_WH",
                        "LEVEL": "ACCOUNT",
                    }
                ]
            )
        )
        mock_execute.return_value = cursor
        result = SnowflakeAppManager().fetch_snow_apps_parameters()
        assert result == {"query_warehouse": "MY_WH"}

    @patch(EXECUTE_QUERY)
    def test_ignores_system_default_level_parameters(self, mock_execute):
        """Parameters with an empty level are system defaults, not explicitly
        configured values, and must be ignored even when value is non-empty."""
        cursor = Mock()
        cursor.__iter__ = Mock(
            return_value=iter(
                [
                    # level="" means Snowflake is reporting the built-in default
                    # (e.g. after ALTER ACCOUNT UNSET). Should be skipped.
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL",
                        "value": "SYSTEM_COMPUTE_POOL_CPU",
                        "level": "",
                    },
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_SERVICE_COMPUTE_POOL",
                        "value": "SYSTEM_COMPUTE_POOL_CPU",
                        "level": "",
                    },
                    # Explicitly set at account level — should be included.
                    {
                        "key": "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE",
                        "value": "MY_WH",
                        "level": "ACCOUNT",
                    },
                ]
            )
        )
        mock_execute.return_value = cursor
        result = SnowflakeAppManager().fetch_snow_apps_parameters()
        assert result == {"query_warehouse": "MY_WH"}
        assert "build_compute_pool" not in result
        assert "service_compute_pool" not in result


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
    @patch(GET_MISSING_PRIVILEGES, return_value=[])
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    def test_parameters_fill_gaps(self, mock_role, mock_missing, mock_ctx, mock_params):
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
    @patch(GET_MISSING_PRIVILEGES, return_value=[])
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    def test_params_beat_session(self, mock_role, mock_missing, mock_ctx, mock_params):
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

    @patch(MANAGER_CLI_CONSOLE)
    @patch(
        FETCH_SNOW_APPS_PARAMS,
        return_value={"database": "PARAM_DB", "schema": "PARAM_SCHEMA"},
    )
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    @patch(GET_PERSONAL_DATABASE, return_value="USER$MYUSER")
    @patch(
        GET_MISSING_PRIVILEGES,
        return_value=[
            {
                "privilege": "CREATE STAGE",
                "objectType": "SCHEMA",
                "objectName": "PARAM_DB.PARAM_SCHEMA",
            }
        ],
    )
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    def test_missing_privileges_fall_back_to_personal_db(
        self,
        mock_role,
        mock_missing,
        mock_personal,
        mock_ctx,
        mock_params,
        mock_console,
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database=None, schema=None)
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        assert result["database"] == "USER$MYUSER"
        assert result["schema"] == "PUBLIC"
        mock_console.warning.assert_called_once()
        warning = mock_console.warning.call_args[0][0]
        assert "ENGINEER" in warning
        assert "Snowflake App Runtime" in warning
        assert "PARAM_DB.PARAM_SCHEMA" in warning
        assert "account-admin-setup" in warning

    @patch(MANAGER_CLI_CONSOLE)
    @patch(
        FETCH_SNOW_APPS_PARAMS,
        return_value={"database": "PARAM_DB", "schema": "PARAM_SCHEMA"},
    )
    @patch(GET_CLI_CONTEXT, return_value=_mock_connection_context())
    @patch(GET_PERSONAL_DATABASE, return_value="USER$MYUSER")
    @patch(GET_MISSING_PRIVILEGES, side_effect=ProgrammingError("cannot resolve"))
    @patch(CURRENT_ROLE, return_value="ENGINEER")
    def test_unresolvable_destination_falls_back_to_personal_db(
        self,
        mock_role,
        mock_missing,
        mock_personal,
        mock_ctx,
        mock_params,
        mock_console,
    ):
        from snowflake.cli._plugins.apps.manager import _resolve_deploy_defaults

        entity = self._make_entity(database=None, schema=None)
        result = _resolve_deploy_defaults(entity, SnowflakeAppManager())
        # Every probe statement errored, so the destination cannot be resolved
        # by the current role and we fall back to the personal database.
        assert result["database"] == "USER$MYUSER"
        assert result["schema"] == "PUBLIC"
        mock_console.warning.assert_called_once()
        assert "PARAM_DB" in mock_console.warning.call_args[0][0]


class TestFlattenMissingPrivileges:
    def test_authorized_returns_empty(self):
        from snowflake.cli._plugins.apps.manager import _flatten_missing_privileges

        assert _flatten_missing_privileges({"authorized": True}) == []

    def test_single_permission_node(self):
        from snowflake.cli._plugins.apps.manager import _flatten_missing_privileges

        node = {"privilege": "USAGE", "objectType": "DATABASE", "objectName": "DB"}
        assert _flatten_missing_privileges(node) == [node]

    def test_nested_all_of_and_one_of(self):
        from snowflake.cli._plugins.apps.manager import _flatten_missing_privileges

        tree = {
            "allOf": [
                {"privilege": "USAGE", "objectType": "DATABASE", "objectName": "DB"},
                {
                    "oneOf": [
                        {
                            "privilege": "CREATE STAGE",
                            "objectType": "SCHEMA",
                            "objectName": "DB.SCH",
                        }
                    ]
                },
            ]
        }
        result = _flatten_missing_privileges(tree)
        assert {n["privilege"] for n in result} == {"USAGE", "CREATE STAGE"}

    def test_non_dict_returns_empty(self):
        from snowflake.cli._plugins.apps.manager import _flatten_missing_privileges

        assert _flatten_missing_privileges(None) == []
        assert _flatten_missing_privileges("nope") == []


class TestDeployPrivilegeCheckStatements:
    def test_statements_reference_destination(self):
        from snowflake.cli._plugins.apps.manager import (
            PRIVILEGE_CHECK_OBJECT_NAME,
            _deploy_privilege_check_statements,
        )

        statements = _deploy_privilege_check_statements("APPS", "PUBLIC")
        # Exactly two statements are probed: CREATE STAGE and CREATE ARTIFACT
        # REPOSITORY, both referencing only the destination database/schema.
        assert len(statements) == 2
        assert any(s.startswith("CREATE STAGE APPS.PUBLIC.") for s in statements)
        assert any("CREATE ARTIFACT REPOSITORY APPS.PUBLIC." in s for s in statements)
        assert all(PRIVILEGE_CHECK_OBJECT_NAME in s for s in statements)
        # Statements that reference not-yet-existing objects, only need USAGE, or
        # belong to the workspace flow are intentionally excluded.
        joined = " ".join(statements)
        assert "CREATE APPLICATION SERVICE" not in joined
        assert "CREATE WORKSPACE" not in joined
        assert "SHOW" not in joined

    def test_personal_database_is_quoted(self):
        from snowflake.cli._plugins.apps.manager import (
            _deploy_privilege_check_statements,
        )

        statements = _deploy_privilege_check_statements(
            "USER$first.last@snowflake.com", "PUBLIC"
        )
        # Personal database names contain characters illegal in unquoted
        # identifiers and must be quoted so EXPLAIN_PRIVILEGES can parse them.
        assert all('"USER$first.last@snowflake.com".PUBLIC.' in s for s in statements)


class TestFilterAccessibleRemoteDefaults:
    """Direct tests for the account-default privilege check that protects deploy
    and setup from targeting a destination the current role cannot use."""

    def _manager(self, *, role="ENGINEER", missing=None, side_effect=None):
        manager = Mock()
        manager.current_role.return_value = role
        if side_effect is not None:
            manager.get_missing_privileges.side_effect = side_effect
        else:
            manager.get_missing_privileges.return_value = missing or []
        return manager

    def test_no_database_returns_params_unchanged(self):
        from snowflake.cli._plugins.apps.manager import (
            _filter_accessible_remote_defaults,
        )

        params = {"query_warehouse": "WH"}
        manager = self._manager()
        assert _filter_accessible_remote_defaults(manager, params) == params
        manager.get_missing_privileges.assert_not_called()

    @patch(MANAGER_CLI_CONSOLE)
    def test_no_missing_privileges_returns_params_unchanged(self, mock_console):
        from snowflake.cli._plugins.apps.manager import (
            _filter_accessible_remote_defaults,
        )

        params = {"database": "DB", "schema": "SCH", "query_warehouse": "WH"}
        manager = self._manager(missing=[])
        result = _filter_accessible_remote_defaults(manager, params)
        assert result == params
        mock_console.warning.assert_not_called()
        # A step message announces the privilege-check phase to the user.
        mock_console.step.assert_called_once()
        assert "Checking deploy privileges" in mock_console.step.call_args[0][0]
        assert "DB.SCH" in mock_console.step.call_args[0][0]
        # Every representative deploy statement is probed for the active role.
        assert manager.get_missing_privileges.call_count == 2
        for call in manager.get_missing_privileges.call_args_list:
            assert call.args[1] == "ENGINEER"

    @patch(MANAGER_CLI_CONSOLE)
    def test_verbose_logs_per_statement_and_summary(self, mock_console, caplog):
        import logging

        from snowflake.cli._plugins.apps.manager import (
            _filter_accessible_remote_defaults,
        )

        manager = self._manager(
            missing=[
                {
                    "privilege": "CREATE STAGE",
                    "objectType": "SCHEMA",
                    "objectName": "DB.SCH",
                }
            ]
        )
        params = {"database": "DB", "schema": "SCH"}
        with caplog.at_level(
            logging.INFO, logger="snowflake.cli._plugins.apps.manager"
        ):
            _filter_accessible_remote_defaults(manager, params)
        messages = "\n".join(r.getMessage() for r in caplog.records)
        # Per-statement results and a final summary are emitted at INFO so they
        # surface under --verbose.
        assert "Privilege check: missing" in messages
        assert "Privilege check failed" in messages
        assert "CREATE STAGE on SCHEMA DB.SCH" in messages

    @patch(MANAGER_CLI_CONSOLE)
    def test_missing_privileges_drops_destination(self, mock_console):
        from snowflake.cli._plugins.apps.manager import (
            _filter_accessible_remote_defaults,
        )

        manager = self._manager(
            missing=[
                {
                    "privilege": "CREATE ARTIFACT REPOSITORY",
                    "objectType": "SCHEMA",
                    "objectName": "DB.SCH",
                }
            ]
        )
        params = {"database": "DB", "schema": "SCH", "query_warehouse": "WH"}
        result = _filter_accessible_remote_defaults(manager, params)
        assert result == {"query_warehouse": "WH"}
        mock_console.warning.assert_called_once()
        warning = mock_console.warning.call_args[0][0]
        # The warning names the destination and feature, not the specific grants
        # (those are only in the verbose INFO logs).
        assert "Snowflake App Runtime" in warning
        assert "'DB.SCH'" in warning
        assert "CREATE ARTIFACT REPOSITORY" not in warning

    @patch(MANAGER_CLI_CONSOLE)
    def test_all_probes_error_drops_destination(self, mock_console):
        from snowflake.cli._plugins.apps.manager import (
            _filter_accessible_remote_defaults,
        )

        manager = self._manager(side_effect=ProgrammingError("cannot resolve"))
        params = {"database": "DB", "schema": "SCH"}
        result = _filter_accessible_remote_defaults(manager, params)
        assert result == {}
        mock_console.warning.assert_called_once()
        assert "'DB.SCH'" in mock_console.warning.call_args[0][0]

    @patch(MANAGER_CLI_CONSOLE)
    def test_any_probe_error_drops_destination(self, mock_console):
        from snowflake.cli._plugins.apps.manager import (
            _filter_accessible_remote_defaults,
        )

        # A probe error means the role cannot analyze/resolve the destination,
        # so the check fails even if another statement reports no missing grants.
        manager = Mock()
        manager.current_role.return_value = "ENGINEER"
        manager.get_missing_privileges.side_effect = [
            ProgrammingError("requires access on all objects"),
            [],
        ]
        params = {"database": "DB", "schema": "SCH"}
        result = _filter_accessible_remote_defaults(manager, params)
        assert result == {}
        mock_console.warning.assert_called_once()
        assert "'DB.SCH'" in mock_console.warning.call_args[0][0]

    @patch(MANAGER_CLI_CONSOLE)
    def test_missing_schema_defaults_to_public(self, mock_console):
        from snowflake.cli._plugins.apps.manager import (
            _deploy_privilege_check_statements,
            _filter_accessible_remote_defaults,
        )

        manager = self._manager(missing=[])
        params = {"database": "DB"}
        _filter_accessible_remote_defaults(manager, params)
        probed = manager.get_missing_privileges.call_args_list[0].args[0]
        assert "DB.PUBLIC." in probed
        # Sanity check the statement set is the deploy set.
        assert probed in _deploy_privilege_check_statements("DB", "PUBLIC")

    @patch(MANAGER_CLI_CONSOLE)
    def test_does_not_mutate_input_params(self, mock_console):
        from snowflake.cli._plugins.apps.manager import (
            _filter_accessible_remote_defaults,
        )

        params = {"database": "DB", "schema": "SCH"}
        manager = self._manager(side_effect=ProgrammingError("cannot resolve"))
        _filter_accessible_remote_defaults(manager, params)
        assert params == {"database": "DB", "schema": "SCH"}


# ── CLI command tests ─────────────────────────────────────────────────


class TestSetupCommand:
    @pytest.fixture(autouse=True)
    def _assume_destination_accessible(self):
        """Resolution/precedence tests assume the active role can access the
        account-configured destination. The privilege probe itself is covered by
        ``TestFilterAccessibleRemoteDefaults`` and ``TestSetupPrivilegeFallback``,
        so patch it to a pass-through here to keep these tests focused.
        """
        with patch(
            "snowflake.cli._plugins.apps.commands._filter_accessible_remote_defaults",
            side_effect=lambda manager, params: params,
        ):
            yield

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
            assert "Initialized Snowflake App Runtime project" in result.output
            assert (tmp_path / "snowflake.yml").exists()

        resolved = mock_gen.call_args[0][1]
        assert resolved["database"] == "PARAM_DB"
        assert resolved["warehouse"] == "PARAM_WH"
        assert resolved["build_compute_pool"] == "PARAM_POOL"
        assert resolved["build_eai"] == "PARAM_EAI"
        assert mock_gen.call_args.kwargs["use_workspace"] is False

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_init_uses_current_directory_name_when_app_name_not_provided(
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

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup"])
            assert result.exit_code == 0, result.output

        assert mock_gen.call_args[0][0] == tmp_path.name

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_init_normalizes_derived_directory_name(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
        }
        project_dir = tmp_path / "my app-name!@#"
        project_dir.mkdir()

        with change_directory(project_dir):
            result = runner.invoke(["app", "setup"])

        assert result.exit_code == 0, result.output
        assert mock_gen.call_args[0][0] == "my_app_name"

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_init_rejects_empty_normalized_directory_name(
        self, mock_mgr_cls, runner, tmp_path
    ):
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
        }
        invalid_project = tmp_path / "!@#"
        invalid_project.mkdir()

        with change_directory(invalid_project):
            result = runner.invoke(["app", "setup"])

        assert result.exit_code == 1
        assert "Could not derive app name from the current directory." in result.output

    def test_init_skips_when_file_exists(self, runner, tmp_path):
        (tmp_path / "snowflake.yml").write_text("existing content")
        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output
            assert "already exists" in result.output

    def test_init_rejects_invalid_explicit_app_name(self, runner, tmp_path):
        """``--app-name`` with characters outside ``[a-zA-Z0-9_]`` is rejected.

        Validation happens at the top of ``snowflake_app_setup`` (the
        ``re.fullmatch`` guard), strictly before ``fetch_snow_apps_parameters``,
        so no manager mock is required.
        """
        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "bad-name!"])

        assert result.exit_code == 1
        assert "Invalid app name 'bad-name!'" in result.output

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
        """CLI flags should override Snowflake App Runtime parameters."""
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
    def test_warehouse_flag_beats_account_param(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """--warehouse CLI flag should override the account parameter and show 'user input' provenance."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
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
                    "--warehouse",
                    "MY_WAREHOUSE",
                ]
            )
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["warehouse"] == "MY_WAREHOUSE"
        assert "warehouse: MY_WAREHOUSE  (user input)" in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_database_flag_beats_account_param(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """--database CLI flag should override the account parameter and show 'user input' provenance."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
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
                    "--database",
                    "MY_DATABASE",
                    "--schema",
                    "MY_SCHEMA",
                ]
            )
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["database"] == "MY_DATABASE"
        assert "database: MY_DATABASE  (user input)" in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_database_without_schema_is_rejected(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """Specifying --database without --schema should fail with a clear error."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
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
                    "--database",
                    "MY_DATABASE",
                ]
            )
            assert result.exit_code != 0
            assert "--schema is required when --database is specified" in result.output
        # The validation must fail before any snowflake.yml is generated.
        mock_gen.assert_not_called()

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_schema_without_database_is_allowed(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """Specifying --schema without --database is allowed; the database is
        resolved from account parameters or the connection as usual."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
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
                    "--schema",
                    "MY_SCHEMA",
                ]
            )
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["schema"] == "MY_SCHEMA"
        assert resolved["database"] == "PARAM_DB"

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_schema_flag_beats_account_param(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """--schema CLI flag should override the account parameter and show 'user input' provenance."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
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
                    "--schema",
                    "MY_SCHEMA",
                ]
            )
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["schema"] == "MY_SCHEMA"
        assert "schema: MY_SCHEMA  (user input)" in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_all_three_flags_beat_account_params(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """--warehouse, --database, and --schema flags should all override account parameters."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
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
                    "--warehouse",
                    "MY_WH",
                    "--database",
                    "MY_DB",
                    "--schema",
                    "MY_SCHEMA",
                ]
            )
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["warehouse"] == "MY_WH"
        assert resolved["database"] == "MY_DB"
        assert resolved["schema"] == "MY_SCHEMA"

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_warehouse_flag_satisfies_missing_warehouse_requirement(
        self, mock_mgr_cls, runner, tmp_path
    ):
        """--warehouse should prevent the 'Missing warehouse' error even when
        no account parameter or connection default is configured."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
        }

        from tests_common import change_directory

        with change_directory(tmp_path):
            result = runner.invoke(
                [
                    "app",
                    "setup",
                    "--app-name",
                    "my_app",
                    "--warehouse",
                    "EXPLICIT_WH",
                ]
            )
            assert result.exit_code == 0, result.output
            assert "Missing warehouse" not in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_database_flag_satisfies_missing_database_requirement(
        self, mock_mgr_cls, runner, tmp_path
    ):
        """--database should prevent the 'Missing database' error even when
        no account parameter, personal DB, or connection default is configured."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.get_personal_database.return_value = None
        mock_mgr.fetch_snow_apps_parameters.return_value = {
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
                    "--database",
                    "EXPLICIT_DB",
                    "--schema",
                    "EXPLICIT_SCHEMA",
                ]
            )
            assert result.exit_code == 0, result.output
            assert "Missing database" not in result.output

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_setup_shows_parameter_provenance(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """Resolved values from Snowflake App Runtime parameters should show 'account parameter' provenance."""
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

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["database"] == "USER$MYUSER"
        assert resolved["schema"] == "PUBLIC"
        assert mock_gen.call_args.kwargs["use_workspace"] is True

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_connection_dict")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_setup_uses_stage_when_database_resolved_from_session(
        self, mock_mgr_cls, mock_get_conn, mock_gen, runner, tmp_path
    ):
        """Session/connection database (not personal DB) should emit code_stage."""
        mock_get_conn.return_value = {"database": "CONN_DB"}
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
            "build_eai": "PARAM_EAI",
        }
        mock_mgr.get_personal_database.return_value = None

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output

        assert mock_gen.call_args.kwargs["use_workspace"] is False

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_connection_dict")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_setup_uses_workspace_when_session_database_is_personal(
        self, mock_mgr_cls, mock_get_conn, mock_gen, runner, tmp_path
    ):
        """A personal database resolved from the session (not the personal-DB
        default tier, e.g. because ``get_personal_database`` returned ``None``)
        must still emit ``code_workspace`` — personal databases never support
        stages."""
        mock_get_conn.return_value = {"database": "USER$SNOTEBAERT"}
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "schema": "PUBLIC",
            "query_warehouse": "PARAM_WH",
        }
        mock_mgr.get_personal_database.return_value = None

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["database"] == "USER$SNOTEBAERT"
        assert mock_gen.call_args.kwargs["use_workspace"] is True

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_compute_pools_resolved_from_account_params(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """``build_compute_pool`` and ``service_compute_pool`` are resolved
        from the ``DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL`` /
        ``DEFAULT_SNOWFLAKE_APPS_SERVICE_COMPUTE_POOL`` account parameters and
        forwarded to the generated snowflake.yml."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
        }

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["build_compute_pool"] == "PARAM_POOL"
        assert resolved["service_compute_pool"] == "PARAM_SVC_POOL"

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_compute_pool_flag_overrides_account_params(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """The (hidden) ``--compute-pool`` flag takes precedence over the
        account-parameter compute pools for both build and service pools."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
        }

        with change_directory(tmp_path):
            result = runner.invoke(
                [
                    "app",
                    "setup",
                    "--app-name",
                    "my_app",
                    "--compute-pool",
                    "FLAG_POOL",
                ]
            )
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["build_compute_pool"] == "FLAG_POOL"
        assert resolved["service_compute_pool"] == "FLAG_POOL"

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_compute_pools_omitted_when_no_source_provides_them(
        self, mock_mgr_cls, runner, tmp_path
    ):
        """When neither the flag nor account parameters provide compute pools,
        both fields are omitted from setup output so the server allocates the
        pools at deploy time."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
        }

        with change_directory(tmp_path):
            result = runner.invoke(
                ["app", "setup", "--app-name", "my_app", "--dry-run"]
            )
            assert result.exit_code == 0, result.output
            assert "build_compute_pool" not in result.output
            assert "service_compute_pool" not in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_compute_pools_dry_run_json_output(self, mock_mgr_cls, runner, tmp_path):
        """In JSON output mode, the account-parameter compute pools are
        reported under their resolution keys."""
        import json as json_mod

        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
            "build_compute_pool": "PARAM_POOL",
            "service_compute_pool": "PARAM_SVC_POOL",
        }

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
        assert parsed["build_compute_pool"] == "PARAM_POOL"
        assert parsed["service_compute_pool"] == "PARAM_SVC_POOL"


# ── perform_bundle tests ──────────────────────────────────────────────


class TestSetupPrivilegeFallback:
    """End-to-end ``snow app setup`` coverage that exercises the real privilege
    probe (no pass-through patch) and asserts the personal-database fallback."""

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_missing_privileges_fall_back_to_personal_db(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """When the current role is missing privileges on the account-configured
        destination, setup falls back to the personal database (as if no account
        default were set) and warns the user."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
        }
        mock_mgr.current_role.return_value = "ENGINEER"
        mock_mgr.get_missing_privileges.return_value = [
            {
                "privilege": "CREATE STAGE",
                "objectType": "SCHEMA",
                "objectName": "PARAM_DB.PARAM_SCHEMA",
            }
        ]
        mock_mgr.get_personal_database.return_value = "USER$MYUSER"

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["database"] == "USER$MYUSER"
        assert resolved["schema"] == "PUBLIC"
        assert mock_gen.call_args.kwargs["use_workspace"] is True

    @patch(
        "snowflake.cli._plugins.apps.commands._generate_snowflake_yml",
        return_value="definition_version: '2'\n",
    )
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    def test_accessible_destination_is_used(
        self, mock_mgr_cls, mock_gen, runner, tmp_path
    ):
        """When the role has the privileges (no missing), the account-configured
        destination is used as-is."""
        mock_mgr = mock_mgr_cls.return_value
        mock_mgr.fetch_snow_apps_parameters.return_value = {
            "database": "PARAM_DB",
            "schema": "PARAM_SCHEMA",
            "query_warehouse": "PARAM_WH",
        }
        mock_mgr.current_role.return_value = "ENGINEER"
        mock_mgr.get_missing_privileges.return_value = []

        with change_directory(tmp_path):
            result = runner.invoke(["app", "setup", "--app-name", "my_app"])
            assert result.exit_code == 0, result.output

        resolved = mock_gen.call_args[0][1]
        assert resolved["database"] == "PARAM_DB"
        assert resolved["schema"] == "PARAM_SCHEMA"


class TestPerformBundle:
    @patch("snowflake.cli._plugins.apps.manager.get_cli_context")
    @patch("snowflake.cli._plugins.apps.manager._bundle_app_artifacts")
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
    @patch("snowflake.cli._plugins.apps.manager._bundle_app_artifacts")
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
    @patch("snowflake.cli._plugins.apps.manager._bundle_app_artifacts")
    def test_returns_project_paths(self, mock_bundle, mock_ctx, tmp_path):
        mock_ctx().project_root = tmp_path

        entity = Mock()
        entity.artifacts = []

        result = perform_bundle("my_app", entity)

        assert result.project_root == tmp_path
        assert result.bundle_root == tmp_path / "output" / "bundle"

    @patch("snowflake.cli._plugins.apps.manager.get_cli_context")
    def test_excludes_bundle_root_recursion_for_snowflake_apps(
        self, mock_ctx, tmp_path
    ):
        mock_ctx().project_root = tmp_path

        (tmp_path / "app").mkdir(parents=True)
        (tmp_path / "app" / "main.py").write_text("print('ok')\n")
        (tmp_path / "output").mkdir(parents=True)
        (tmp_path / "output" / "keep.txt").write_text("keep\n")
        (tmp_path / "output" / "bundle").mkdir(parents=True)
        (tmp_path / "output" / "bundle" / "recursive.txt").write_text("recursive\n")

        entity = Mock()
        entity.artifacts = [PathMapping(src="*", dest="./")]

        result = perform_bundle("my_app", entity)

        assert (result.bundle_root / "app" / "main.py").exists()
        assert (result.bundle_root / "output" / "keep.txt").exists()
        assert not (result.bundle_root / "output" / "bundle" / "recursive.txt").exists()


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


# ── Non-ASCII definition encoding regression tests ────────────────────


class TestBundleNonAsciiDefinitionEncoding:
    """Regression tests for the Windows-only ``UnicodeDecodeError`` raised while
    bundling a project whose ``snowflake.yml`` contains non-ASCII characters.

    The project definition is read while resolving the snowflake-app flow (and
    again during bundling). Without an explicit ``encoding=`` the read falls
    back to the platform default, which on Windows is the ANSI code page
    (cp1252). Bytes undefined there — e.g. the UTF-8 encoding of U+0401
    (Cyrillic Yo, 0xD0 0x81) — raise ``UnicodeDecodeError``. macOS/Linux
    default to UTF-8 so the bug never reproduces there. ``SecurePath`` and the
    project-definition loader now read UTF-8 explicitly.
    """

    # U+0401 encodes to UTF-8 bytes 0xD0 0x81; 0x81 is undefined in cp1252.
    _YML_WITH_NON_ASCII = (
        "definition_version: '2'\n"
        "entities:\n"
        "  my_app:\n"
        "    type: snowflake-app\n"
        "    identifier: my_app\n"
        "    meta:\n"
        '      title: "Demo \u0401 app"\n'
        "    artifacts:\n"
        "      - src: app/*\n"
        "        dest: ./\n"
    )

    @staticmethod
    @contextmanager
    def _simulated_ansi_locale(simulated_encoding="cp1252"):
        """Force text-mode opens that omit ``encoding=`` to use a non-UTF-8 code
        page, mimicking the Windows ANSI default in-process on any host OS.

        ``pathlib.Path.open`` forwards the ``"locale"`` sentinel (or ``None``)
        when the caller does not pass ``encoding=``; both are substituted so the
        regression reproduces deterministically. Reads that pass an explicit
        ``encoding`` (e.g. the post-fix UTF-8 reads) are left untouched.
        """
        real_open = io.open

        def fake_open(
            file,
            mode="r",
            buffering=-1,
            encoding=None,
            errors=None,
            newline=None,
            closefd=True,
            opener=None,
        ):
            if "b" not in mode and encoding in (None, "locale"):
                encoding = simulated_encoding
            return real_open(
                file, mode, buffering, encoding, errors, newline, closefd, opener
            )

        with patch("io.open", fake_open):
            yield

    def _write_project(self, tmp_path):
        app_dir = tmp_path / "app"
        app_dir.mkdir()
        (app_dir / "main.py").write_text("print('hello')\n", encoding="utf-8")
        (tmp_path / "snowflake.yml").write_text(
            self._YML_WITH_NON_ASCII, encoding="utf-8"
        )

    def test_simulation_reproduces_decode_error_for_unguarded_reads(self, tmp_path):
        """Sanity check: the ANSI-locale simulation does raise for an unguarded
        (no ``encoding=``) read, so the regression tests below are meaningful."""
        self._write_project(tmp_path)
        with self._simulated_ansi_locale():
            with pytest.raises(UnicodeDecodeError):
                (tmp_path / "snowflake.yml").read_text()
            # An explicit UTF-8 read still succeeds under the same simulation.
            assert "\u0401" in (tmp_path / "snowflake.yml").read_text(encoding="utf-8")

    def test_bundle_succeeds_with_non_ascii_definition(self, runner, tmp_path):
        self._write_project(tmp_path)
        with change_directory(tmp_path):
            with self._simulated_ansi_locale():
                result = runner.invoke(["app", "bundle"])
        assert result.exit_code == 0, result.output
        assert "Bundle generated at" in result.output

    def test_validate_succeeds_with_non_ascii_definition(self, runner, tmp_path):
        self._write_project(tmp_path)
        with change_directory(tmp_path):
            with self._simulated_ansi_locale():
                result = runner.invoke(["app", "validate"])
        assert result.exit_code == 0, result.output
        assert "Valid Snowflake App Runtime project" in result.output


# ── Validate CLI command tests ────────────────────────────────────────


class TestValidateCommand:
    @staticmethod
    def _make_validate_entity():
        entity = Mock()
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
            assert "Valid Snowflake App Runtime project" in result.output

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
    def test_validate_succeeds_without_dockerfile(
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
            assert result.exit_code == 0, result.output
            assert "Valid Snowflake App Runtime project" in result.output

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_validate_cleans_up_bundle_after_validation(
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
            assert result.exit_code == 0, result.output
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

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_permission_error_includes_role_guidance(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
        runner,
        tmp_path,
    ):
        entity = Mock()
        fqn = Mock(database="DB", schema="SCHEMA")
        fqn.name = "MY_APP"
        entity.fqn = fqn
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.get_service_endpoint_url.side_effect = ProgrammingError(
            "not authorized"
        )

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "open"])
            assert result.exit_code == 1
            assert (
                "Could not resolve endpoint URL for service DB.SCHEMA.MY_APP"
                in result.output
            )
            span = _get_completed_span("snowflake_app.open.resolve_endpoint")
            assert span[CLIMetricsSpan.ERROR_KEY] == ProgrammingError.__name__

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
        return_value="https://app.snowflake.com/org/acct/#/apps/app-service/DB.SCHEMA.MY_APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_launches_snowsight(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
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
        mock_manager_cls.return_value.is_application_service.return_value = True

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings"])
            assert result.exit_code == 0, result.output
            assert "#/apps/app-service/DB.SCHEMA.MY_APP/details" in result.output
            mock_launch.assert_called_once()
            mock_snowsight.assert_called_once()
            path_arg = mock_snowsight.call_args[0][1]
            assert path_arg == "#/apps/app-service/DB.SCHEMA.MY_APP/details"

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch(
        "snowflake.cli._plugins.apps.commands.make_snowsight_url",
        return_value="https://app.snowflake.com/org/acct/#/apps/app-service/DB.SCHEMA.MY_APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_print_only(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
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
        mock_manager_cls.return_value.is_application_service.return_value = True

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings", "--print-only"])
            assert result.exit_code == 0, result.output
            assert "#/apps/app-service/DB.SCHEMA.MY_APP/details" in result.output
            mock_launch.assert_not_called()

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch(
        "snowflake.cli._plugins.apps.commands.make_snowsight_url",
        return_value="https://app.snowflake.com/org/acct/#/apps/app-service/CONN_DB.CONN_SCHEMA.MY_APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_falls_back_to_connection_context(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
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
        mock_manager_cls.return_value.is_application_service.return_value = True

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings"])
            assert result.exit_code == 0, result.output
            path_arg = mock_snowsight.call_args[0][1]
            assert path_arg == "#/apps/app-service/CONN_DB.CONN_SCHEMA.MY_APP/details"

    @patch("snowflake.cli._plugins.apps.commands.typer.launch")
    @patch(
        "snowflake.cli._plugins.apps.commands.make_snowsight_url",
        return_value="https://app.snowflake.com/org/acct/#/apps/service/DB.SCHEMA.MY_APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_uses_service_segment_for_legacy_services(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
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
        mock_manager_cls.return_value.is_application_service.return_value = False

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings"])
            assert result.exit_code == 0, result.output
            path_arg = mock_snowsight.call_args[0][1]
            assert path_arg == "#/apps/service/DB.SCHEMA.MY_APP/details"

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
        return_value="https://app.snowflake.com/org/acct/#/apps/app-service/MY%20DB.MY%20SCHEMA.MY%20APP/details",
    )
    @patch("snowflake.cli._plugins.apps.commands.get_cli_context")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_open_settings_url_encodes_identifiers(
        self,
        mock_resolve,
        mock_get_entity,
        mock_manager_cls,
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
        mock_manager_cls.return_value.is_application_service.return_value = True

        from tests_common import change_directory

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "open", "--settings"])
            assert result.exit_code == 0, result.output
            path_arg = mock_snowsight.call_args[0][1]
            assert "#/apps/app-service/" in path_arg
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
            _reset_command_metrics()
            result = runner.invoke(["app", "events"])
            assert result.exit_code == 1
            assert "Could not retrieve logs" in result.output
            assert "Verify that the app is deployed" in result.output
            span = _get_completed_span("snowflake_app.events.fetch_logs")
            assert span[CLIMetricsSpan.ERROR_KEY] == ProgrammingError.__name__


class TestResolveCodeStorage:
    """Unit coverage for the workspace-vs-stage backend selection.

    A personal database (``USER$<user>``) never supports stages, so the
    resolver must route every personal-database destination to a workspace —
    even when ``snowflake.yml`` explicitly configures a stage or omits code
    storage entirely.
    """

    @staticmethod
    def _entity(*, code_stage=None, code_workspace=None):
        entity = Mock()
        entity.code_stage = code_stage
        entity.code_workspace = code_workspace
        return entity

    def test_explicit_workspace_is_honored(self):
        ws = Mock(database="WS_DB", schema_="WS_SCHEMA")
        ws.name = "MY_WS"
        storage = _resolve_code_storage(
            self._entity(code_workspace=ws),
            database="TEST_DB",
            schema="TEST_SCHEMA",
            app_name="MY_APP",
        )
        assert storage == _CodeStorage(
            type="workspace",
            name="MY_WS",
            database_override="WS_DB",
            schema_override="WS_SCHEMA",
            encryption_type="SNOWFLAKE_SSE",
        )

    def test_explicit_stage_on_regular_db_is_honored(self):
        stage = Mock(database=None, schema_=None, encryption_type="SNOWFLAKE_SSE")
        stage.name = "MY_STAGE"
        storage = _resolve_code_storage(
            self._entity(code_stage=stage),
            database="TEST_DB",
            schema="TEST_SCHEMA",
            app_name="MY_APP",
        )
        assert storage.type == "stage"
        assert storage.name == "MY_STAGE"

    def test_explicit_stage_on_personal_db_is_honored_with_warning(self):
        """An explicit ``code_stage`` aimed at a personal database is honored
        (the user's choice wins); a warning is emitted because stages are
        generally unsupported in personal databases."""
        stage = Mock(database=None, schema_=None, encryption_type="SNOWFLAKE_SSE")
        stage.name = "MY_APP_CODE"
        with patch("snowflake.cli._plugins.apps.commands.cli_console") as mock_console:
            storage = _resolve_code_storage(
                self._entity(code_stage=stage),
                database="USER$SNOTEBAERT",
                schema="PUBLIC",
                app_name="MY_APP",
            )
        assert storage == _CodeStorage(
            type="stage",
            name="MY_APP_CODE",
            database_override=None,
            schema_override=None,
            encryption_type="SNOWFLAKE_SSE",
        )
        mock_console.warning.assert_called_once()

    def test_no_code_storage_on_regular_db_defaults_to_stage(self):
        storage = _resolve_code_storage(
            self._entity(),
            database="TEST_DB",
            schema="TEST_SCHEMA",
            app_name="MY_APP",
        )
        assert storage.type == "stage"
        assert storage.name == "MY_APP_CODE"

    def test_no_code_storage_on_personal_db_defaults_to_workspace(self):
        storage = _resolve_code_storage(
            self._entity(),
            database="USER$SNOTEBAERT",
            schema="PUBLIC",
            app_name="MY_APP",
        )
        assert storage.type == "workspace"
        assert storage.name == "SNOWFLAKE_APPS"


# ── Deploy CLI command tests ──────────────────────────────────────────


RESOLVE_DEPLOY_DEFAULTS = (
    "snowflake.cli._plugins.apps.commands._resolve_deploy_defaults"
)

# Workspace builds use the committed ``last`` alias directly.
_WORKSPACE_BUILD_SOURCE_URI = (
    "snow://workspace/TEST_DB.TEST_SCHEMA.MY_APP_CODE/versions/last/MY_APP"
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
    def test_deploy_service_creation_error_includes_role_guidance(
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

        create_error = ProgrammingError("not authorized")
        create_error.errno = 2043

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.create_app_service.side_effect = create_error
        mock_mgr.get_service_logs.return_value = (
            "create failed line1\ncreate failed line2"
        )
        mock_poll.return_value = {
            "url": "my-app.snowflakecomputing.app",
            "is_upgrading": "false",
        }

        with (
            change_directory(tmp_path),
            patch("snowflake.cli._plugins.apps.commands.log") as mock_log,
        ):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "deploy", "--deploy-only"])
            assert result.exit_code == 1
            assert (
                "Deployment failed while creating application service" in result.output
            )
            assert "Verify privileges for CREATE" in result.output
            create_span = _get_completed_span("snowflake_app.deploy_service.create")
            assert create_span[CLIMetricsSpan.ERROR_KEY] == ProgrammingError.__name__
            mock_log.info.assert_any_call("create failed line1")
            mock_log.info.assert_any_call("create failed line2")

        mock_mgr.get_service_logs.assert_called_once()
        service_fqn = mock_mgr.get_service_logs.call_args.args[0]
        assert service_fqn.identifier == "TEST_DB.TEST_SCHEMA.MY_APP"

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
    def test_deploy_service_upgrade_error_records_upgrade_span(
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

        create_error = ProgrammingError("already exists")
        create_error.errno = 2002
        upgrade_error = ProgrammingError("permission denied")
        upgrade_error.errno = 2043

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.create_app_service.side_effect = create_error
        mock_mgr.upgrade_app_service.side_effect = upgrade_error
        mock_mgr.get_service_logs.return_value = (
            "upgrade failed line1\nupgrade failed line2"
        )
        mock_poll.return_value = {
            "url": "my-app.snowflakecomputing.app",
            "is_upgrading": "false",
        }

        with (
            change_directory(tmp_path),
            patch("snowflake.cli._plugins.apps.commands.log") as mock_log,
        ):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "deploy", "--deploy-only"])
            assert result.exit_code == 1
            assert (
                "Deployment failed while upgrading application service" in result.output
            )
            create_span = _get_completed_span("snowflake_app.deploy_service.create")
            upgrade_span = _get_completed_span("snowflake_app.deploy_service.upgrade")
            # The "already exists" ProgrammingError on CREATE is an
            # expected redeploy signal, not a failure of the Create step;
            # only the upgrade span should record the failure.
            assert create_span[CLIMetricsSpan.ERROR_KEY] is None
            assert upgrade_span[CLIMetricsSpan.ERROR_KEY] == ProgrammingError.__name__
            mock_log.info.assert_any_call("upgrade failed line1")
            mock_log.info.assert_any_call("upgrade failed line2")

        mock_mgr.get_service_logs.assert_called_once()
        service_fqn = mock_mgr.get_service_logs.call_args.args[0]
        assert service_fqn.identifier == "TEST_DB.TEST_SCHEMA.MY_APP"

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
    def test_deploy_wait_does_not_stream_service_logs_on_success(
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

        _, kwargs = mock_poll.call_args
        assert (
            kwargs["timeout_message"]
            == "Application service deployment timed out. Check application service state and logs:\n"
            "  DESCRIBE APPLICATION SERVICE TEST_DB.TEST_SCHEMA.MY_APP\n"
            "  CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('TEST_DB.TEST_SCHEMA.MY_APP')"
        )
        assert "on_poll" not in kwargs
        mock_mgr.get_service_logs.assert_not_called()

    @patch("snowflake.cli._plugins.apps.manager.time.sleep")
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
    def test_deploy_service_failed_status_reports_deployment_failure(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
        mock_sleep,
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
        mock_mgr.get_service_logs.return_value = "failed line1\nfailed line2"
        mock_mgr.resolve_application_service_url_from_describe.return_value = None
        mock_mgr.describe_app_service.return_value = {
            "status": "FAILED",
            "url": "provisioning in progress",
            "is_upgrading": "false",
        }

        with (
            change_directory(tmp_path),
            patch("snowflake.cli._plugins.apps.commands.log") as mock_log,
        ):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "deploy", "--deploy-only"])

        assert result.exit_code == 1
        assert "Application service deployment failed." in result.output
        assert "Check application service state and" in result.output
        assert "logs:" in result.output
        assert (
            "DESCRIBE APPLICATION SERVICE TEST_DB.TEST_SCHEMA.MY_APP" in result.output
        )
        assert (
            "CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('TEST_DB.TEST_SCHEMA.MY_APP')"
            in result.output
        )
        assert "timed out" not in result.output
        assert "Endpoint provisioning" not in result.output
        mock_log.info.assert_any_call("failed line1")
        mock_log.info.assert_any_call("failed line2")

        mock_mgr.get_service_logs.assert_called_once()
        service_fqn = mock_mgr.get_service_logs.call_args.args[0]
        assert service_fqn.identifier == "TEST_DB.TEST_SCHEMA.MY_APP"

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
    def test_deploy_upgrade_wait_uses_fqn_service_log_hint(
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

        already_exists = ProgrammingError("already exists")
        already_exists.errno = 2002

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.create_app_service.side_effect = already_exists
        mock_poll.return_value = {
            "url": "my-app.snowflakecomputing.app",
            "is_upgrading": "false",
        }

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "deploy", "--deploy-only"])
            assert result.exit_code == 0, result.output
            create_span = _get_completed_span("snowflake_app.deploy_service.create")
            upgrade_span = _get_completed_span("snowflake_app.deploy_service.upgrade")
            assert create_span[CLIMetricsSpan.ERROR_KEY] is None
            assert upgrade_span[CLIMetricsSpan.ERROR_KEY] is None

        _, kwargs = mock_poll.call_args
        assert (
            kwargs["timeout_message"]
            == "Upgrade timed out. Check application service state and logs:\n"
            "  DESCRIBE APPLICATION SERVICE TEST_DB.TEST_SCHEMA.MY_APP\n"
            "  CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('TEST_DB.TEST_SCHEMA.MY_APP')"
        )
        assert "on_poll" not in kwargs
        mock_mgr.get_service_logs.assert_not_called()

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
        entity.code_workspace = Mock(
            database=None,
            schema_=None,
        )
        entity.code_workspace.name = "MY_APP_CODE"
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
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
        mock_mgr.workspace_last_subdirectory_uri.return_value = (
            _WORKSPACE_BUILD_SOURCE_URI
        )
        mock_mgr.artifact_repo_exists.return_value = False
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        _real_manager = SnowflakeAppManager()
        mock_mgr.resolve_application_service_url_from_describe.side_effect = (
            _real_manager.resolve_application_service_url_from_describe
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
        mock_mgr.clear_workspace_subdirectory.assert_called_once_with(
            FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP_CODE"),
            "MY_APP",
        )
        mock_mgr.commit_workspace_live_version.assert_called_once_with(
            FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP_CODE")
        )
        mock_mgr.ensure_workspace_live_version.assert_called_once_with(
            FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP_CODE")
        )
        mock_mgr.create_stage.assert_not_called()
        mock_mgr.build_app_artifact_repo.assert_called_once_with(
            source_uri=mock_mgr.workspace_last_subdirectory_uri.return_value,
            artifact_repo_fqn="TEST_DB.TEST_SCHEMA.MY_APP_REPO",
            app_id="MY_APP",
            compute_pool="BUILD_POOL",
            database="TEST_DB",
            schema="TEST_SCHEMA",
            runtime_image="runtime:latest",
            build_eai="MY_EAI",
            project_type="",
        )
        mock_mgr.workspace_last_subdirectory_uri.assert_called_once_with(
            FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP_CODE"),
            "MY_APP",
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
            "database": "USER$guy.bloom@snowflake.com",
            "schema": "APPS",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "USER$guy.bloom@snowflake.com",
            "artifact_repo_schema": "APPS",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_quotes_personal_database_in_artifact_repo_fqn(
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
        """When the resolved deploy defaults point at a personal database
        (e.g. ``USER$first.last@domain.com``), the ``artifact_repo_fqn``
        string forwarded to ``build_app_artifact_repo`` and
        ``create_app_service`` must wrap the database in double quotes.
        ``SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO`` parses this string
        server-side as a Snowflake identifier; without quoting the dots
        in the email cause the parser to interpret it as a 5-part name.
        """
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "USER$guy.bloom@snowflake.com"
        fqn.schema = "APPS"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = Mock(database=None, schema_=None)
        entity.code_workspace.name = "MY_APP_CODE"
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
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
        mock_mgr.workspace_last_subdirectory_uri.return_value = (
            _WORKSPACE_BUILD_SOURCE_URI
        )
        mock_mgr.artifact_repo_exists.return_value = True
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: DB.SCHEMA.BUILD_JOB_123"
        )
        _real_manager = SnowflakeAppManager()
        mock_mgr.resolve_application_service_url_from_describe.side_effect = (
            _real_manager.resolve_application_service_url_from_describe
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

        expected_repo_fqn = '"USER$guy.bloom@snowflake.com".APPS.MY_APP_REPO'
        build_kwargs = mock_mgr.build_app_artifact_repo.call_args.kwargs
        assert build_kwargs["artifact_repo_fqn"] == expected_repo_fqn
        # The session DB/schema are still passed unquoted because
        # ``_use_database_and_schema`` quotes them itself before issuing
        # the ``USE`` statements.
        assert build_kwargs["database"] == "USER$guy.bloom@snowflake.com"
        assert build_kwargs["schema"] == "APPS"
        create_kwargs = mock_mgr.create_app_service.call_args.kwargs
        assert create_kwargs["artifact_repo_fqn"] == expected_repo_fqn

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
    def test_deploy_forwards_project_type_override(
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
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = Mock(database=None, schema_=None)
        entity.code_workspace.name = "MY_APP_CODE"
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
        entity.artifact_repository = None
        entity.build_compute_pool = None
        entity.service_compute_pool = None
        entity.build_eai = None
        entity.spcs_test_project_type = "nextjs"
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.workspace_last_subdirectory_uri.return_value = (
            _WORKSPACE_BUILD_SOURCE_URI
        )
        mock_mgr.artifact_repo_exists.return_value = False
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        _real_manager = SnowflakeAppManager()
        mock_mgr.resolve_application_service_url_from_describe.side_effect = (
            _real_manager.resolve_application_service_url_from_describe
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

        mock_mgr.build_app_artifact_repo.assert_called_once()
        assert mock_mgr.build_app_artifact_repo.call_args.kwargs["project_type"] == (
            "nextjs"
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
            project_type="",
        )

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
    def test_deploy_create_stage_privilege_error_includes_role_guidance(
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
        """A privilege error while creating the code stage is rewrapped with
        the failed action, the stage object, the role, and a privileges hint."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = Mock(
            encryption_type="SNOWFLAKE_SSE",
            database=None,
            schema_=None,
        )
        entity.code_stage.name = "MY_STAGE"
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        create_error = ProgrammingError("Insufficient privileges")
        create_error.errno = 3001

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.stage_exists.return_value = False
        mock_mgr.create_stage.side_effect = create_error
        mock_mgr.current_role.return_value = "APP_DEPLOYER"

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "deploy"])
            assert result.exit_code == 1, result.output
            assert (
                "Failed to create stage 'TEST_DB.TEST_SCHEMA.MY_STAGE'" in result.output
            )
            assert "role 'APP_DEPLOYER'" in result.output
            assert "CREATE STAGE on the schema" in result.output
            prepare_span = _get_completed_span("snowflake_app.upload.prepare_stage")
            assert prepare_span[CLIMetricsSpan.ERROR_KEY] == CliError.__name__

        mock_mgr.build_app_artifact_repo.assert_not_called()

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
    def test_deploy_clear_stage_privilege_error_includes_role_guidance(
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
        """A privilege error while clearing an existing stage reports the clear
        action and the WRITE privilege hint, and falls back to a generic role
        phrase when the role cannot be resolved."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = Mock(
            encryption_type="SNOWFLAKE_SSE",
            database=None,
            schema_=None,
        )
        entity.code_stage.name = "MY_STAGE"
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.stage_exists.return_value = True
        mock_mgr.clear_stage.side_effect = ProgrammingError("Insufficient privileges")
        mock_mgr.current_role.return_value = None

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "deploy"])
            assert result.exit_code == 1, result.output
            assert (
                "Failed to clear existing stage 'TEST_DB.TEST_SCHEMA.MY_STAGE'"
                in result.output
            )
            assert "your role" in result.output
            assert "WRITE on the stage" in result.output

        mock_mgr.create_stage.assert_not_called()
        mock_mgr.build_app_artifact_repo.assert_not_called()

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
            "database": "USER$DEV",
            "schema": "PUBLIC",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "USER$DEV",
            "artifact_repo_schema": "PUBLIC",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_create_workspace_privilege_error_includes_role_guidance(
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
        """A privilege error while creating the workspace is rewrapped with the
        failed action, the workspace object, the role, and a privileges hint."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "USER$DEV"
        fqn.schema = "PUBLIC"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = Mock(database=None, schema_=None)
        entity.code_workspace.name = "SNOWFLAKE_APPS"
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        create_error = ProgrammingError("Insufficient privileges")
        create_error.errno = 3001

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.workspace_subdirectory_uri.return_value = (
            "snow://workspace/USER$DEV.PUBLIC.SNOWFLAKE_APPS/versions/live/MY_APP"
        )
        mock_mgr.create_workspace.side_effect = create_error
        mock_mgr.current_role.return_value = "APP_DEPLOYER"

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "deploy"])
            assert result.exit_code == 1, result.output
            assert (
                "Failed to create workspace "
                "'USER$DEV.PUBLIC.SNOWFLAKE_APPS'" in result.output
            )
            assert "role 'APP_DEPLOYER'" in result.output
            assert "CREATE WORKSPACE on the schema" in result.output
            prepare_span = _get_completed_span("snowflake_app.upload.prepare_workspace")
            assert prepare_span[CLIMetricsSpan.ERROR_KEY] == CliError.__name__

        mock_mgr.clear_workspace_subdirectory.assert_not_called()
        mock_mgr.build_app_artifact_repo.assert_not_called()

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
            "database": "USER$DEV",
            "schema": "PUBLIC",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "USER$DEV",
            "artifact_repo_schema": "PUBLIC",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_clear_workspace_privilege_error_includes_role_guidance(
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
        """A privilege error while clearing existing workspace files reports the
        clear action and the WRITE privilege hint, and falls back to a generic
        role phrase when the role cannot be resolved."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "USER$DEV"
        fqn.schema = "PUBLIC"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = Mock(database=None, schema_=None)
        entity.code_workspace.name = "SNOWFLAKE_APPS"
        entity.artifacts = []
        entity.meta = None
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.workspace_subdirectory_uri.return_value = (
            "snow://workspace/USER$DEV.PUBLIC.SNOWFLAKE_APPS/versions/live/MY_APP"
        )
        mock_mgr.clear_workspace_subdirectory.side_effect = ProgrammingError(
            "Insufficient privileges"
        )
        mock_mgr.current_role.return_value = None

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "deploy"])
            assert result.exit_code == 1, result.output
            assert (
                "Failed to clear workspace files "
                "'USER$DEV.PUBLIC.SNOWFLAKE_APPS'" in result.output
            )
            assert "your role" in result.output
            assert "WRITE on the workspace" in result.output

        mock_mgr.create_workspace.assert_called_once()
        mock_mgr.build_app_artifact_repo.assert_not_called()

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
            "database": "USER$SNOTEBAERT",
            "schema": "PUBLIC",
            "artifact_repository": "MY_APP_REPO",
            "artifact_repo_database": "USER$SNOTEBAERT",
            "artifact_repo_schema": "PUBLIC",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_deploy_personal_db_with_code_stage_is_honored_with_warning(
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
        """An explicit ``code_stage`` pointing at a personal database is honored
        (the stage flow runs), but the user is warned that stages are generally
        unsupported in personal databases."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "USER$SNOTEBAERT"
        fqn.schema = "PUBLIC"
        entity.fqn = fqn
        entity.code_stage = Mock(
            encryption_type="SNOWFLAKE_SSE",
            database=None,
            schema_=None,
        )
        entity.code_stage.name = "MY_APP_CODE"
        entity.code_workspace = None
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
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
            "Build job submitted: USER$SNOTEBAERT.PUBLIC.BUILD_JOB_123"
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
            assert "generally does not support stages" in result.output

        mock_mgr.create_workspace.assert_not_called()
        mock_mgr.create_stage.assert_called_once()
        mock_mgr.build_app_artifact_repo.assert_called_once_with(
            stage_fqn=FQN(
                database="USER$SNOTEBAERT", schema="PUBLIC", name="MY_APP_CODE"
            ),
            artifact_repo_fqn="USER$SNOTEBAERT.PUBLIC.MY_APP_REPO",
            app_id="MY_APP",
            compute_pool="BUILD_POOL",
            database="USER$SNOTEBAERT",
            schema="PUBLIC",
            runtime_image="runtime:latest",
            build_eai="MY_EAI",
            project_type="",
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
        mock_mgr.workspace_last_subdirectory_uri.return_value = (
            _WORKSPACE_BUILD_SOURCE_URI
        )
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
        entity.code_workspace = Mock(database=None, schema_=None)
        entity.code_workspace.name = "MY_APP_CODE"
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
        mock_mgr.workspace_last_subdirectory_uri.return_value = (
            _WORKSPACE_BUILD_SOURCE_URI
        )
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
        entity.code_workspace = Mock(database=None, schema_=None)
        entity.code_workspace.name = "MY_APP_CODE"
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
        mock_mgr.commit_workspace_live_version.assert_called_once()
        mock_mgr.ensure_workspace_live_version.assert_called_once()
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
        mock_mgr.workspace_last_subdirectory_uri.return_value = (
            _WORKSPACE_BUILD_SOURCE_URI
        )
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

    @patch("snowflake.cli._plugins.apps.commands._poll_until")
    @patch("snowflake.cli._plugins.apps.commands.StageManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
            "build_compute_pool": "YML_BUILD_POOL",
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
    def test_compute_pools_passed_through_to_server(
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
        """Resolved ``build_compute_pool`` / ``service_compute_pool`` values
        are forwarded to ``SYSTEM$SPCS_TEST_BUILD_APP_ARTIFACT_REPO`` and
        emitted as an ``IN COMPUTE POOL`` clause on
        ``CREATE APPLICATION SERVICE``, with no managed-pool warning."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = Mock(database=None, schema_=None)
        entity.code_workspace.name = "MY_APP_CODE"
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
        entity.artifact_repository = None
        entity.build_compute_pool = Mock()
        entity.build_compute_pool.name = "YML_BUILD_POOL"
        entity.service_compute_pool = Mock()
        entity.service_compute_pool.name = "YML_SVC_POOL"
        entity.build_eai = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.workspace_last_subdirectory_uri.return_value = (
            _WORKSPACE_BUILD_SOURCE_URI
        )
        mock_mgr.artifact_repo_exists.return_value = False
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        _real_manager = SnowflakeAppManager()
        mock_mgr.resolve_application_service_url_from_describe.side_effect = (
            _real_manager.resolve_application_service_url_from_describe
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
            assert "managed compute pools" not in result.output
            assert "may not honor this value" not in result.output

        _, build_kwargs = mock_mgr.build_app_artifact_repo.call_args
        assert build_kwargs["compute_pool"] == "YML_BUILD_POOL"

        _, create_kwargs = mock_mgr.create_app_service.call_args
        assert create_kwargs["compute_pool"] == "SVC_POOL"

    @patch("snowflake.cli._plugins.apps.commands._poll_until")
    @patch("snowflake.cli._plugins.apps.commands.StageManager")
    @patch("snowflake.cli._plugins.apps.commands.perform_bundle")
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "query_warehouse": "WH",
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
    def test_no_compute_pools_lets_server_allocate(
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
        """When neither ``snowflake.yml`` nor account parameters provide
        compute pools, deploy forwards ``None`` so the server allocates the
        pools itself (no ``IN COMPUTE POOL`` clause, empty 4th build arg)."""
        from snowflake.cli.api.project.project_paths import ProjectPaths

        entity = Mock()
        fqn = Mock()
        fqn.name = "MY_APP"
        fqn.database = "TEST_DB"
        fqn.schema = "TEST_SCHEMA"
        entity.fqn = fqn
        entity.code_stage = None
        entity.code_workspace = Mock(database=None, schema_=None)
        entity.code_workspace.name = "MY_APP_CODE"
        entity.artifacts = []
        entity.meta = None
        entity.runtime_image = "runtime:latest"
        entity.query_warehouse = "WH"
        entity.artifact_repository = None
        entity.build_compute_pool = None
        entity.service_compute_pool = None
        entity.build_eai = None
        mock_get_entity.return_value = entity

        bundle_dir = tmp_path / "output" / "bundle"
        bundle_dir.mkdir(parents=True)
        mock_perform_bundle.return_value = ProjectPaths(project_root=tmp_path)

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.workspace_last_subdirectory_uri.return_value = (
            _WORKSPACE_BUILD_SOURCE_URI
        )
        mock_mgr.artifact_repo_exists.return_value = False
        mock_mgr.build_app_artifact_repo.return_value = (
            "Build job submitted: TEST_DB.TEST_SCHEMA.BUILD_JOB_123"
        )
        _real_manager = SnowflakeAppManager()
        mock_mgr.resolve_application_service_url_from_describe.side_effect = (
            _real_manager.resolve_application_service_url_from_describe
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
            assert "managed compute pools" not in result.output

        _, build_kwargs = mock_mgr.build_app_artifact_repo.call_args
        assert build_kwargs["compute_pool"] is None

        _, create_kwargs = mock_mgr.create_app_service.call_args
        assert create_kwargs["compute_pool"] is None


class TestTeardownCommand:
    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_teardown_detects_application_service_and_drops_it(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
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
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.is_application_service.return_value = True
        mock_mgr.describe_app_service.return_value = {}
        mock_mgr.get_service_status.return_value = "IDLE"

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            result = runner.invoke(["app", "teardown", "--force"])

        assert result.exit_code == 0, result.output
        assert (
            "Successfully dropped application service TEST_DB.TEST_SCHEMA.MY_APP."
            in result.output
        )
        mock_mgr.drop_app_service_if_exists.assert_called_once_with(
            FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        )
        mock_mgr.drop_service_if_exists.assert_not_called()
        mock_mgr.drop_stage_if_exists.assert_called_once_with(
            FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP_CODE")
        )

    @patch("snowflake.cli._plugins.apps.commands.SnowflakeAppManager")
    @patch(
        RESOLVE_DEPLOY_DEFAULTS,
        return_value={
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
        },
    )
    @patch("snowflake.cli._plugins.apps.commands._get_entity")
    @patch(
        "snowflake.cli._plugins.apps.commands._resolve_entity_id",
        return_value="my_app",
    )
    def test_teardown_fails_when_application_service_still_exists(
        self,
        mock_resolve,
        mock_get_entity,
        mock_defaults,
        mock_manager_cls,
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
        entity.artifact_repository = None
        mock_get_entity.return_value = entity

        mock_mgr = mock_manager_cls.return_value
        mock_mgr.is_application_service.return_value = True
        mock_mgr.describe_app_service.return_value = {"status": "READY"}

        with change_directory(tmp_path):
            _write_snowflake_app_yml(tmp_path)
            _reset_command_metrics()
            result = runner.invoke(["app", "teardown", "--force"])

        assert result.exit_code == 1
        assert (
            "Failed to drop application service TEST_DB.TEST_SCHEMA.MY_APP."
            in result.output
        )
        assert "Successfully dropped" not in result.output
        mock_mgr.drop_app_service_if_exists.assert_called_once_with(
            FQN(database="TEST_DB", schema="TEST_SCHEMA", name="MY_APP")
        )
        mock_mgr.drop_stage_if_exists.assert_not_called()
        span = _get_completed_span("snowflake_app.teardown.drop_service")
        assert span[CLIMetricsSpan.ERROR_KEY] == CliError.__name__
