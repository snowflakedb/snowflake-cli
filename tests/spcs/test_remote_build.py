# Copyright (c) 2024 Snowflake Inc.
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

"""
Tests for the ``remote-build`` command and ``RemoteBuildManager``.

Mirrors the style of the ``build-image`` tests in ``test_services.py``.
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from snowflake.cli._plugins.spcs.services.remote_build_manager import (
    RemoteBuildJobStatus,
    RemoteBuildManager,
    RemoteBuildPermanentError,
    RemoteBuildStatus,
    _handle_remote_build_error,
)
from snowflake.cli.api.exceptions import CliConnectionError, CliError

# ---------------------------------------------------------------------------
# Path constants for patching
# ---------------------------------------------------------------------------

_REST_API_SEND = "snowflake.cli._plugins.spcs.services.remote_build_manager.RestApi.send_rest_request"
_COMMANDS_REMOTE_BUILD_MANAGER = (
    "snowflake.cli._plugins.spcs.services.commands.RemoteBuildManager"
)
_COMMANDS_SERVICE_MANAGER = (
    "snowflake.cli._plugins.spcs.services.commands.ServiceManager"
)
_STAGE_EXECUTE_QUERY = "snowflake.cli._plugins.stage.manager.StageManager.execute_query"
_STAGE_PUT = "snowflake.cli._plugins.stage.manager.StageManager.put"
_STAGE_PUT_RECURSIVE = "snowflake.cli._plugins.stage.manager.StageManager.put_recursive"
_COMMANDS_OBJECT_MANAGER = "snowflake.cli._plugins.spcs.services.commands.ObjectManager"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_JOB = {
    "job_name": "remote_build_abc12345",
    "job_status": "DONE",
    "creation_time": "2026-07-11T00:00:00Z",
    "end_time": "2026-07-11T00:05:00Z",
}


# ---------------------------------------------------------------------------
# RemoteBuildStatus / RemoteBuildJobStatus unit tests
# ---------------------------------------------------------------------------


class TestRemoteBuildStatus:
    @pytest.mark.parametrize(
        "status",
        [RemoteBuildStatus.DONE, RemoteBuildStatus.FAILED, RemoteBuildStatus.CANCELLED],
    )
    def test_is_terminal_true(self, status):
        assert status.is_terminal is True

    @pytest.mark.parametrize(
        "status",
        [
            RemoteBuildStatus.PENDING,
            RemoteBuildStatus.RUNNING,
            RemoteBuildStatus.INTERNAL_ERROR,
            RemoteBuildStatus.UNKNOWN,
        ],
    )
    def test_is_terminal_false(self, status):
        assert status.is_terminal is False

    def test_str_equality(self):
        # str Enum so comparisons against the wire-format strings still work.
        assert RemoteBuildStatus.DONE == "DONE"
        assert RemoteBuildStatus("FAILED") is RemoteBuildStatus.FAILED

    def test_missing_maps_unmodeled_to_unknown(self):
        assert RemoteBuildStatus("SOME_NEW_STATUS") is RemoteBuildStatus.UNKNOWN
        assert RemoteBuildStatus("").is_terminal is False
        assert RemoteBuildStatus("SOME_NEW_STATUS").is_terminal is False


class TestRemoteBuildJobStatus:
    def test_from_dict_full(self):
        status = RemoteBuildJobStatus.from_dict(_SAMPLE_JOB)
        assert status.job_name == "remote_build_abc12345"
        assert status.job_status == "DONE"
        assert status.creation_time == "2026-07-11T00:00:00Z"
        assert status.end_time == "2026-07-11T00:05:00Z"

    def test_from_dict_minimal(self):
        status = RemoteBuildJobStatus.from_dict(
            {"job_name": "j", "job_status": "RUNNING"}
        )
        assert status.job_name == "j"
        assert status.job_status == "RUNNING"
        assert status.creation_time is None

    def test_from_dict_missing_fields_use_defaults(self):
        status = RemoteBuildJobStatus.from_dict({})
        assert status.job_name == ""
        assert status.job_status == "UNKNOWN"

    @pytest.mark.parametrize("status", ["DONE", "FAILED", "CANCELLED"])
    def test_is_terminal_true(self, status):
        j = RemoteBuildJobStatus.from_dict({"job_name": "x", "job_status": status})
        assert j.is_terminal is True

    @pytest.mark.parametrize(
        "status", ["PENDING", "RUNNING", "UNKNOWN", "", "SOME_NEW_STATUS"]
    )
    def test_is_terminal_false(self, status):
        j = RemoteBuildJobStatus.from_dict({"job_name": "x", "job_status": status})
        assert j.is_terminal is False


# ---------------------------------------------------------------------------
# RemoteBuildManager unit tests
# ---------------------------------------------------------------------------


class TestRemoteBuildManagerCreateRemoteBuilder:
    """Tests for create_remote_builder → POST /api/v2/remote-build/execute."""

    def _make_manager(self):
        mock_conn = Mock()
        mock_conn.rest.server_url = "https://account.snowflakecomputing.com"
        mock_conn.rest.token = "fake_token"
        mock_conn.rest.fetch = Mock()
        return RemoteBuildManager(connection=mock_conn)

    @patch(_REST_API_SEND)
    def test_create_remote_builder_minimal(self, mock_send):
        """Only build_source required; other fields default."""
        mock_send.return_value = {"job_name": "server_generated_job"}
        manager = self._make_manager()

        result = manager.create_remote_builder(build_source="@MY_DB.PUBLIC.SRC/ctx")

        assert result == "server_generated_job"
        mock_send.assert_called_once()
        _, kwargs = mock_send.call_args
        assert kwargs.get("url") or mock_send.call_args[0][0]
        call_kwargs = mock_send.call_args
        url = call_kwargs[1].get("url") or call_kwargs[0][0]
        assert url.endswith("/remote-build/execute")
        body = call_kwargs[1].get("data") or call_kwargs[0][2]
        assert body["build_source"] == "@MY_DB.PUBLIC.SRC/ctx"
        assert body["build_type"] == "image"
        assert "location" not in body
        assert "name" not in body
        assert "image_tag" not in body
        assert "job_name" not in body

    @patch(_REST_API_SEND)
    def test_create_remote_builder_all_fields(self, mock_send):
        """All optional fields appear in the request body when provided."""
        mock_send.return_value = {"job_name": "server_assigned_job"}
        manager = self._make_manager()

        result = manager.create_remote_builder(
            build_source="@MY_DB.PUBLIC.SRC/ctx",
            location="my_db.my_schema.my_repo",
            name="my_image",
            image_tag="v1.0",
            project_type="node",
            compute_pool="MY_POOL",
            build_type="app",
        )

        assert result == "server_assigned_job"
        body = mock_send.call_args[1]["data"]
        assert body["build_source"] == "@MY_DB.PUBLIC.SRC/ctx"
        assert body["location"] == "my_db.my_schema.my_repo"
        assert body["name"] == "my_image"
        assert body["image_tag"] == "v1.0"
        assert body["project_type"] == "node"
        assert body["compute_pool"] == "MY_POOL"
        assert body["build_type"] == "app"
        # These SPCS-specific knobs were removed from the CLI/manager surface so the
        # backend can change (e.g. SPCS → CNG) without a CLI-facing contract change.
        assert "runtime_image" not in body
        assert "config" not in body
        assert "job_name" not in body

    @patch(_REST_API_SEND)
    def test_create_remote_builder_no_job_name_in_response_raises(self, mock_send):
        """Missing job_name in server response raises CliError."""
        mock_send.return_value = {"status": "ok"}
        manager = self._make_manager()

        with pytest.raises(CliError, match="did not return a job_name"):
            manager.create_remote_builder(build_source="@stage/ctx")

    @patch(_REST_API_SEND)
    def test_create_remote_builder_empty_response_raises(self, mock_send):
        """Empty/None response raises CliError."""
        mock_send.return_value = None
        manager = self._make_manager()

        with pytest.raises(CliError, match="did not return a job_name"):
            manager.create_remote_builder(build_source="@stage/ctx")

    @patch(_REST_API_SEND)
    def test_create_remote_builder_http_403_raises(self, mock_send):
        """403 from the server maps to a CliError listing all relevant account params."""
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=403,
        ):
            mock_send.side_effect = err
            manager = self._make_manager()
            with pytest.raises(CliError) as exc_info:
                manager.create_remote_builder(build_source="@stage/ctx")
        msg = exc_info.value.format_message()
        assert "403 Forbidden" in msg
        assert "ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS" in msg
        assert "ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS" in msg

    @patch(_REST_API_SEND)
    def test_create_remote_builder_http_405_raises(self, mock_send):
        """405 from the server maps to an ENABLE_SNOW_API_FOR_REMOTE_BUILD hint."""
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=405,
        ):
            mock_send.side_effect = err
            manager = self._make_manager()
            with pytest.raises(CliError, match="ENABLE_SNOW_API_FOR_REMOTE_BUILD"):
                manager.create_remote_builder(build_source="@stage/ctx")


class TestRemoteBuildManagerGetRemoteBuilder:
    """Tests for get_remote_builder → GET /api/v2/remote-build/jobs/{job_name}.

    The server returns the RemoteBuildJob object directly (not wrapped in a ``jobs`` list) —
    single-job lookup is a path parameter, distinct from the paginated list endpoint.
    """

    def _make_manager(self):
        mock_conn = Mock()
        mock_conn.rest.server_url = "https://account.snowflakecomputing.com"
        mock_conn.rest.token = "fake_token"
        return RemoteBuildManager(connection=mock_conn)

    @patch(_REST_API_SEND)
    def test_get_remote_builder_found(self, mock_send):
        mock_send.return_value = dict(_SAMPLE_JOB)
        manager = self._make_manager()

        result = manager.get_remote_builder("remote_build_abc12345")

        assert result is not None
        assert result.job_name == "remote_build_abc12345"
        assert result.job_status == "DONE"

        url = mock_send.call_args[1]["url"]
        assert url.endswith("/remote-build/jobs/remote_build_abc12345")
        assert "?" not in url
        assert mock_send.call_args[1]["method"] == "get"

    @patch(_REST_API_SEND)
    def test_get_remote_builder_empty_response_returns_none(self, mock_send):
        """Empty/None response → None (not an exception)."""
        mock_send.return_value = None
        manager = self._make_manager()

        result = manager.get_remote_builder("nonexistent_job")

        assert result is None

    @patch(_REST_API_SEND)
    def test_get_remote_builder_http_404_returns_none(self, mock_send):
        """404 from server means job doesn't exist → return None, not raise."""
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=404,
        ):
            mock_send.side_effect = err
            manager = self._make_manager()
            result = manager.get_remote_builder("missing_job")
            assert result is None

    @patch(_REST_API_SEND)
    def test_get_remote_builder_http_403_still_raises(self, mock_send):
        """Non-404 HTTP errors still propagate as CliError."""
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=403,
        ):
            mock_send.side_effect = err
            manager = self._make_manager()
            with pytest.raises(CliError, match="403 Forbidden"):
                manager.get_remote_builder("some_job")

    @patch(_REST_API_SEND)
    def test_get_remote_builder_url_encodes_job_name(self, mock_send):
        """job_name with special characters is percent-encoded as a URL path segment."""
        mock_send.return_value = dict(_SAMPLE_JOB)
        manager = self._make_manager()

        manager.get_remote_builder("MY DB.PUBLIC.JOB NAME")

        url = mock_send.call_args[1]["url"]
        # Raw spaces must never appear in the URL.
        assert " " not in url
        # Path-segment quoting percent-encodes spaces as %20 (not '+', which is only valid
        # in query strings).
        assert "MY%20DB" in url
        assert "?" not in url


class TestRemoteBuildManagerListRemoteBuilds:
    """Tests for list_remote_build_jobs → GET /api/v2/remote-build/jobs."""

    def _make_manager(self):
        mock_conn = Mock()
        mock_conn.rest.server_url = "https://account.snowflakecomputing.com"
        mock_conn.rest.token = "fake_token"
        return RemoteBuildManager(connection=mock_conn)

    @patch(_REST_API_SEND)
    def test_list_no_params(self, mock_send):
        mock_send.return_value = {"jobs": [_SAMPLE_JOB]}
        manager = self._make_manager()

        result = manager.list_remote_build_jobs()

        url = mock_send.call_args[1]["url"]
        assert url.endswith("/remote-build/jobs")
        assert "?" not in url
        assert result == {"jobs": [_SAMPLE_JOB]}

    @patch(_REST_API_SEND)
    def test_list_with_limit(self, mock_send):
        mock_send.return_value = {"jobs": [_SAMPLE_JOB]}
        manager = self._make_manager()

        manager.list_remote_build_jobs(limit=20)

        url = mock_send.call_args[1]["url"]
        assert "limit=20" in url

    @patch(_REST_API_SEND)
    def test_list_with_page_token(self, mock_send):
        mock_send.return_value = {"jobs": [], "next_page_token": None}
        manager = self._make_manager()

        manager.list_remote_build_jobs(limit=10, page_token="abc123token")

        url = mock_send.call_args[1]["url"]
        assert "limit=10" in url
        assert "page_token=abc123token" in url

    @patch(_REST_API_SEND)
    def test_list_url_encodes_page_token(self, mock_send):
        """page_token with special characters is percent-encoded (no raw slashes or equals)."""
        mock_send.return_value = {"jobs": []}
        manager = self._make_manager()

        # Use a token that contains characters illegal in a raw query-string value.
        manager.list_remote_build_jobs(page_token="tok/val=ue")

        url = mock_send.call_args[1]["url"]
        assert " " not in url
        assert "page_token=" in url
        # Raw '/' and '=' inside the token value must be percent-encoded by urlencode.
        raw_qs = url.split("?", 1)[-1]
        # After the 'page_token=' prefix the rest of the value should not contain a raw '/'.
        token_value = raw_qs.split("page_token=", 1)[-1].split("&")[0]
        assert "/" not in token_value
        # The literal '=' delimiter must only appear as the key=value separator, not raw inside the value.
        assert token_value.count("=") == 0  # urlencode encodes '=' in values as %3D

    @patch(_REST_API_SEND)
    def test_list_returns_empty_dict_on_none_response(self, mock_send):
        mock_send.return_value = None
        manager = self._make_manager()

        result = manager.list_remote_build_jobs()

        assert result == {"jobs": []}


# ---------------------------------------------------------------------------
# _handle_remote_build_error helper tests
# ---------------------------------------------------------------------------


class TestHandleRemoteBuildError:
    def _raise(self, err, operation="test"):
        with pytest.raises(CliError):
            _handle_remote_build_error(err, operation)

    def test_400_bad_request(self):
        from snowflake.connector.errors import BadRequest

        err = BadRequest(msg="bad")
        with pytest.raises(RemoteBuildPermanentError, match="400 Bad Request"):
            _handle_remote_build_error(err, "submit")

    def test_snowservices_not_enabled_mentions_all_flags(self):
        """SNOWSERVICES_NOT_ENABLED in the error body surfaces all relevant account params."""
        err = Exception(
            "250065: SNOWSERVICES_NOT_ENABLED: feature not enabled for account"
        )
        with pytest.raises(RemoteBuildPermanentError) as exc_info:
            _handle_remote_build_error(err, "submit build")
        msg = exc_info.value.format_message()
        assert "ENABLE_SNOW_API_FOR_REMOTE_BUILD" in msg
        assert "ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS" in msg
        assert "ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS" in msg

    def test_feature_not_enabled_generic_wording(self):
        """Generic 'feature not enabled' error text also triggers the flag hint."""
        err = Exception("feature not enabled for this account")
        with pytest.raises(RemoteBuildPermanentError) as exc_info:
            _handle_remote_build_error(err, "submit build")
        msg = exc_info.value.format_message()
        assert "ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS" in msg
        assert "ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS" in msg

    def test_401(self):
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=401,
        ):
            with pytest.raises(RemoteBuildPermanentError, match="401 Unauthorized"):
                _handle_remote_build_error(err, "op")

    def test_403_mentions_all_flags(self):
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=403,
        ):
            with pytest.raises(RemoteBuildPermanentError) as exc_info:
                _handle_remote_build_error(err, "op")
        msg = exc_info.value.format_message()
        assert "403 Forbidden" in msg
        assert "ENABLE_SNOW_API_FOR_REMOTE_BUILD" in msg
        assert "ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS" in msg
        assert "ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS" in msg

    def test_404(self):
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=404,
        ):
            with pytest.raises(RemoteBuildPermanentError, match="404 Not Found"):
                _handle_remote_build_error(err, "op")

    def test_405_mentions_all_flags(self):
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=405,
        ):
            with pytest.raises(RemoteBuildPermanentError) as exc_info:
                _handle_remote_build_error(err, "op")
        msg = exc_info.value.format_message()
        assert "ENABLE_SNOW_API_FOR_REMOTE_BUILD" in msg
        assert "ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS" in msg
        assert "ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS" in msg

    def test_unknown_http_code_is_not_permanent(self):
        """Unclassified HTTP codes (e.g. 500) may be transient, so they must raise a
        plain CliError rather than RemoteBuildPermanentError — callers that
        retry on transient errors should still retry these.
        """
        err = Mock(spec=Exception)
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=500,
        ):
            with pytest.raises(CliError, match="HTTP 500") as exc_info:
                _handle_remote_build_error(err, "op")
        assert not isinstance(exc_info.value, RemoteBuildPermanentError)

    def test_non_http_error_is_connection_error(self):
        """A network-level failure with no HTTP status at all must raise
        CliConnectionError (treated as transient/retryable), not RemoteBuildPermanentError.
        """
        err = ValueError("some internal error")
        with patch(
            "snowflake.cli.api.connector_errors.get_http_status_code",
            return_value=None,
        ):
            with pytest.raises(
                CliConnectionError, match="some internal error"
            ) as exc_info:
                _handle_remote_build_error(err, "op")
        assert not isinstance(exc_info.value, RemoteBuildPermanentError)


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


def _make_build_context(tmp_dir: Path) -> Path:
    """Create a minimal build context directory with a Dockerfile."""
    ctx = tmp_dir / "build_context"
    ctx.mkdir()
    (ctx / "Dockerfile").write_text("FROM alpine:3.18\nRUN echo hello")
    (ctx / "app.py").write_text("print('hello')")
    return ctx


class TestRemoteBuildCliValidation:
    """Input validation tests — no mocking of managers needed."""

    def test_missing_dockerfile(self, runner, temporary_directory):
        tmp = Path(temporary_directory)
        ctx = tmp / "ctx"
        ctx.mkdir()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code != 0
        assert "Dockerfile not found" in result.output

    def test_dockerfile_is_a_directory(self, runner, temporary_directory):
        tmp = Path(temporary_directory)
        ctx = tmp / "ctx"
        ctx.mkdir()
        (ctx / "Dockerfile").mkdir()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code != 0
        assert "Dockerfile not found" in result.output

    def test_invalid_name(self, runner, temporary_directory):
        ctx = _make_build_context(Path(temporary_directory))

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--name",
                "invalid@name",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code != 0
        assert "Invalid name" in result.output

    def test_invalid_image_tag(self, runner, temporary_directory):
        ctx = _make_build_context(Path(temporary_directory))

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--image-tag",
                "invalid tag!",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code != 0
        assert "Invalid image tag" in result.output

    def test_invalid_build_type(self, runner, temporary_directory):
        ctx = _make_build_context(Path(temporary_directory))

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--build-type",
                "invalid_type",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code != 0
        assert "Invalid build type" in result.output

    def test_job_name_option_removed(self, runner, temporary_directory):
        """--job-name, --runtime-image, and --config were removed from remote-build to
        keep the CLI surface backend-agnostic (SPCS-specific knobs); the server always
        assigns its own job name and these are no longer accepted.
        """
        ctx = _make_build_context(Path(temporary_directory))

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--job-name",
                "some_job",
            ],
        )

        assert result.exit_code != 0
        assert "No such option" in result.output

    def test_remote_build_hidden_by_default(self, runner):
        """remote-build does not appear in help when the feature flag is off."""
        result = runner.invoke(["spcs", "service", "--help"])
        assert result.exit_code == 0
        assert "remote-build" not in result.output


class TestRemoteBuildCliSuccess:
    """Happy-path and edge-case CLI tests that mock the manager layer."""

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_remote_build_success_with_explicit_stage(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """Happy path using an explicit --stage to avoid a real Snowflake connection."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "remote_build_test123"

        running_status = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "remote_build_test123", "job_status": "RUNNING"}
        )
        done_status = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "remote_build_test123", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.side_effect = [running_status, done_status]

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = ["2026-07-11T00:00:01Z Building image..."]

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--location",
                "my_db.my_schema.my_repo",
                "--name",
                "my_image",
                "--image-tag",
                "v1.0",
                "--stage",
                "test_db.public.test_stage",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        # create_remote_builder was called with the right arguments
        mock_rb_manager.create_remote_builder.assert_called_once()
        call_kwargs = mock_rb_manager.create_remote_builder.call_args[1]
        assert call_kwargs["location"] == "my_db.my_schema.my_repo"
        assert call_kwargs["name"] == "my_image"
        assert call_kwargs["image_tag"] == "v1.0"
        assert call_kwargs["build_type"] == "image"
        assert "@" in call_kwargs["build_source"]
        assert "build_contexts/" in call_kwargs["build_source"]

        # With explicit --stage, the stage is NOT dropped
        mock_obj_manager.drop.assert_not_called()

        # Success message appears
        assert "remote_build_test123" in result.output or "DONE" in result.output

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    @patch("snowflake.connector.connect")
    def test_remote_build_success_with_temp_stage(
        self,
        mock_connector,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
        mock_ctx,
    ):
        """Temp stage is created (no --stage) and cleaned up after the build."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_connector.return_value = mock_ctx()
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "remote_build_tmp123"
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "remote_build_tmp123", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.return_value = done

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        # Temporary stage was dropped after the build
        mock_obj_manager.drop.assert_called_once()
        drop_kwargs = mock_obj_manager.drop.call_args[1]
        assert drop_kwargs["object_type"] == "stage"

        # build_source contains the auto-generated stage path
        call_kwargs = mock_rb_manager.create_remote_builder.call_args[1]
        assert call_kwargs["build_source"].startswith("@")
        assert "build_contexts/" in call_kwargs["build_source"]

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_remote_build_with_custom_stage_does_not_drop_stage(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """When --stage is provided, the stage is not dropped after the build."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "remote_build_cust"
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "remote_build_cust", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.return_value = done

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "my_db.my_schema.existing_stage",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        # Stage should NOT be dropped (it was customer-provided)
        mock_obj_manager.drop.assert_not_called()

        # The build source contains the customer stage name
        call_kwargs = mock_rb_manager.create_remote_builder.call_args[1]
        assert "existing_stage" in call_kwargs["build_source"]

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_remote_build_app_build_type(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """--build-type app is forwarded to create_remote_builder."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "app_build_job"
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "app_build_job", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.return_value = done

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        mock_obj_manager_class.return_value = Mock()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "test_stage",
                "--build-type",
                "app",
                "--location",
                "testdb.testschema.test_artifact_repo",
                "--name",
                "my_app",
                "--project-type",
                "node",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        call_kwargs = mock_rb_manager.create_remote_builder.call_args[1]
        assert call_kwargs["build_type"] == "app"
        assert call_kwargs["location"] == "testdb.testschema.test_artifact_repo"
        assert call_kwargs["name"] == "my_app"
        assert call_kwargs["project_type"] == "node"

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_remote_build_failed_job_warns(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """When the job ends with FAILED the command reports the status."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "fail_job"
        running = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "fail_job", "job_status": "RUNNING"}
        )
        failed = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "fail_job", "job_status": "FAILED"}
        )
        mock_rb_manager.get_remote_builder.side_effect = [running, failed]

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = ["2026-07-11T00:00:01Z Error occurred"]

        mock_obj_manager_class.return_value = Mock()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "test_stage",
            ],
            catch_exceptions=False,
        )

        # FAILED is a terminal state but not success — must exit non-zero so automation detects it.
        assert result.exit_code != 0
        assert "FAILED" in result.output or "failed" in result.output

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    @patch("snowflake.connector.connect")
    def test_remote_build_stage_cleanup_on_submit_error(
        self,
        mock_connector,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
        mock_ctx,
    ):
        """If create_remote_builder raises, the temporary stage is still cleaned up."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_connector.return_value = mock_ctx()
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.side_effect = CliError(
            "405 Method Not Allowed. ENABLE_SNOW_API_FOR_REMOTE_BUILD"
        )

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager
        mock_svc_manager_class.return_value = Mock()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                # No --stage: uses temp stage that should be dropped on error
            ],
        )

        # Command should exit with an error
        assert result.exit_code != 0
        # The temp stage should still have been dropped
        mock_obj_manager.drop.assert_called_once()

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    @patch("snowflake.connector.connect")
    def test_remote_build_stage_cleanup_on_upload_error(
        self,
        mock_connector,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
        mock_ctx,
    ):
        """Fix #3: if put_recursive raises, the temporary stage is cleaned up (stage leak fix)."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_connector.return_value = mock_ctx()
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager
        mock_svc_manager_class.return_value = Mock()
        mock_rb_manager_class.return_value = Mock()

        with patch(
            _STAGE_PUT_RECURSIVE, side_effect=Exception("Network error during upload")
        ):
            result = runner.invoke(
                [
                    "spcs",
                    "service",
                    "remote-build",
                    "--build-context-dir",
                    str(ctx),
                    # No --stage: uses temp stage — must be dropped even on upload failure.
                ],
            )

        assert result.exit_code != 0
        # The temp stage must have been cleaned up despite the upload error.
        mock_obj_manager.drop.assert_called_once()

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_logs_are_streamed_opportunistically_via_sql(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """Holistic redesign: the unified wait loop fetches and prints new log lines via
        ``ServiceManager.logs`` every iteration, with no separate SQL-readiness gate.
        REST (``get_remote_builder``) alone determines completion.
        """
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "sql_log_job"
        running = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "sql_log_job", "job_status": "RUNNING"}
        )
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "sql_log_job", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.side_effect = [running, done]

        mock_obj_manager_class.return_value = Mock()

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = ["2026-07-11T00:00:01Z Building image..."]

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "test_stage",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "Building image..." in result.output
        assert "completed successfully" in result.output
        # logs() is called with the live job name each iteration.
        mock_svc_manager.logs.assert_called()
        assert mock_svc_manager.logs.call_args[1]["service_name"] == "sql_log_job"

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_logs_fetch_failure_does_not_block_completion_detection(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """If the SQL plane never becomes available (``ServiceManager.logs`` keeps
        raising), the command must not crash — REST polling alone still determines
        the final status, since log streaming is best-effort and never gates
        completion detection.
        """
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "no_sql_job"
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "no_sql_job", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.return_value = done

        mock_obj_manager_class.return_value = Mock()

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        # SQL plane never catches up — every log fetch fails.
        mock_svc_manager.logs.side_effect = Exception("service not visible")

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "test_stage",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "completed successfully" in result.output

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_transient_rest_errors_are_retried_without_aborting(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """Transient REST errors (network blips, 5xx) are retried in place by the
        unified wait loop rather than aborting the whole command.
        """
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "flaky_job"
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "flaky_job", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.side_effect = [
            Exception("network blip"),
            Exception("network blip"),
            done,
        ]

        mock_obj_manager_class.return_value = Mock()
        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "test_stage",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "REST status check failed" in result.output
        assert "completed successfully" in result.output
        assert mock_rb_manager.get_remote_builder.call_count == 3

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    @patch("snowflake.connector.connect")
    def test_permanent_rest_error_fails_fast_without_retrying(
        self,
        mock_connector,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
        mock_ctx,
    ):
        """A definitive, non-retryable REST failure (401/403/etc., raised as
        RemoteBuildPermanentError) must propagate immediately instead of being retried
        for the length of the wait budget. The stage must be preserved (job may still
        be running server-side) and the user pointed at remote-build-status.
        """
        from snowflake.cli._plugins.spcs.services.remote_build_manager import (
            RemoteBuildPermanentError,
        )

        ctx = _make_build_context(Path(temporary_directory))
        mock_connector.return_value = mock_ctx()
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "forbidden_job"
        mock_rb_manager.get_remote_builder.side_effect = RemoteBuildPermanentError(
            "Remote build API returned 403 Forbidden while trying to get job "
            "'forbidden_job'."
        )

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager
        mock_svc_manager_class.return_value = Mock()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                # No --stage: uses a temp stage, which must NOT be dropped since the
                # job's status could not be confirmed.
            ],
        )

        assert result.exit_code != 0
        assert "403 Forbidden" in result.output
        # Must not have retried — a single call establishes the permanent failure.
        assert mock_rb_manager.get_remote_builder.call_count == 1
        assert "REST status check failed" not in result.output
        # Stage must be preserved — the job may still be running server-side even
        # though we can no longer confirm its status.
        mock_obj_manager.drop.assert_not_called()
        assert "remote-build-status" in result.output

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_keyboard_interrupt_during_wait_detaches_gracefully(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """Ctrl+C during the wait loop is detected via a SIGINT flag (installed by
        ``_wait_for_remote_build_completion``) rather than relying on a
        ``KeyboardInterrupt`` propagating out of a blocking call — which would never
        happen for SQL log streaming since ``ServiceManager.stream_logs`` swallows it
        internally. The command must detach and print background guidance without
        touching the stage.

        The handler is invoked directly (not via ``os.kill``) so this works on Windows,
        where ``os.kill(pid, SIGINT)`` terminates the process instead of delivering the
        signal to a Python handler.
        """
        import signal as signal_module

        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "interrupt_job"
        running = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "interrupt_job", "job_status": "RUNNING"}
        )
        mock_rb_manager.get_remote_builder.return_value = running

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        # Capture the SIGINT handler installed by the wait loop, then fire it from
        # sleep so the next loop iteration sees interrupt_flag.is_set.
        installed_handlers: list = []
        real_signal = signal_module.signal

        def _spy_signal(signum, handler):
            previous = real_signal(signum, handler)
            if signum == signal_module.SIGINT and callable(handler):
                installed_handlers.append(handler)
            return previous

        def _fire_interrupt(*_args, **_kwargs):
            assert (
                installed_handlers
            ), "SIGINT handler was not installed by the wait loop"
            installed_handlers[-1](signal_module.SIGINT, None)

        mock_sleep.side_effect = _fire_interrupt

        with patch(
            "snowflake.cli._plugins.spcs.services.commands.signal.signal",
            side_effect=_spy_signal,
        ):
            result = runner.invoke(
                [
                    "spcs",
                    "service",
                    "remote-build",
                    "--build-context-dir",
                    str(ctx),
                    "--stage",
                    "test_stage",
                ],
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        assert "running in the background" in result.output
        # Stage must not be touched — the job may still be running server-side.
        mock_obj_manager.drop.assert_not_called()

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_terminal_status_returned_immediately_on_first_rest_poll(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """REST (``get_remote_builder``) is the single source of truth for completion in
        the unified wait loop: as soon as it reports a terminal status the loop returns,
        without depending on any SQL-side signal (sentinel or otherwise).
        """
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "immediate_job"
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "immediate_job", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.return_value = done

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        mock_obj_manager_class.return_value = Mock()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "test_stage",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "completed successfully" in result.output
        # Terminal status observed on the very first poll — no need to sleep/retry.
        assert mock_rb_manager.get_remote_builder.call_count == 1

    @patch(
        "snowflake.cli._plugins.spcs.services.commands._REMOTE_BUILD_OVERALL_WAIT_SECONDS",
        10,
    )
    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    @patch("snowflake.connector.connect")
    def test_timeout_does_not_clean_up_stage(
        self,
        mock_connector,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
        mock_ctx,
    ):
        """When the job is still PENDING after the overall wait budget elapses, the temp
        stage is NOT dropped.

        The build was already submitted to the server; the build context on the stage may still
        be needed by the queued job. Stage cleanup must only happen after a confirmed terminal
        status.
        """
        ctx = _make_build_context(Path(temporary_directory))
        mock_connector.return_value = mock_ctx()
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "pending_job"
        # Job stays PENDING for all poll iterations.
        mock_rb_manager.get_remote_builder.return_value = (
            RemoteBuildJobStatus.from_dict(
                {**_SAMPLE_JOB, "job_name": "pending_job", "job_status": "PENDING"}
            )
        )

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager
        mock_svc_manager_class.return_value = Mock()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                # No --stage: creates a temp stage that must NOT be dropped on timeout.
            ],
            catch_exceptions=False,
        )

        # Timing out is an inconclusive outcome — must exit non-zero so CI/CD pipelines
        # don't treat a submitted-but-unstarted job as a successful build.
        assert result.exit_code != 0
        # Stage must NOT be dropped — the submitted job may still need the build context.
        mock_obj_manager.drop.assert_not_called()
        # User should be told the job was submitted and how to check on it.
        assert "pending_job" in result.output
        assert "remote-build-status" in result.output

    @patch(
        "snowflake.cli._plugins.spcs.services.commands._REMOTE_BUILD_OVERALL_WAIT_SECONDS",
        10,
    )
    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_stage_not_cleaned_up_when_final_status_not_terminal(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """Stage is NOT cleaned up when final status is non-terminal (e.g. RUNNING).

        If the overall wait budget elapses while REST still reports RUNNING, the job may
        still be active. Cleaning up the stage at that point would destroy the build
        context before the job finishes.
        """
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "indeterminate_job"

        running = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "indeterminate_job", "job_status": "RUNNING"}
        )
        # Poll returns RUNNING initially; final REST poll also returns RUNNING (still active).
        mock_rb_manager.get_remote_builder.return_value = running

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        mock_obj_manager = Mock()
        mock_obj_manager_class.return_value = mock_obj_manager

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "test_stage",
            ],
            catch_exceptions=False,
        )

        # Non-terminal status is unconfirmed — exits non-zero so automation doesn't
        # treat it as success.
        assert result.exit_code != 0
        # Stage must NOT be dropped when status is non-terminal.
        mock_obj_manager.drop.assert_not_called()
        assert "did not reach a terminal state" in result.output

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_remote_build_build_source_format(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """build_source sent to the REST API starts with @ and includes the stage and context path."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "bs_job"
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "bs_job", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.return_value = done

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        mock_obj_manager_class.return_value = Mock()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "MY_DB.PUBLIC.MY_STAGE",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        call_kwargs = mock_rb_manager.create_remote_builder.call_args[1]
        build_source = call_kwargs["build_source"]
        assert build_source.startswith("@")
        assert "MY_STAGE" in build_source or "my_stage" in build_source.lower()
        assert "build_contexts/" in build_source


class TestRemoteBuildCliDefaultsAndNones:
    """Verify optional flags default to None/image and are passed through correctly."""

    @patch("time.sleep")
    @patch(_COMMANDS_OBJECT_MANAGER)
    @patch(_COMMANDS_SERVICE_MANAGER)
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    @patch(_STAGE_PUT)
    @patch(_STAGE_EXECUTE_QUERY)
    def test_optional_flags_default_to_none(
        self,
        mock_stage_exec,
        mock_stage_put,
        mock_rb_manager_class,
        mock_svc_manager_class,
        mock_obj_manager_class,
        mock_sleep,
        runner,
        temporary_directory,
    ):
        """location, name, and image_tag default to None."""
        ctx = _make_build_context(Path(temporary_directory))
        mock_stage_put.return_value = Mock(fetchall=lambda: [])

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.create_remote_builder.return_value = "auto_job"
        done = RemoteBuildJobStatus.from_dict(
            {**_SAMPLE_JOB, "job_name": "auto_job", "job_status": "DONE"}
        )
        mock_rb_manager.get_remote_builder.return_value = done

        mock_svc_manager = Mock()
        mock_svc_manager_class.return_value = mock_svc_manager
        mock_svc_manager.logs.return_value = []

        mock_obj_manager_class.return_value = Mock()

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build",
                "--build-context-dir",
                str(ctx),
                "--stage",
                "test_stage",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        call_kwargs = mock_rb_manager.create_remote_builder.call_args[1]
        assert call_kwargs["location"] is None
        assert call_kwargs["name"] is None
        assert call_kwargs["image_tag"] is None
        assert call_kwargs["build_type"] == "image"
        # runtime_image, config, and job_name were removed from the CLI/manager surface.
        assert "runtime_image" not in call_kwargs
        assert "config" not in call_kwargs
        assert "job_name" not in call_kwargs


# ---------------------------------------------------------------------------
# remote-build-status command tests
# ---------------------------------------------------------------------------


class TestRemoteBuildStatusCommand:
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_status_found(self, mock_rb_manager_class, runner):
        """Returns job details when the job exists."""
        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.get_remote_builder.return_value = (
            RemoteBuildJobStatus.from_dict(
                {
                    **_SAMPLE_JOB,
                    "job_name": "MYDB.PUBLIC.SPCS_IMAGE_BUILDER_JOB_abc123",
                    "job_status": "DONE",
                }
            )
        )

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build-status",
                "--job-name",
                "MYDB.PUBLIC.SPCS_IMAGE_BUILDER_JOB_abc123",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        mock_rb_manager.get_remote_builder.assert_called_once_with(
            "MYDB.PUBLIC.SPCS_IMAGE_BUILDER_JOB_abc123"
        )
        assert "DONE" in result.output
        assert "MYDB.PUBLIC.SPCS_IMAGE_BUILDER_JOB_abc123" in result.output

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_status_not_found(self, mock_rb_manager_class, runner):
        """Returns a not-found message when the job doesn't exist."""
        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.get_remote_builder.return_value = None

        result = runner.invoke(
            [
                "spcs",
                "service",
                "remote-build-status",
                "--job-name",
                "MYDB.PUBLIC.MISSING",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "No remote build job found" in result.output

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_status_propagates_error(self, mock_rb_manager_class, runner):
        """HTTP errors from get_remote_builder surface as a non-zero exit."""
        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.get_remote_builder.side_effect = CliError("403 Forbidden")

        result = runner.invoke(
            ["spcs", "service", "remote-build-status", "--job-name", "MYDB.PUBLIC.JOB"],
        )

        assert result.exit_code != 0
        assert "403 Forbidden" in result.output


# ---------------------------------------------------------------------------
# remote-build-history command tests
# ---------------------------------------------------------------------------


class TestRemoteBuildHistoryCommand:
    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_single_page(self, mock_rb_manager_class, runner):
        """All jobs on a single page are returned; no further calls are made."""
        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.list_remote_build_jobs.return_value = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_1", "job_status": "DONE"},
                {
                    **_SAMPLE_JOB,
                    "job_name": "MYDB.PUBLIC.JOB_2",
                    "job_status": "RUNNING",
                },
            ],
            "next_page_token": None,
        }

        result = runner.invoke(
            ["spcs", "service", "remote-build-history"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "JOB_1" in result.output
        assert "JOB_2" in result.output
        assert "DONE" in result.output
        assert "RUNNING" in result.output
        # Default page-size=50, no start token
        mock_rb_manager.list_remote_build_jobs.assert_called_once_with(
            limit=50, page_token=None
        )

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_auto_paginates_across_pages(self, mock_rb_manager_class, runner):
        """All server pages are followed automatically; jobs from every page appear."""
        import base64
        import time as time_mod

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager

        def _encode(days_ago: int) -> str:
            ms = int(time_mod.time() * 1000) - days_ago * 24 * 60 * 60 * 1000
            return base64.urlsafe_b64encode(str(ms).encode()).rstrip(b"=").decode()

        # Tokens point 5 and 10 days ago — both within the 30-day cutoff.
        token_p2 = _encode(5)
        token_p3 = _encode(10)

        page1 = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_1", "job_status": "DONE"}
            ],
            "next_page_token": token_p2,
        }
        page2 = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_2", "job_status": "FAILED"}
            ],
            "next_page_token": token_p3,
        }
        page3 = {
            "jobs": [
                {
                    **_SAMPLE_JOB,
                    "job_name": "MYDB.PUBLIC.JOB_3",
                    "job_status": "RUNNING",
                }
            ],
            "next_page_token": None,
        }
        mock_rb_manager.list_remote_build_jobs.side_effect = [page1, page2, page3]

        result = runner.invoke(
            ["spcs", "service", "remote-build-history", "--page-size", "1"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "JOB_1" in result.output
        assert "JOB_2" in result.output
        assert "JOB_3" in result.output

        from unittest.mock import call

        mock_rb_manager.list_remote_build_jobs.assert_has_calls(
            [
                call(limit=1, page_token=None),
                call(limit=1, page_token=token_p2),
                call(limit=1, page_token=token_p3),
            ]
        )

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_start_token_used_for_first_page(
        self, mock_rb_manager_class, runner
    ):
        """--start-token is passed as the page_token for the first request."""
        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.list_remote_build_jobs.return_value = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_1", "job_status": "DONE"}
            ],
            "next_page_token": None,
        }

        result = runner.invoke(
            ["spcs", "service", "remote-build-history", "--start-token", "abc123token"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        mock_rb_manager.list_remote_build_jobs.assert_called_once_with(
            limit=50, page_token="abc123token"
        )

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_empty(self, mock_rb_manager_class, runner):
        """No jobs → friendly message, not an error."""
        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.list_remote_build_jobs.return_value = {"jobs": []}

        result = runner.invoke(
            ["spcs", "service", "remote-build-history"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert "No remote build jobs found" in result.output

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_custom_page_size(self, mock_rb_manager_class, runner):
        """--page-size is forwarded to every page request."""
        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager
        mock_rb_manager.list_remote_build_jobs.return_value = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_1", "job_status": "DONE"}
            ],
            "next_page_token": None,
        }

        result = runner.invoke(
            ["spcs", "service", "remote-build-history", "--page-size", "25"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        mock_rb_manager.list_remote_build_jobs.assert_called_once_with(
            limit=25, page_token=None
        )

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_stops_when_token_older_than_30_days(
        self, mock_rb_manager_class, runner
    ):
        """Pagination stops when the next token's timestamp is past the 30-day cutoff."""
        import base64
        import time as time_mod

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager

        # Encode a token that points to 31 days ago.
        old_ms = int(time_mod.time() * 1000) - (31 * 24 * 60 * 60 * 1000)
        old_token = base64.urlsafe_b64encode(str(old_ms).encode()).rstrip(b"=").decode()

        mock_rb_manager.list_remote_build_jobs.return_value = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_1", "job_status": "DONE"}
            ],
            "next_page_token": old_token,
        }

        result = runner.invoke(
            ["spcs", "service", "remote-build-history"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        # Should have stopped after the first page; the stale token must not trigger a second call.
        assert mock_rb_manager.list_remote_build_jobs.call_count == 1
        assert "JOB_1" in result.output

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_continues_when_token_within_30_days(
        self, mock_rb_manager_class, runner
    ):
        """Pagination continues when the next token's timestamp is still within the 30-day window."""
        import base64
        import time as time_mod

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager

        # Encode a token that points to 15 days ago (still within the window).
        recent_ms = int(time_mod.time() * 1000) - (15 * 24 * 60 * 60 * 1000)
        recent_token = (
            base64.urlsafe_b64encode(str(recent_ms).encode()).rstrip(b"=").decode()
        )

        page1 = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_1", "job_status": "DONE"}
            ],
            "next_page_token": recent_token,
        }
        page2 = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_2", "job_status": "FAILED"}
            ],
            "next_page_token": None,
        }
        mock_rb_manager.list_remote_build_jobs.side_effect = [page1, page2]

        result = runner.invoke(
            ["spcs", "service", "remote-build-history"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert mock_rb_manager.list_remote_build_jobs.call_count == 2
        assert "JOB_1" in result.output
        assert "JOB_2" in result.output

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_stops_on_undecipherable_token(self, mock_rb_manager_class, runner):
        """Fix #1: pagination stops when the server returns an opaque/undecodable token."""
        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager

        mock_rb_manager.list_remote_build_jobs.return_value = {
            "jobs": [
                {**_SAMPLE_JOB, "job_name": "MYDB.PUBLIC.JOB_1", "job_status": "DONE"}
            ],
            "next_page_token": "!!!not-base64!!!",
        }

        result = runner.invoke(
            ["spcs", "service", "remote-build-history"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        # Must stop after the first page — no second call with the bad token.
        assert mock_rb_manager.list_remote_build_jobs.call_count == 1
        assert "unrecognised page token" in result.output

    @patch(_COMMANDS_REMOTE_BUILD_MANAGER)
    def test_history_stops_at_max_page_cap(self, mock_rb_manager_class, runner):
        """Fix #1: pagination stops at _REMOTE_BUILD_HISTORY_MAX_PAGES even if the
        server keeps returning tokens.
        """
        import base64
        import time as time_mod

        mock_rb_manager = Mock()
        mock_rb_manager_class.return_value = mock_rb_manager

        # Each call returns a fresh within-window token so the cutoff guard never fires.
        def _fresh_page(*args, **kwargs):
            recent_ms = int(time_mod.time() * 1000) - (
                1 * 24 * 60 * 60 * 1000
            )  # 1 day ago
            token = (
                base64.urlsafe_b64encode(str(recent_ms).encode()).rstrip(b"=").decode()
            )
            return {
                "jobs": [
                    {
                        **_SAMPLE_JOB,
                        "job_name": "MYDB.PUBLIC.JOB_1",
                        "job_status": "DONE",
                    }
                ],
                "next_page_token": token,
            }

        mock_rb_manager.list_remote_build_jobs.side_effect = _fresh_page

        result = runner.invoke(
            ["spcs", "service", "remote-build-history"],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output
        assert mock_rb_manager.list_remote_build_jobs.call_count == 1000
        assert "Stopped after 1000 pages" in result.output
        # The next page token must be printed so the run can be resumed with --start-token.
        assert "--start-token" in result.output
