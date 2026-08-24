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

"""Unit tests for the upload-phase error classifier."""

import pytest
from snowflake.cli._plugins.apps.upload_errors import (
    UploadError,
    UploadPhase,
    classify_upload_error,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.metrics import CLIMetricsSpan
from snowflake.connector.errors import OperationalError, ProgrammingError

STAGE = "TEST_DB.TEST_SCHEMA.MY_APP_CODE"
WORKSPACE = "snow://workspace/TEST_DB.TEST_SCHEMA.SNOWFLAKE_APPS/versions/live/MY_APP"


def _sql_error(errno: int, msg: str = "server said no") -> ProgrammingError:
    error = ProgrammingError(msg)
    error.errno = errno
    return error


class TestUploadPhase:
    @pytest.mark.parametrize(
        "phase, is_push, target_kind",
        [
            (UploadPhase.PREPARE_STAGE, False, "stage"),
            (UploadPhase.PREPARE_WORKSPACE, False, "workspace"),
            (UploadPhase.PUSH_STAGE_FILES, True, "stage"),
            (UploadPhase.PUSH_WORKSPACE_FILES, True, "workspace"),
        ],
    )
    def test_phase_properties(self, phase, is_push, target_kind):
        assert phase.is_push is is_push
        assert phase.target_kind == target_kind

    def test_values_match_span_suffixes(self):
        """The enum values are the telemetry span names, minus the prefix."""
        assert {p.value for p in UploadPhase} == {
            "prepare_stage",
            "prepare_workspace",
            "push_stage_files",
            "push_workspace_files",
        }


class TestErrorCodePreservation:
    def test_errno_is_kept_for_telemetry(self):
        """A wrapped error must still report its code on the span, which
        ``CLIMetricsSpan`` reads from ``error.errno``."""
        error = classify_upload_error(
            _sql_error(3001),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
        )
        assert error.errno == 3001

        span = CLIMetricsSpan(name="snowflake_app.upload.prepare_stage", start_time=0.0)
        span.finish(error=error)
        assert span.to_dict()[CLIMetricsSpan.ERROR_CODE_KEY] == 3001

    def test_is_a_cli_error(self):
        """Callers raise the result directly, so it must exit like a CliError."""
        error = classify_upload_error(
            _sql_error(3001), phase=UploadPhase.PREPARE_STAGE, target=STAGE
        )
        assert isinstance(error, CliError)
        assert isinstance(error, UploadError)
        assert error.exit_code == 1

    def test_errno_is_none_for_non_sql_errors(self):
        error = classify_upload_error(
            OSError("permission denied"),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
        )
        assert error.errno is None

    def test_the_connector_placeholder_code_is_not_reported(self):
        """The connector defaults ``errno`` to -1 when the server sent none."""
        error = classify_upload_error(
            ProgrammingError("Insufficient privileges"),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
        )
        assert ProgrammingError("boom").errno == -1
        assert error.errno is None


class TestPrepareMessages:
    def test_insufficient_privileges_names_the_required_grant(self):
        error = classify_upload_error(
            _sql_error(3001, "Insufficient privileges"),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
            action="create the stage",
            required_privilege="CREATE STAGE on the schema",
            role="APP_DEPLOYER",
        )
        assert f"Failed to create the stage '{STAGE}'" in error.message
        assert "Insufficient privileges" in error.message
        assert "role 'APP_DEPLOYER' has CREATE STAGE on the schema" in error.message
        assert "USAGE on the database and schema" in error.message

    def test_insufficient_privileges_without_a_resolved_role(self):
        error = classify_upload_error(
            _sql_error(3001),
            phase=UploadPhase.PREPARE_WORKSPACE,
            target="TEST_DB.TEST_SCHEMA.SNOWFLAKE_APPS",
            action="create the workspace",
            required_privilege="CREATE WORKSPACE on the schema",
        )
        assert "your role has CREATE WORKSPACE on the schema" in error.message

    @pytest.mark.parametrize("errno", [2003, 2043])
    def test_missing_object_points_at_the_database_and_schema(self, errno):
        """Group 2: these codes almost always mean the database or schema is
        missing, so the message must not send the user hunting for a grant."""
        error = classify_upload_error(
            _sql_error(
                errno, "Object does not exist, or operation cannot be performed"
            ),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
            action="create the stage",
            role="APP_DEPLOYER",
            database="TEST_DB",
            schema="TEST_SCHEMA",
        )
        assert "Database 'TEST_DB' or schema 'TEST_SCHEMA' does not exist" in (
            error.message
        )
        assert "Create the schema" in error.message
        assert "OWNERSHIP" not in error.message

    @pytest.mark.parametrize("errno", [2003, 2043])
    def test_missing_object_without_a_resolved_schema(self, errno):
        error = classify_upload_error(
            _sql_error(errno),
            phase=UploadPhase.PREPARE_WORKSPACE,
            target="TEST_DB.TEST_SCHEMA.SNOWFLAKE_APPS",
        )
        assert "The workspace, or the schema holding it, does not exist" in (
            error.message
        )

    def test_object_already_exists(self):
        error = classify_upload_error(
            _sql_error(3041, "Object already exists"),
            phase=UploadPhase.PREPARE_WORKSPACE,
            target="TEST_DB.TEST_SCHEMA.SNOWFLAKE_APPS",
            role="APP_DEPLOYER",
        )
        assert "'TEST_DB.TEST_SCHEMA.SNOWFLAKE_APPS' already exists" in error.message
        assert "role 'APP_DEPLOYER' cannot use it" in error.message

    def test_unsupported_feature_names_the_encryption_type(self):
        """Group 7: the likely trigger is the requested stage encryption, so
        the message says which type was asked for."""
        error = classify_upload_error(
            _sql_error(60119, "Unsupported feature"),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
            action="create the stage",
            encryption_type="SNOWFLAKE_SSE",
        )
        assert "ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')" in error.message
        assert "Set a different encryption_type" in error.message

    def test_unsupported_feature_without_an_encryption_type(self):
        error = classify_upload_error(
            _sql_error(60119),
            phase=UploadPhase.PREPARE_WORKSPACE,
            target="TEST_DB.TEST_SCHEMA.SNOWFLAKE_APPS",
        )
        assert "does not support the requested feature" in error.message

    def test_default_action_when_the_caller_supplies_none(self):
        error = classify_upload_error(
            _sql_error(3001),
            phase=UploadPhase.PREPARE_WORKSPACE,
            target="TEST_DB.TEST_SCHEMA.SNOWFLAKE_APPS",
        )
        assert "Failed to prepare the workspace" in error.message


class TestPushMessages:
    def test_names_the_file_the_target_and_the_progress(self):
        error = classify_upload_error(
            _sql_error(1003, "SQL compilation error"),
            phase=UploadPhase.PUSH_STAGE_FILES,
            target=f"@{STAGE}",
            source_file="src/app.py",
            files_uploaded=12,
        )
        assert f"Failed to upload 'src/app.py' to @{STAGE}" in error.message
        assert "after 12 files had already uploaded" in error.message
        assert "could not compile the statement" in error.message

    def test_singular_file_count(self):
        error = classify_upload_error(
            _sql_error(1003),
            phase=UploadPhase.PUSH_STAGE_FILES,
            target=f"@{STAGE}",
            source_file="src/app.py",
            files_uploaded=1,
        )
        assert "after 1 file had already uploaded" in error.message

    def test_no_progress_clause_before_the_first_file_lands(self):
        error = classify_upload_error(
            _sql_error(1003),
            phase=UploadPhase.PUSH_STAGE_FILES,
            target=f"@{STAGE}",
            source_file="src/app.py",
            files_uploaded=0,
        )
        assert "had already uploaded" not in error.message

    def test_no_file_clause_when_the_failing_file_is_unknown(self):
        error = classify_upload_error(
            _sql_error(1003),
            phase=UploadPhase.PUSH_WORKSPACE_FILES,
            target=WORKSPACE,
        )
        assert f"Failed to upload files to {WORKSPACE}" in error.message

    @pytest.mark.parametrize("errno", [2003, 2043])
    def test_target_disappearing_mid_upload_calls_out_concurrent_deploys(self, errno):
        """Group 3: the stage was visible during prepare and gone during push."""
        error = classify_upload_error(
            _sql_error(errno, "Stage does not exist or not authorized"),
            phase=UploadPhase.PUSH_STAGE_FILES,
            target=f"@{STAGE}",
            source_file="src/app.py",
            files_uploaded=8,
        )
        assert "existed when the upload started and is gone now" in error.message
        assert "concurrent deploys" in error.message

    def test_transfer_failure_suggests_re_running(self):
        """Group 4: 253003 is an OperationalError, not a ProgrammingError."""
        exc = OperationalError("Failed to upload file to cloud storage")
        exc.errno = 253003
        error = classify_upload_error(
            exc,
            phase=UploadPhase.PUSH_WORKSPACE_FILES,
            target=WORKSPACE,
            source_file="src/app.py",
        )
        assert error.errno == 253003
        assert "transfer to cloud storage failed" in error.message
        assert "transient network problem" in error.message

    def test_workspace_write_rejected(self):
        error = classify_upload_error(
            _sql_error(99108, "Data exception"),
            phase=UploadPhase.PUSH_WORKSPACE_FILES,
            target=WORKSPACE,
            source_file="src/app.py",
        )
        assert "rejected the write to the workspace" in error.message
        assert "code_stage" in error.message

    def test_privileges_during_push_asks_for_write(self):
        error = classify_upload_error(
            _sql_error(3001, "Insufficient privileges"),
            phase=UploadPhase.PUSH_STAGE_FILES,
            target=f"@{STAGE}",
            role="APP_DEPLOYER",
        )
        assert "role 'APP_DEPLOYER' has WRITE on the stage" in error.message

    def test_vanished_file(self):
        """Group 10: the file was removed between the scan and its PUT."""
        error = classify_upload_error(
            FileNotFoundError("src/app.py"),
            phase=UploadPhase.PUSH_WORKSPACE_FILES,
            target=WORKSPACE,
            source_file="src/app.py",
        )
        assert "Failed to upload 'src/app.py'" in error.message
        assert "removed from the bundle before it could be uploaded" in error.message

    def test_local_os_error(self):
        """Group 6: an OSError with no SQL code is a local filesystem problem."""
        error = classify_upload_error(
            OSError("Permission denied"),
            phase=UploadPhase.PUSH_STAGE_FILES,
            target=f"@{STAGE}",
            source_file="src/app.py",
        )
        assert "The local file could not be read" in error.message


class TestFallback:
    def test_prepare_falls_back_to_the_privilege_this_step_needs(self):
        """Preparing storage fails on privileges far more often than anything
        else, so an unrecognized code still points at the grant."""
        error = classify_upload_error(
            ProgrammingError("Insufficient privileges"),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
            action="drop stage",
            required_privilege="OWNERSHIP on the stage",
        )
        assert f"Failed to drop stage '{STAGE}'" in error.message
        assert "your role has OWNERSHIP on the stage" in error.message

    def test_unknown_code_still_names_the_phase_and_target(self):
        error = classify_upload_error(
            _sql_error(123456, "Something new"),
            phase=UploadPhase.PUSH_WORKSPACE_FILES,
            target=WORKSPACE,
        )
        assert WORKSPACE in error.message
        assert "push workspace files step of the upload failed" in error.message
        assert "--debug" in error.message
        assert error.errno == 123456


class TestMessageShape:
    def test_no_dangling_colon_when_the_error_stringifies_to_nothing(self):
        error = classify_upload_error(
            OSError(),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
            action="create the stage",
        )
        assert f"Failed to create the stage '{STAGE}'." in error.message
        assert ": ." not in error.message

    def test_server_detail_is_not_double_punctuated(self):
        error = classify_upload_error(
            _sql_error(3001, "Insufficient privileges to operate on stage."),
            phase=UploadPhase.PREPARE_STAGE,
            target=STAGE,
            action="create the stage",
        )
        assert "operate on stage. Verify" in error.message


class TestSanitization:
    def test_server_text_and_identifiers_are_stripped_of_escape_codes(self):
        error = classify_upload_error(
            _sql_error(3001, "\x1b[31mInsufficient privileges\x1b[0m"),
            phase=UploadPhase.PREPARE_STAGE,
            target="TEST_DB.TEST_SCHEMA.\x1b[31mMY_STAGE",
            action="create the stage",
            role="\x1b[31mAPP_DEPLOYER",
        )
        assert "\x1b" not in error.message
        assert "Insufficient privileges" in error.message
        assert "role 'APP_DEPLOYER'" in error.message
