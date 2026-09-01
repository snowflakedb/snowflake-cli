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

"""Actionable error messages for the ``snow app deploy`` upload phase.

The upload phase talks to Snowflake through stage/workspace DDL and ``PUT``, so
nearly every failure arrives as a connector exception that says what the server
refused but not what the user should do about it. :func:`classify_upload_error`
turns one of those exceptions into a message that names the target, the step
that failed, and a remediation.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from snowflake.cli.api.errno import (
    DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
    INSUFFICIENT_PRIVILEGES,
    OBJECT_ALREADY_EXISTS_NO_PRIVILEGES,
    SQL_COMPILATION_ERROR,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal

# Error codes only the upload phase needs, so they live here rather than in
# api/errno.py. Move them there if another plugin comes to need them.
UNSUPPORTED_FEATURE = 60119
# Returned when the server rejects a write to a workspace. The message text
# carries the reason, so treat this as a generic workspace write failure.
WORKSPACE_WRITE_REJECTED = 99108
# Raised by the Python connector itself rather than the server.
FILE_TRANSFER_FAILED = 253003


class UploadPhase(Enum):
    """An upload sub-phase, named after its telemetry span."""

    PREPARE_STAGE = "prepare_stage"
    PREPARE_WORKSPACE = "prepare_workspace"
    PUSH_STAGE_FILES = "push_stage_files"
    PUSH_WORKSPACE_FILES = "push_workspace_files"

    @property
    def is_push(self) -> bool:
        """True when this phase transfers files rather than preparing storage."""
        return self in (
            UploadPhase.PUSH_STAGE_FILES,
            UploadPhase.PUSH_WORKSPACE_FILES,
        )

    @property
    def target_kind(self) -> str:
        """The code-storage backend this phase writes to."""
        return (
            "workspace"
            if self in (UploadPhase.PREPARE_WORKSPACE, UploadPhase.PUSH_WORKSPACE_FILES)
            else "stage"
        )


class UploadError(CliError):
    """An upload failure that keeps the originating Snowflake error code.

    :class:`~snowflake.cli.api.metrics.CLIMetricsSpan` records ``error.errno``
    on the span, so wrapping a connector exception in a plain ``CliError``
    would report the failure to telemetry without its error code.
    """

    def __init__(self, message: str, *, errno: Optional[int] = None):
        super().__init__(message)
        self.errno = errno


def classify_upload_error(
    exc: BaseException,
    *,
    phase: UploadPhase,
    target: str,
    action: Optional[str] = None,
    required_privilege: Optional[str] = None,
    role: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    encryption_type: Optional[str] = None,
    source_file: Optional[str] = None,
    files_uploaded: Optional[int] = None,
) -> UploadError:
    """Describe an upload failure in terms the user can act on.

    *target* is the stage or workspace being written to, and *action* is the
    step that failed (``"create the stage"``). For a push phase, *source_file*
    and *files_uploaded* say which file failed and how far the upload got.
    *required_privilege*, *database*, *schema* and *encryption_type* refine the
    remediation for the errors where they are relevant; each is optional and
    the message falls back to a less specific phrasing without it.
    """
    code = _error_code(exc)
    detail = (sanitize_for_terminal(str(exc)) or "").strip().rstrip(".")
    prefix = _describe_failure(
        phase=phase,
        target=target,
        action=action,
        source_file=source_file,
        files_uploaded=files_uploaded,
    )
    remediation = _remediation(
        exc,
        code,
        phase=phase,
        target=target,
        role=role,
        required_privilege=required_privilege,
        database=database,
        schema=schema,
        encryption_type=encryption_type,
    )
    message = (
        f"{prefix}: {detail}. {remediation}" if detail else f"{prefix}. {remediation}"
    )
    return UploadError(message, errno=code)


def _error_code(exc: BaseException) -> Optional[int]:
    """Return the Snowflake error code of *exc*, or ``None`` if it has none.

    The connector defaults ``errno`` to ``-1`` when the server did not supply
    one, so that placeholder is normalized away rather than reported to
    telemetry as a real code.
    """
    code = getattr(exc, "errno", None)
    if not isinstance(code, int) or code < 0:
        return None
    return code


def _describe_failure(
    *,
    phase: UploadPhase,
    target: str,
    action: Optional[str],
    source_file: Optional[str],
    files_uploaded: Optional[int],
) -> str:
    """Build the leading clause naming what failed and how far it got."""
    safe_target = sanitize_for_terminal(target)
    if not phase.is_push:
        step = action or f"prepare the {phase.target_kind}"
        return f"Failed to {step} '{safe_target}'"

    if source_file:
        prefix = (
            f"Failed to upload '{sanitize_for_terminal(source_file)}' to {safe_target}"
        )
    else:
        prefix = f"Failed to upload files to {safe_target}"
    if files_uploaded:
        uploaded = "1 file" if files_uploaded == 1 else f"{files_uploaded} files"
        prefix = f"{prefix} after {uploaded} had already uploaded"
    return prefix


def _remediation(
    exc: BaseException,
    code: Optional[int],
    *,
    phase: UploadPhase,
    target: str,
    role: Optional[str],
    required_privilege: Optional[str],
    database: Optional[str],
    schema: Optional[str],
    encryption_type: Optional[str],
) -> str:
    """Return the "how to fix it" half of the message for *exc*."""
    kind = phase.target_kind
    role_clause = f"role '{sanitize_for_terminal(role)}'" if role else "your role"

    if isinstance(exc, FileNotFoundError):
        return (
            "The file was removed from the bundle before it could be uploaded. "
            "Re-run the deploy, and avoid changing the project while it runs."
        )

    if code is None and isinstance(exc, OSError):
        return (
            "The local file could not be read. Check the permissions and free "
            "space on the bundle directory, then re-run the deploy."
        )

    if code == INSUFFICIENT_PRIVILEGES:
        needed = required_privilege or (
            f"WRITE on the {kind}" if phase.is_push else "the privileges it requires"
        )
        return (
            f"Verify that {role_clause} has {needed}, plus USAGE on the "
            "database and schema."
        )

    if code in (
        DOES_NOT_EXIST_OR_NOT_AUTHORIZED,
        DOES_NOT_EXIST_OR_CANNOT_BE_PERFORMED,
    ):
        if phase.is_push:
            return (
                f"The {kind} existed when the upload started and is gone now. "
                "This usually means another deploy of the same app recreated "
                "it at the same time. Check for concurrent deploys, then "
                "re-run this one."
            )
        if database and schema:
            return (
                f"Database '{sanitize_for_terminal(database)}' or schema "
                f"'{sanitize_for_terminal(schema)}' does not exist, or "
                f"{role_clause} cannot see it. Create the schema, or correct "
                "the database and schema in your project configuration."
            )
        return (
            f"The {kind}, or the schema holding it, does not exist or "
            f"{role_clause} cannot see it. Create it, or correct the database "
            "and schema in your project configuration."
        )

    if code == OBJECT_ALREADY_EXISTS_NO_PRIVILEGES:
        return (
            f"'{sanitize_for_terminal(target)}' already exists and "
            f"{role_clause} cannot use it. Drop it, or point code storage at a "
            "name your role owns."
        )

    if code == UNSUPPORTED_FEATURE:
        if encryption_type:
            return (
                "This Snowflake deployment does not support a stage with "
                f"ENCRYPTION = (TYPE = '{sanitize_for_terminal(encryption_type)}'). "
                "Set a different encryption_type on the code stage, or use a "
                "workspace for code storage."
            )
        return (
            "This Snowflake deployment does not support the requested "
            "feature. Contact your account administrator to find out whether "
            "it can be enabled."
        )

    if code == SQL_COMPILATION_ERROR:
        return (
            f"Snowflake could not compile the statement. Check that the {kind} "
            "name in your project configuration is a valid identifier."
        )

    if code == FILE_TRANSFER_FAILED:
        return (
            "The transfer to cloud storage failed. This is usually a transient "
            "network problem, so re-running the deploy often succeeds."
        )

    if code == WORKSPACE_WRITE_REJECTED:
        return (
            "Snowflake rejected the write to the workspace. Re-run the deploy; "
            "if it keeps failing, set code_stage to use a stage for code "
            "storage instead."
        )

    # Unrecognized code. Preparing storage fails on privileges far more often
    # than anything else, so name the grant this step needs when the caller
    # knows it — it is the most useful thing to check first.
    if not phase.is_push and required_privilege:
        return (
            f"Check that {role_clause} has {required_privilege}, then re-run "
            "with --debug for the full traceback."
        )
    return (
        f"The {phase.value.replace('_', ' ')} step of the upload failed. "
        "Re-run with --debug for the full traceback."
    )
