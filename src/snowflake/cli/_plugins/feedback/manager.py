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

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Optional

from click.exceptions import ClickException
from snowflake.cli._plugins.feedback.constants import (
    ISSUES_URL,
    PRIVACY_NOTICE,
    STATUS_CONFIRMATION_REQUIRED,
    STATUS_NO_CONNECTION,
    STATUS_RATE_LIMITED,
    STATUS_REJECTED,
    STATUS_SEND_FAILED,
    STATUS_SUBMITTED,
)
from snowflake.cli._plugins.feedback.last_error import (
    clear_last_cli_error,
    read_last_cli_error,
)
from snowflake.cli._plugins.feedback.rate_limit import check, record
from snowflake.cli._plugins.feedback.redact import prepare_message
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.exceptions import (
    MissingConfigurationError,
    SnowflakeConnectionError,
)

log = logging.getLogger(__name__)

# ClickException messages that mean the user has nothing to connect with.
# Auth and network failures are SnowflakeConnectionError and are not in this list.
_MISSING_CONNECTION_MARKERS = (
    "is not configured",
    "connection name or temporary connection is required",
)


@dataclass
class FeedbackOutcome:
    status: str
    exit_code: int = 0
    feedback_id: Optional[str] = None
    category: Optional[str] = None
    rating: Optional[int] = None
    truncated: Optional[bool] = None
    retry_after_seconds: Optional[int] = None
    issue_url: Optional[str] = None
    reason: Optional[str] = None

    def payload(self) -> dict:
        data = {
            "status": self.status,
            "privacy_notice": PRIVACY_NOTICE,
        }
        if self.feedback_id is not None:
            data["feedback_id"] = self.feedback_id
        if self.category is not None:
            data["category"] = self.category
        if self.rating is not None:
            data["rating"] = self.rating
        if self.truncated is not None:
            data["truncated"] = self.truncated
        if self.retry_after_seconds is not None:
            data["retry_after_seconds"] = self.retry_after_seconds
        if self.issue_url is not None:
            data["issue_url"] = self.issue_url
        if self.reason is not None:
            data["reason"] = self.reason
        return data

    def message(self) -> str:
        notice = PRIVACY_NOTICE
        if self.status == STATUS_SUBMITTED:
            return f"{notice}\nFeedback submitted ({self.feedback_id})."
        if self.status == STATUS_CONFIRMATION_REQUIRED:
            return f"{notice}\nPass --yes to acknowledge the storage warning."
        if self.status == STATUS_RATE_LIMITED:
            return (
                f"{notice}\nRate limit exceeded. Try again in "
                f"{self.retry_after_seconds} seconds."
            )
        if self.status == STATUS_NO_CONNECTION:
            return (
                f"{notice}\nA Snowflake connection is required to send feedback. "
                f"Open an issue at {self.issue_url}"
            )
        if self.status == STATUS_SEND_FAILED:
            detail = self.reason if self.reason else "Failed to send feedback."
            return f"{notice}\n{detail}"
        if self.status == STATUS_REJECTED:
            return f"{notice}\n{self.reason}"
        return notice


def submit_feedback(
    rating: int,
    category: str,
    message: Optional[str] = None,
    attach_last_error: bool = False,
) -> FeedbackOutcome:
    from snowflake.cli._app.telemetry import is_coding_agent, send_cli_user_feedback

    if message is None:
        if is_coding_agent():
            return FeedbackOutcome(
                status=STATUS_REJECTED,
                exit_code=1,
                reason="Message is required when running as an AI agent.",
            )
        redacted: Optional[str] = None
        truncated = False
    else:
        if not message.strip():
            return FeedbackOutcome(
                status=STATUS_REJECTED,
                exit_code=1,
                reason="Message is empty.",
            )
        redacted, truncated = prepare_message(message)
        if not redacted.strip():
            return FeedbackOutcome(
                status=STATUS_REJECTED,
                exit_code=1,
                reason="Message is empty.",
            )

    last_error_id: Optional[str] = None
    last_error_type: Optional[str] = None
    attached_last_error = False
    if attach_last_error:
        recorded = read_last_cli_error()
        if recorded is not None:
            last_error_id = recorded["id"]
            last_error_type = recorded["error_type"]
            attached_last_error = True

    retry_after = check()
    if retry_after is not None:
        return FeedbackOutcome(
            status=STATUS_RATE_LIMITED,
            exit_code=1,
            retry_after_seconds=retry_after,
        )

    try:
        connection = get_cli_context().connection
    except ClickException as err:
        log.debug("Feedback could not open a connection", exc_info=True)
        return _outcome_for_connection_error(err)
    if connection is None or getattr(connection, "_telemetry", None) is None:
        return FeedbackOutcome(
            status=STATUS_NO_CONNECTION,
            exit_code=1,
            issue_url=ISSUES_URL,
        )

    feedback_id = str(uuid.uuid4())
    payload = {
        "feedback_id": feedback_id,
        "category": category,
        "rating": rating,
        "truncated": truncated,
    }
    if redacted is not None:
        payload["message"] = redacted
    if last_error_id is not None and last_error_type is not None:
        payload["last_error_id"] = last_error_id
        payload["last_error_type"] = last_error_type
    try:
        send_cli_user_feedback(payload)
    except Exception:
        log.debug("Feedback telemetry send failed", exc_info=True)
        return FeedbackOutcome(
            status=STATUS_SEND_FAILED,
            exit_code=1,
        )

    if attached_last_error:
        clear_last_cli_error()
    record()
    return FeedbackOutcome(
        status=STATUS_SUBMITTED,
        feedback_id=feedback_id,
        category=category,
        rating=rating,
        truncated=truncated,
    )


def _outcome_for_connection_error(err: ClickException) -> FeedbackOutcome:
    """Issues link only when nothing is configured. Otherwise show the error."""
    if isinstance(err, SnowflakeConnectionError):
        return FeedbackOutcome(
            status=STATUS_SEND_FAILED,
            exit_code=1,
            reason=str(err),
        )
    text = str(err)
    lowered = text.lower()
    if isinstance(err, MissingConfigurationError) or any(
        marker in lowered for marker in _MISSING_CONNECTION_MARKERS
    ):
        return FeedbackOutcome(
            status=STATUS_NO_CONNECTION,
            exit_code=1,
            issue_url=ISSUES_URL,
        )
    return FeedbackOutcome(
        status=STATUS_SEND_FAILED,
        exit_code=1,
        reason=text,
    )
