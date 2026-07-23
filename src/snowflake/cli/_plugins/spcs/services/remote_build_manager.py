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

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, NoReturn, Optional
from urllib.parse import quote, urlencode

from snowflake.cli.api.constants import SF_REST_API_URL_PREFIX
from snowflake.cli.api.exceptions import CliConnectionError, CliError
from snowflake.cli.api.rest_api import RestApi
from snowflake.cli.api.sql_execution import SqlExecutionMixin

_REMOTE_BUILD_EXECUTE_URL = f"{SF_REST_API_URL_PREFIX}/remote-build/execute"
_REMOTE_BUILD_JOBS_URL = f"{SF_REST_API_URL_PREFIX}/remote-build/jobs"


class RemoteBuildStatus(str, Enum):
    """Known ``job_status`` values returned by the remote-build REST API.

    The server may also return additional transient values; those are not members of
    this enum. ``_missing_`` maps any unmodeled value to :attr:`UNKNOWN` so
    ``RemoteBuildStatus(raw)`` never raises. Callers that only need to know whether a
    status is terminal should use :meth:`is_terminal` (or
    :attr:`RemoteBuildJobStatus.is_terminal`) rather than enumerating members themselves.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def _missing_(cls, value):  # noqa: ARG003
        # Unmodeled server statuses (and empty/None-ish values) fall back to UNKNOWN
        # so callers can always do RemoteBuildStatus(raw).is_terminal without a try/except.
        return cls.UNKNOWN

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_STATUSES


# Single source of truth for which job_status values are terminal. Callers outside this
# module should drive terminal checks off RemoteBuildStatus.is_terminal /
# RemoteBuildJobStatus.is_terminal rather than duplicating this set.
_TERMINAL_STATUSES = frozenset(
    {
        RemoteBuildStatus.DONE,
        RemoteBuildStatus.FAILED,
        RemoteBuildStatus.CANCELLED,
    }
)


class RemoteBuildPermanentError(CliError):
    """A definitive, non-retryable remote-build REST failure.

    Raised for errors the server has unambiguously classified as permanent — bad
    request, auth failure, not found, method not allowed, or a missing feature
    flag — as opposed to network blips or unclassified/5xx errors (plain
    ``CliError`` / ``CliConnectionError``) that may be worth retrying. Callers that
    poll ``get_remote_builder`` in a loop (e.g. ``remote_build``'s wait loop) should
    fail fast on this rather than retrying for the length of their wait budget.
    """


@dataclass
class RemoteBuildJobStatus:
    """Mirrors the ``RemoteBuildJob`` schema returned by the GS REST API: ``job_name``,
    ``job_status``, ``creation_time`` and ``end_time`` (both ISO-8601 strings, ``end_time``
    ``None`` until the job reaches a terminal state) are the only fields the server sends.
    """

    job_name: str
    job_status: str
    creation_time: Optional[str]
    end_time: Optional[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RemoteBuildJobStatus":
        return cls(
            job_name=data.get("job_name", ""),
            job_status=data.get("job_status", RemoteBuildStatus.UNKNOWN.value),
            creation_time=data.get("creation_time"),
            end_time=data.get("end_time"),
        )

    @property
    def is_terminal(self) -> bool:
        return RemoteBuildStatus(self.job_status).is_terminal


class RemoteBuildManager(SqlExecutionMixin):
    """
    Manager that calls the GS REST API endpoints for remote image/app builds.

    POST /api/v2/remote-build/execute       — createRemoteBuilder
    GET  /api/v2/remote-build/jobs/{name}   — getRemoteBuilderJob (single job)
    GET  /api/v2/remote-build/jobs          — getRemoteBuilder (paginated list)
    """

    def create_remote_builder(
        self,
        build_source: str,
        location: Optional[str] = None,
        name: Optional[str] = None,
        image_tag: Optional[str] = None,
        project_type: Optional[str] = None,
        compute_pool: Optional[str] = None,
        build_type: str = "image",
    ) -> str:
        """
        Submit a remote build via POST /api/v2/remote-build/execute.

        Args:
            build_source: Stage path containing the build context, e.g.
                          ``@db.schema.stage/build_contexts/my_job``.
            location: For image builds — IMAGE REPOSITORY in [db.][schema.]repo format (optional,
                      account default used when omitted). For app builds — ARTIFACT REPOSITORY in
                      db.schema.repo format (required).
            name: For image builds — short image name without a tag. For app builds — artifact
                  package name. Auto-generated by the server when omitted.
            image_tag: Tag for the built image. Applies to image builds only; defaults to
                       ``"latest"`` when omitted.
            project_type: Project type hint for app builds (e.g. ``"node"``, ``"python"``).
            compute_pool: Compute pool to run the build job on.
            build_type: ``"image"`` (default) or ``"app"``.

        Returns:
            The ``job_name`` assigned to this build by the server.
        """
        body: Dict[str, Any] = {
            "build_type": build_type,
            "build_source": build_source,
        }

        if location:
            body["location"] = location
        if name:
            body["name"] = name
        if image_tag:
            body["image_tag"] = image_tag
        if project_type:
            body["project_type"] = project_type
        if compute_pool:
            body["compute_pool"] = compute_pool

        rest = RestApi(self._conn)
        try:
            response = rest.send_rest_request(
                url=_REMOTE_BUILD_EXECUTE_URL,
                method="post",
                data=body,
            )
        except Exception as err:
            _handle_remote_build_error(err, "submit build")

        returned_job = (response or {}).get("job_name")
        if not returned_job:
            raise CliError(
                f"Remote build API did not return a job_name. Response: {response}"
            )
        return returned_job

    def get_remote_builder(self, job_name: str) -> Optional[RemoteBuildJobStatus]:
        """
        Look up a single remote build job via GET /api/v2/remote-build/jobs/{job_name}.

        The server performs a two-phase lookup: live service store first, then up to 30 days of
        job history for completed jobs.

        Returns:
            A :class:`RemoteBuildJobStatus` when found (live or historical), ``None`` when the
            server confirms the job does not exist (404).

        Raises:
            CliError: for any error other than 404 (auth failures, server errors, etc.).
            CliConnectionError: for network-level failures (connection/timeout/DNS).
        """
        from snowflake.cli.api.connector_errors import get_http_status_code

        rest = RestApi(self._conn)
        # job_name is a path segment (not a query param), so it needs URL path quoting rather
        # than urlencode — e.g. quoted identifiers may contain spaces or other reserved chars.
        url = f"{_REMOTE_BUILD_JOBS_URL}/{quote(job_name, safe='')}"
        try:
            response = rest.send_rest_request(url=url, method="get")
        except Exception as err:
            # 404 means the server found no job with this name — return None so callers
            # can distinguish "not found" from real errors without catching CliError.
            if get_http_status_code(err) == 404:
                return None
            _handle_remote_build_error(err, f"get job '{job_name}'")

        if not response:
            return None
        return RemoteBuildJobStatus.from_dict(response)

    def list_remote_build_jobs(
        self,
        limit: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List recent remote build jobs via GET /api/v2/remote-build/jobs.

        Args:
            limit: Maximum number of results to return.
            page_token: Opaque pagination token from a previous response.

        Returns:
            Raw API response dict with ``jobs`` list and optional ``next_page_token``.
        """
        params: Dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if page_token:
            params["page_token"] = page_token

        url = _REMOTE_BUILD_JOBS_URL
        if params:
            url = f"{url}?{urlencode(params)}"

        rest = RestApi(self._conn)
        try:
            response = rest.send_rest_request(url=url, method="get")
        except Exception as err:
            _handle_remote_build_error(err, "list jobs")

        return response or {"jobs": []}


def _handle_remote_build_error(err: Exception, operation: str) -> NoReturn:
    """Convert connector HTTP errors into user-friendly CliError subclasses."""
    from snowflake.cli.api.connector_errors import get_http_status_code
    from snowflake.connector.errors import BadRequest

    if isinstance(err, BadRequest):
        raise RemoteBuildPermanentError(
            f"Remote build API returned 400 Bad Request while trying to {operation}. "
            "Check that all required fields (build_source, image-repository, etc.) are correct."
        )

    # Check the raw error message for a server-side feature-not-enabled signal.
    # GS's ensureFeatureEnabled() now checks the flag that matches the build type:
    #   image builds  → ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS
    #   app builds    → ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS
    # Both paths also require ENABLE_SNOW_API_FOR_REMOTE_BUILD to reach the REST handler.
    err_str = str(err)
    if "SNOWSERVICES_NOT_ENABLED" in err_str or (
        "not enabled" in err_str.lower() and "feature" in err_str.lower()
    ):
        raise RemoteBuildPermanentError(
            f"Remote build API error while trying to {operation}: the account is missing a "
            "required feature flag.\n"
            "Ensure the following account parameters are set to 'enable':\n"
            "  - ENABLE_SNOW_API_FOR_REMOTE_BUILD  (required for all build types)\n"
            "  - ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS  (required for image builds)\n"
            "  - ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS  (required for app/tarball builds)"
        )

    code = get_http_status_code(err)
    if code is not None:
        match code:
            case 401:
                raise RemoteBuildPermanentError(
                    "Remote build API returned 401 Unauthorized. Check your credentials."
                )
            case 403:
                raise RemoteBuildPermanentError(
                    f"Remote build API returned 403 Forbidden while trying to {operation}. "
                    "Possible causes:\n"
                    "  - You lack the required privilege on the build service.\n"
                    "  - ENABLE_SNOW_API_FOR_REMOTE_BUILD is not set to 'enable'.\n"
                    "  - The build-type-specific flag is not set to 'enable':\n"
                    "      image builds → ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS\n"
                    "      app builds   → ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS"
                )
            case 404:
                raise RemoteBuildPermanentError(
                    f"Remote build API returned 404 Not Found while trying to {operation}."
                )
            case 405:
                raise RemoteBuildPermanentError(
                    "Remote build API returned 405 Method Not Allowed. "
                    "Ensure ENABLE_SNOW_API_FOR_REMOTE_BUILD is set to 'enable' and that the "
                    "build-type-specific flag is also enabled:\n"
                    "  image builds → ENABLE_SPCS_RUNTIME_IMAGE_BUILDER_FUNCTIONS\n"
                    "  app builds   → ENABLE_SPCS_RUNTIME_APP_BUILDER_FUNCTIONS"
                )
            case _:
                # Unclassified HTTP status (e.g. 429/5xx) — may well be transient
                # (rate limiting, momentary server trouble), so leave this as a plain
                # CliError rather than RemoteBuildPermanentError.
                raise CliError(
                    f"Remote build API returned HTTP {code} while trying to {operation}: {err}"
                )
    # No HTTP status at all — a network-level failure (connection error, timeout, DNS,
    # etc.) rather than a server response. CliConnectionError so retry loops treat it
    # as transient and exit codes reflect a connection problem.
    raise CliConnectionError(
        f"Remote build API error while trying to {operation}: {err}"
    )
