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
from datetime import datetime
from typing import List, NamedTuple, Optional, Tuple

from snowflake.cli._plugins.remote.constants import (
    DEFAULT_SERVICE_TIMEOUT_MINUTES,
    SERVER_UI_ENDPOINT_NAME,
    SERVICE_NAME_PREFIX,
    ServiceResult,
    ServiceStatus,
)
from snowflake.cli._plugins.remote.container_spec import generate_service_spec_yaml
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

log = logging.getLogger(__name__)


class ServiceOperationResult(NamedTuple):
    """Result of a service operation containing service details."""

    service_name: Optional[str]
    url: Optional[str]
    status: Optional[str]


class RemoteManager(SqlExecutionMixin):
    """Manager for remote development environments using Snowpark Container Services."""

    def _get_current_snowflake_user(self) -> str:
        """Get the current Snowflake username from the connection."""
        result = self.execute_query("SELECT CURRENT_USER()").fetchone()
        return result[0] if result else "unknown"

    def _get_service_status(self, service_name: str) -> Tuple[bool, Optional[str]]:
        """
        Get service status using DESC SERVICE.

        Returns:
            Tuple of (service_exists, current_status)
        """
        try:
            log.debug("Checking service status for %s using DESC SERVICE", service_name)

            # Use DESC SERVICE to get service information
            # This returns exactly one row with service details, or none if service doesn't exist
            desc_query = f"DESC SERVICE {service_name}"
            cursor = self.execute_query(desc_query, cursor_class=DictCursor)
            result = cursor.fetchone()

            if result:
                # DESC SERVICE returns columns similar to SHOW SERVICES
                # Access status by column name for robustness
                current_status = result.get("status")
                log.debug(
                    "Found service %s with status: %s", service_name, current_status
                )
                return True, current_status
            else:
                log.debug("DESC SERVICE returned no result for %s", service_name)
                return False, None

        except Exception as e:
            log.debug("Error checking service status for %s: %s", service_name, e)
            # Service doesn't exist or other error
            return False, None

    def _resolve_service_name(self, name_input: str) -> str:
        """
        Resolve the service name from user input.

        Accepts either:
        1. Full service name (SNOW_REMOTE_{snowflake_username}_{name})
        2. Customer input name (which gets prefixed)

        Args:
            name_input: Either customer name or full service name

        Returns:
            Full service name (SNOW_REMOTE_{snowflake_username}_{name})
        """
        # If it already starts with SNOW_REMOTE_, use as-is (case-insensitive)
        if name_input.upper().startswith(f"{SERVICE_NAME_PREFIX}_"):
            return name_input

        # Otherwise, treat as customer input name and add prefix with Snowflake username
        snowflake_username = self._get_current_snowflake_user()
        # Convert to uppercase to match Snowflake service naming convention
        return f"{SERVICE_NAME_PREFIX}_{snowflake_username}_{name_input}".upper()

    def _handle_existing_service(
        self, service_name: str, current_status: str
    ) -> Optional[ServiceOperationResult]:
        """
        Handle an existing service based on its current service status.

        Note: current_status is always service status (from DESC SERVICE).

        Returns:
            ServiceOperationResult with service details, or None if service needs recreation
        """
        # Handle service statuses (from DESC SERVICE)
        if current_status == ServiceStatus.RUNNING.value:
            log.debug("Service %s is already running", service_name)
            url = self.get_server_ui_url(service_name)
            return ServiceOperationResult(
                service_name, url, ServiceResult.RUNNING.value
            )
        elif current_status in [
            ServiceStatus.SUSPENDED.value,
            ServiceStatus.SUSPENDING.value,
        ]:
            log.debug(
                "Service %s is suspended/suspending, resuming...",
                service_name,
            )
            service_manager = ServiceManager()
            service_manager.resume(service_name)
            self.wait_for_service_ready(service_name)
            url = self.get_server_ui_url(service_name)
            return ServiceOperationResult(
                service_name, url, ServiceResult.RESUMED.value
            )
        elif current_status == ServiceStatus.PENDING.value:
            log.debug(
                "Service %s is pending, waiting for it to be ready...",
                service_name,
            )
            self.wait_for_service_ready(service_name)
            url = self.get_server_ui_url(service_name)
            return ServiceOperationResult(
                service_name, url, ServiceResult.RUNNING.value
            )
        elif current_status in [
            ServiceStatus.FAILED.value,
            ServiceStatus.INTERNAL_ERROR.value,
            ServiceStatus.DELETING.value,
            ServiceStatus.DELETED.value,
        ]:
            log.debug(
                "Service %s is in failed/error/deleting state (%s), will recreate...",
                service_name,
                current_status,
            )
            return None
        else:
            log.debug(
                "Service %s has unknown status '%s', will recreate...",
                service_name,
                current_status,
            )
            return None

    def _create_new_service(
        self,
        service_name: str,
        compute_pool: Optional[str],
        external_access: Optional[List[str]],
        stage: Optional[str],
        image: Optional[str],
    ) -> ServiceOperationResult:
        """
        Create a new service or recreate a failed one.

        Returns:
            ServiceOperationResult with service details
        """
        # Validate compute pool is provided for service creation
        if not compute_pool:
            raise ValueError("compute_pool is required for creating a new service")

        cc.step(f"Creating remote development environment '{service_name}'...")

        # Generate container service specification as YAML
        spec_content = generate_service_spec_yaml(
            session=self.snowpark_session,
            compute_pool=compute_pool,
            stage=stage,
            image=image,
            ssh_public_key=None,  # SSH key support will be added in later PR
        )

        # Create the service directly with spec content
        self._create_service_with_spec(
            service_name=service_name,
            compute_pool=compute_pool,
            spec_content=spec_content,
            external_access_integrations=external_access,
        )

        # Wait for service to be ready
        self.wait_for_service_ready(service_name)

        # Get the service endpoint URL
        url = self.get_server_ui_url(service_name)

        log.debug(
            "✓ Remote Development Environment %s created successfully!", service_name
        )
        return ServiceOperationResult(service_name, url, ServiceResult.CREATED.value)

    def start(
        self,
        name: Optional[str] = None,
        compute_pool: Optional[str] = None,
        external_access: Optional[List[str]] = None,
        stage: Optional[str] = None,
        image: Optional[str] = None,
    ) -> ServiceOperationResult:
        """
        Starts a remote development environment with VS Code Server.

        This method is idempotent and handles various service states:
        - If service doesn't exist: creates it
        - If service exists but is suspended: resumes it
        - If service exists and is running: returns existing URL
        - If service is starting: waits for it to be ready
        - If service is failed: recreates it

        Args:
            name: Custom name for the service. If None, generates timestamp-based name.
                 Can be either a short name (e.g., 'myproject') or full service name
                 (e.g., 'SNOW_REMOTE_admin_myproject').
            compute_pool: Name of the compute pool to use. Required only for new service
                         creation, not needed for resuming existing services.
            external_access: List of external access integration names to enable network
                           access to external resources.
            stage: Internal Snowflake stage to mount for persistent storage
                  (e.g., '@my_stage' or '@my_stage/folder').
            image: Custom image to use for the remote development environment.
                   Can be either a full image path (e.g., 'repo/image:tag') or just a tag (e.g., '1.7.1').
                   If not provided, uses the default image.

        Returns:
            ServiceOperationResult with:
            - service_name: Full resolved service name
            - url: VS Code Server URL for accessing the environment
            - status: One of 'created', 'resumed', or 'running'

        Raises:
            ValueError: If neither name nor compute_pool is provided, or if compute_pool
                       is missing for new service creation.
            RuntimeError: If service fails to start or become ready within timeout.

        Examples:
            # Resume existing service (tuple unpacking still works)
            service_name, url, status = manager.start(name="myproject")

            # Create new service with auto-generated name (using named fields)
            result = manager.start(compute_pool="my_pool")

            # Create named service with stage mount
            result = manager.start(
                name="myproject",
                compute_pool="my_pool",
                stage="@my_stage"
            )
        """
        # Validate that either name (for resume) or compute_pool (for creation) is provided
        if not name and not compute_pool:
            raise ValueError(
                "Either 'name' (for service resumption) or 'compute_pool' (for service creation) must be provided"
            )

        # Resolve service name (handles both custom names and existing service names)
        if not name:
            name = datetime.now().strftime("%y%m%d%H%M")
        service_name = self._resolve_service_name(name)

        # Check if service already exists and its status
        service_exists, current_status = self._get_service_status(service_name)

        # Handle existing service based on status
        if service_exists and current_status:
            result = self._handle_existing_service(service_name, current_status)
            if result is not None:  # Service was handled successfully
                return result
            # If result is None, service needs recreation - continue to creation
        else:
            log.debug("Service %s does not exist, creating...", service_name)

        # Create the service (either new or recreating failed one)
        return self._create_new_service(
            service_name=service_name,
            compute_pool=compute_pool,
            external_access=external_access,
            stage=stage,
            image=image,
        )

    def _create_service_with_spec(
        self,
        service_name: str,
        compute_pool: str,
        spec_content: str,
        external_access_integrations: Optional[List[str]] = None,
    ) -> None:
        """Create a service directly with spec content (avoiding temporary files).

        This method constructs and executes a CREATE SERVICE SQL statement with the
        provided YAML specification embedded directly in the query.

        Args:
            service_name: Full name of the service to create (e.g., 'SNOW_REMOTE_admin_myproject')
            compute_pool: Name of the compute pool to run the service on
            spec_content: Complete YAML specification content as string
            external_access_integrations: Optional list of external access integration names
                                        to enable network access to external resources

        Raises:
            Exception: If service creation fails due to SQL execution errors
        """

        # Build the CREATE SERVICE SQL query
        query_parts = [
            f"CREATE SERVICE {service_name}",
            f"IN COMPUTE POOL {compute_pool}",
            f"FROM SPECIFICATION $$---",
            spec_content,
            "$$",
            "MIN_INSTANCES = 1",
            "MAX_INSTANCES = 1",
            "AUTO_RESUME = true",
        ]

        if external_access_integrations:
            eai_list = ",".join(f"{e}" for e in external_access_integrations)
            query_parts.append(f"EXTERNAL_ACCESS_INTEGRATIONS = ({eai_list})")

        query_parts.append(
            "COMMENT = 'Remote development environment created by Snowflake CLI'"
        )

        query = "\n".join(query_parts)
        self.execute_query(query)

    def wait_for_service_ready(
        self, service_name: str, timeout_minutes: int = DEFAULT_SERVICE_TIMEOUT_MINUTES
    ) -> None:
        """Wait for service to be in RUNNING state using Snowflake's native SPCS_WAIT_FOR function."""
        cc.step(f"Waiting for service {service_name} to be ready...")

        timeout_seconds = timeout_minutes * 60

        try:
            # Use Snowflake's native SPCS_WAIT_FOR function
            # Reference: https://docs.snowflake.com/en/sql-reference/functions/spcs_wait_for
            wait_query = f"CALL {service_name}!SPCS_WAIT_FOR('{ServiceStatus.RUNNING.value}', {timeout_seconds})"

            log.debug("Executing SPCS_WAIT_FOR: %s", wait_query)
            result = self.execute_query(wait_query).fetchone()

            if result:
                log.debug(
                    "✓ Service %s completed waiting! Result: %s",
                    service_name,
                    result[0],
                )
            else:
                log.debug("✓ Service %s completed waiting!", service_name)

        except Exception as e:
            # SPCS_WAIT_FOR returns an error if timeout is reached or status can't be achieved
            error_msg = str(e)
            log.debug("SPCS_WAIT_FOR failed: %s", error_msg)

            # Re-raise with a more user-friendly message
            if "timeout" in error_msg.lower():
                raise RuntimeError(
                    f"Service {service_name} did not become ready within {timeout_minutes} minutes. "
                    f"Check service status with 'snow remote list' for more details."
                ) from e
            else:
                raise RuntimeError(
                    f"Service {service_name} failed to start. Error: {error_msg}"
                ) from e

    def get_server_ui_url(self, service_name: str) -> str:
        """Get the VS Code server UI endpoint URL for a service.

        Args:
            service_name: Full name of the service to get URL for

        Returns:
            Public URL for accessing the VS Code Server UI

        Raises:
            RuntimeError: If no VS Code endpoint is found for the service
        """
        service_manager = ServiceManager()
        endpoints = service_manager.list_endpoints(service_name)

        # Look for the server-ui endpoint
        for row in endpoints:
            # Assuming the row format is [name, port, public, protocol, host, url]
            if len(row) >= 6 and row[0] == SERVER_UI_ENDPOINT_NAME:
                return row[5]  # Return the URL

        raise RuntimeError(f"No VS Code endpoint found for service {service_name}")

    def list_services(self) -> SnowflakeCursor:
        """List all remote development environment services.

        Returns:
            Cursor containing service information including name, status, database,
            schema, compute pool, external access integrations, and timestamps.
        """
        # Use SQL to list services with the SNOW_CR prefix
        query = f"""
        SHOW SERVICES EXCLUDE JOBS
        LIKE '{SERVICE_NAME_PREFIX}_%';
        """
        cur = self.execute_query(query)
        qid = cur.sfqid
        return cur.execute(
            f"""
            select "name", "status", "database_name", "schema_name", "compute_pool", "external_access_integrations", "created_on", "updated_on", "resumed_on", "suspended_on", "comment"
            FROM TABLE(RESULT_SCAN('{qid}'))
        """
        )

    def stop(self, name_input: str) -> SnowflakeCursor:
        """Stop (suspend) a remote development service.

        Args:
            name_input: Service name (can be short name or full service name)

        Returns:
            Cursor from the suspend operation
        """
        service_name = self._resolve_service_name(name_input)
        service_manager = ServiceManager()
        return service_manager.suspend(service_name)

    def delete(self, name_input: str) -> SnowflakeCursor:
        """Delete a remote development service.

        Args:
            name_input: Service name (can be short name or full service name)

        Returns:
            Cursor from the drop service operation

        Warning:
            This permanently deletes the service and all associated data.
        """
        service_name = self._resolve_service_name(name_input)
        return self.execute_query(f"DROP SERVICE {service_name}")
