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
import time
from datetime import datetime
from typing import Dict, List, NamedTuple, Optional, Tuple

from snowflake.cli._plugins.remote.constants import (
    DEFAULT_ENDPOINT_TIMEOUT_MINUTES,
    DEFAULT_SERVICE_TIMEOUT_MINUTES,
    DEFAULT_SSH_REFRESH_INTERVAL,
    SERVER_UI_ENDPOINT_NAME,
    SERVICE_NAME_PREFIX,
    SSH_COUNTDOWN_INTERVAL,
    SSH_RETRY_INTERVAL,
    USER_WORKSPACE_VOLUME_MOUNT_PATH,
    WEBSOCKET_SSH_ENDPOINT_NAME,
    ServiceResult,
    ServiceStatus,
)
from snowflake.cli._plugins.remote.container_spec import generate_service_spec_yaml
from snowflake.cli._plugins.remote.utils import (
    cleanup_ssh_config,
    generate_ssh_key_pair,
    get_existing_ssh_key,
    get_ssh_key_paths,
    launch_ide,
    setup_ssh_config_with_token,
    validate_endpoint_ready,
    validate_service_name,
)
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

log = logging.getLogger(__name__)


class ServiceOperationResult(NamedTuple):
    """Result of a service operation containing service details."""

    service_name: Optional[str]
    url: Optional[str]
    status: Optional[str]


class RemoteManager(SqlExecutionMixin):
    """Manager for remote development environments using Snowpark Container Services."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Holds at most two most-recent temporary connections created for token refresh
        self._temp_connections: List[SnowflakeConnection] = []

    def _enqueue_temp_connection(self, conn: SnowflakeConnection) -> None:
        """Add a temporary connection to the queue and close older ones.

        Keeps at most two connections (latest and previous). Older ones are closed immediately.
        """
        try:
            self._temp_connections.append(conn)
            while len(self._temp_connections) > 2:
                old = self._temp_connections.pop(0)
                try:
                    old.close()
                except Exception as e:
                    log.debug("Failed to close outdated temp connection: %s", e)
        except Exception as e:
            log.debug("Failed to manage temp connection queue: %s", e)

    def _close_temp_connections(self) -> None:
        """Close all remaining temporary connections in the queue."""
        while self._temp_connections:
            conn = self._temp_connections.pop()
            try:
                conn.close()
            except Exception as e:
                log.debug("Failed to close temp connection during shutdown: %s", e)

    def _get_current_snowflake_user(self) -> str:
        """Get the current Snowflake username from the connection."""
        result = self.execute_query("SELECT CURRENT_USER()").fetchone()
        return result[0] if result else "unknown"

    def _setup_ssh_key(self, service_name: str, generate_key: bool) -> Optional[str]:
        """Set up SSH key for a service and return the public key.

        Args:
            service_name: Name of the service
            generate_key: Whether to generate or use SSH keys for the service.
                         If False, no SSH key operations are performed.

        Returns:
            SSH public key content if generate_key is True and either:
            - An existing SSH key pair is found for the service, or
            - A new SSH key pair is successfully generated
            None if generate_key is False (no SSH key operations performed)
        """
        if not generate_key:
            return None

        ssh_key_result = get_existing_ssh_key(service_name)
        if ssh_key_result:
            _, ssh_public_key = ssh_key_result
            log.debug("Using existing SSH key pair for service %s", service_name)
            return ssh_public_key
        else:
            log.debug("Generating SSH key pair for service %s", service_name)
            _, ssh_public_key = generate_ssh_key_pair(service_name)
            return ssh_public_key

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

    def _warn_about_config_changes(
        self,
        service_name: str,
        stage: Optional[str] = None,
        image: Optional[str] = None,
        external_access: Optional[List[str]] = None,
        compute_pool: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Warn user if they provided configuration options that cannot be applied to existing service.

        Args:
            service_name: Name of the existing service
            stage: Stage mount option provided by user
            image: Custom image option provided by user
            external_access: External access integrations provided by user
            compute_pool: Compute pool option provided by user
            **kwargs: Other configuration options
        """
        has_modifying_options = any(
            option is not None
            for option in (
                stage,
                image,
                external_access,
                compute_pool,
            )
        )

        if has_modifying_options:
            cc.warning(
                f"Service '{service_name}' is already running. "
                "Configuration changes provided to 'snow remote start' will be ignored for existing services. "
                f"To apply changes, delete the service first with 'snow remote delete {service_name}' and then start it again with the new configuration."
            )

    def _finalize_service_result(
        self,
        service_name: str,
        result_status: ServiceResult,
        timeout_minutes: int = DEFAULT_ENDPOINT_TIMEOUT_MINUTES,
    ) -> ServiceOperationResult:
        """
        Finalize service operation by getting validated URL and returning result.

        Args:
            service_name: Name of the service
            result_status: Status to return in ServiceOperationResult
            timeout_minutes: Timeout for endpoint validation

        Returns:
            ServiceOperationResult with validated URL
        """
        url = self.get_validated_server_ui_url(service_name, timeout_minutes)
        return ServiceOperationResult(service_name, url, result_status.value)

    def _handle_existing_service(
        self, service_name: str, current_status: str, **config_options
    ) -> Optional[ServiceOperationResult]:
        """
        Handle an existing service based on its current service status.

        Note: current_status is always service status (from DESC SERVICE).

        Args:
            service_name: Name of the service
            current_status: Current service status from DESC SERVICE
            **config_options: Configuration options provided by user (stage, image, external_access, etc.)

        Returns:
            ServiceOperationResult with service details, or None if service needs recreation
        """
        # Check if user provided configuration options that would modify the service
        self._warn_about_config_changes(service_name, **config_options)

        # Handle service statuses (from DESC SERVICE)
        if current_status == ServiceStatus.RUNNING.value:
            log.debug("Service %s is already running", service_name)
            # Use shorter timeout since service should already be ready
            return self._finalize_service_result(
                service_name, ServiceResult.RUNNING, timeout_minutes=2
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
            return self._finalize_service_result(service_name, ServiceResult.RESUMED)

        elif current_status == ServiceStatus.PENDING.value:
            log.debug(
                "Service %s is pending, waiting for it to be ready...",
                service_name,
            )
            self.wait_for_service_ready(service_name)
            return self._finalize_service_result(service_name, ServiceResult.RUNNING)

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
        ssh_public_key: Optional[str],
    ) -> ServiceOperationResult:
        """
        Create a new service or recreate a failed one.

        Returns:
            ServiceOperationResult with service details
        """
        # Validate compute pool is provided for service creation
        if not compute_pool:
            raise CliError("compute_pool is required for creating a new service")

        cc.step(f"Creating remote development environment '{service_name}'...")

        # Generate container service specification as YAML
        spec_content = generate_service_spec_yaml(
            session=self.snowpark_session,
            compute_pool=compute_pool,
            stage=stage,
            image=image,
            ssh_public_key=ssh_public_key,
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

        log.debug(
            "✓ Remote Development Environment %s created successfully!", service_name
        )
        return self._finalize_service_result(service_name, ServiceResult.CREATED)

    def start(
        self,
        name: Optional[str] = None,
        compute_pool: Optional[str] = None,
        external_access: Optional[List[str]] = None,
        stage: Optional[str] = None,
        image: Optional[str] = None,
        generate_ssh_key: bool = False,
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
            generate_ssh_key: Whether to generate a new SSH key pair for the service.

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
            raise CliError(
                "Either 'name' (for service resumption) or 'compute_pool' (for service creation) must be provided"
            )

        # Resolve service name (handles both custom names and existing service names)
        if not name:
            resolved_name = datetime.now().strftime("%y%m%d%H%M")
        else:
            resolved_name = name

        service_name = self._resolve_service_name(resolved_name)

        # Validate final resolved service name to prevent CREATE SERVICE failures
        validate_service_name(service_name)

        # Check if service already exists and its status
        service_exists, current_status = self._get_service_status(service_name)

        # Handle existing service based on status
        if service_exists and current_status:
            result = self._handle_existing_service(
                service_name,
                current_status,
                stage=stage,
                image=image,
                external_access=external_access,
                compute_pool=compute_pool,
            )
            if result is not None:  # Service was handled successfully
                return result
            # If result is None, service needs recreation - continue to creation
        else:
            cc.step(f"Service {service_name} does not exist, creating...")

        # Handle SSH key generation if requested
        ssh_public_key = self._setup_ssh_key(service_name, generate_ssh_key)

        # Create the service (either new or recreating failed one)
        return self._create_new_service(
            service_name=service_name,
            compute_pool=compute_pool,
            external_access=external_access,
            stage=stage,
            image=image,
            ssh_public_key=ssh_public_key,
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
                raise CliError(
                    f"Service {service_name} did not become ready within {timeout_minutes} minutes. "
                    f"Check service status with 'snow remote list' for more details."
                ) from e
            else:
                raise CliError(
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
        return self.get_endpoint_url(service_name, SERVER_UI_ENDPOINT_NAME)

    def get_validated_server_ui_url(
        self, service_name: str, timeout_minutes: int = DEFAULT_ENDPOINT_TIMEOUT_MINUTES
    ) -> str:
        """
        Get server UI URL and validate that the endpoint is ready and responding.

        This method combines URL retrieval and endpoint validation to ensure the returned URL
        is immediately usable by the user.

        Args:
            service_name: Full name of the service
            timeout_minutes: Maximum time to wait for endpoint readiness

        Returns:
            Validated server UI URL

        Raises:
            CliError: If URL cannot be retrieved or endpoint doesn't become ready
        """
        # Get the server UI URL
        url = self.get_server_ui_url(service_name)

        try:
            self.snowpark_session.sql(
                "ALTER SESSION SET python_connector_query_result_format = 'JSON'"
            ).collect()
            token = self.snowpark_session.connection.rest.token
        except Exception as e:
            raise CliError(
                f"Failed to get authentication token for endpoint validation: {e}"
            )

        if not token:
            raise CliError("No authentication token available for endpoint validation")

        validate_endpoint_ready(url, token, SERVER_UI_ENDPOINT_NAME, timeout_minutes)

        return url

    def get_public_endpoint_urls(self, service_name: str) -> Dict[str, str]:
        """Get all public endpoint URLs for a service.

        Args:
            service_name: Full name of the service to get URLs for

        Returns:
            Dictionary mapping endpoint names to URLs

        Raises:
            RuntimeError: If no endpoints are found for the service
        """
        endpoints_cursor = self.execute_query(
            f"show endpoints in service {service_name}", cursor_class=DictCursor
        )

        public_endpoints = {}
        for row in endpoints_cursor:
            # Only include public endpoints that have an ingress URL
            if row.get("is_public") and row.get("ingress_url"):
                endpoint_name = row["name"]
                url = row["ingress_url"]
                public_endpoints[endpoint_name] = url
                log.debug("Found public endpoint: %s -> %s", endpoint_name, url)
            else:
                log.debug(
                    "Skipping endpoint %s: is_public=%s, has_url=%s",
                    row.get("name"),
                    row.get("is_public"),
                    bool(row.get("ingress_url")),
                )

        log.debug("Total public endpoints found: %d", len(public_endpoints))
        return public_endpoints

    def get_endpoint_url(self, service_name: str, endpoint_name: str) -> str:
        """Get a specific endpoint URL for a service.

        Args:
            service_name: Full name of the service
            endpoint_name: Name of the endpoint to retrieve

        Returns:
            The endpoint URL

        Raises:
            RuntimeError: If the endpoint is not found
        """
        endpoints = self.get_public_endpoint_urls(service_name)

        if endpoint_name not in endpoints:
            available = ", ".join(endpoints.keys()) if endpoints else "none"
            raise CliError(
                f"Endpoint '{endpoint_name}' not found for service {service_name}. "
                f"Available public endpoints: {available}"
            )

        url = endpoints[endpoint_name]
        log.debug("Retrieved endpoint URL for %s: %s", endpoint_name, url)
        return url

    def validate_ide_requirements(
        self, name: Optional[str], eai_name: Optional[List[str]]
    ) -> bool:
        """Validate that EAI is provided when creating a new service for IDE launch,
        or that existing service has EAI configured.

        Args:
            name: Service name (if provided)
            eai_name: External access integration names

        Returns:
            True if creating a new service, False if using existing service

        Raises:
            CliError: If EAI is required but not provided, or if existing service lacks EAI
        """
        creating = False
        if not name:
            creating = True
        else:
            resolved_name = self._resolve_service_name(name)
            exists, _status = self._get_service_status(resolved_name)
            creating = not exists

        if creating:
            if not eai_name:
                raise CliError(
                    "External access integration is required for IDE launch. Provide --eai-name."
                )
        else:
            # For existing services, check if they have EAI configured
            # SSH setup for IDE requires internet access
            try:
                desc_cursor = self.execute_query(
                    f"DESC SERVICE {resolved_name}", cursor_class=DictCursor
                )
                service_details = desc_cursor.fetchone()

                if service_details:
                    existing_eai = service_details.get("external_access_integrations")
                    if not existing_eai or existing_eai.strip().lower() in (
                        "",
                        "none",
                        "null",
                    ):
                        raise CliError(
                            f"Service '{name}' does not have external access integration configured. "
                            "External access integration is required for IDE launch to enable network access for remote setup. "
                            f"Please delete the service first with 'snow remote delete {name}' and recreate it with proper --eai-name."
                        )
            except Exception as e:
                if "not found" in str(e).lower():
                    # Service doesn't exist, will be created
                    creating = True
                    if not eai_name:
                        raise CliError(
                            "External access integration is required for IDE launch. Provide --eai-name."
                        )
                else:
                    raise CliError(f"Failed to validate service configuration: {e}")

        return creating

    def setup_ssh_connection(
        self,
        service_name: str,
        refresh_interval: int = DEFAULT_SSH_REFRESH_INTERVAL,
        ide: Optional[str] = None,
    ) -> None:
        """Set up SSH connection with token refresh for a remote service.

        This is a blocking operation that continuously refreshes authentication tokens.
        Automatically detects and uses SSH keys if they exist for the service.

        Args:
            service_name: Full name of the service
            refresh_interval: Token refresh interval in seconds
            ide: Optional IDE to launch ("code" or "cursor"). If provided, the IDE will be
                launched after the initial SSH configuration is written.
        """

        try:
            # Step 1: Do all preparation work
            # Get the websocket-ssh endpoint hostname
            log.debug("Getting SSH endpoint for '%s'...", service_name)
            ssh_hostname = self.get_endpoint_url(
                service_name, WEBSOCKET_SSH_ENDPOINT_NAME
            )
            log.debug("Found websocket SSH hostname: %s", ssh_hostname)

            # Check if SSH keys exist for this service
            private_key_path = None
            private_key_file, _ = get_ssh_key_paths(service_name)
            if private_key_file.exists():
                private_key_path = str(private_key_file)
                log.debug(
                    "Found SSH private key for service '%s': %s",
                    service_name,
                    private_key_path,
                )
            else:
                log.debug(
                    "No SSH private key found for service '%s', using token-only authentication",
                    service_name,
                )

            # Configure session for token compatibility
            log.debug("Configuring session for SSH token compatibility...")
            self.snowpark_session.sql(
                "ALTER SESSION SET python_connector_query_result_format = 'JSON'"
            ).collect()

            # Step 2: Refresh token and write SSH config for the first time
            token = self._get_fresh_token()
            setup_ssh_config_with_token(
                service_name, ssh_hostname, token, private_key_path
            )

            # Step 3: Launch IDE if requested
            if ide:
                cc.step(
                    f"Starting {ide} and managing SSH session. You can also SSH via 'ssh {service_name}'. Press Ctrl+C to stop."
                )
                launch_ide(ide, service_name, USER_WORKSPACE_VOLUME_MOUNT_PATH)
            else:
                cc.step(f"Starting SSH session management for '{service_name}'...")
                cc.step(f"You can now connect using: ssh {service_name}")
                cc.step(f"Press Ctrl+C to stop SSH session management")

            # Step 4: Start refresh loop (delay first refresh since we just wrote config)
            self._ssh_token_refresh_loop(
                service_name,
                ssh_hostname,
                private_key_path,
                refresh_interval,
                delay_first_refresh=True,
            )

        except KeyboardInterrupt:
            cc.step("\n🛑 SSH session management stopped.")
        except Exception as e:
            raise CliError(f"Error setting up SSH: {str(e)}")
        finally:
            # Clean up SSH configuration when SSH session ends
            cc.step("🧹 Cleaning up SSH configuration...")
            cleanup_ssh_config(service_name)
            # Ensure any remaining temporary connections are closed
            self._close_temp_connections()

    def _ssh_token_refresh_loop(
        self,
        service_name: str,
        ssh_hostname: str,
        private_key_path: Optional[str],
        refresh_interval: int,
        delay_first_refresh: bool = False,
    ) -> None:
        """Handle the SSH token refresh loop.

        Args:
            service_name: Name of the service
            ssh_hostname: SSH hostname for the service
            private_key_path: Path to private key file (if any)
            refresh_interval: Interval between token refreshes in seconds
            delay_first_refresh: If True, wait before first refresh (useful when SSH config was just written)
        """
        token_refresh_count = 0

        try:
            # If delay is requested, wait before first refresh
            if delay_first_refresh:
                log.debug(
                    "Delaying first token refresh by %d seconds", refresh_interval
                )
                self._interruptible_sleep(refresh_interval)

            while True:
                log.debug("Token refresh cycle #%d", token_refresh_count + 1)

                # Get fresh token with proper interrupt handling
                try:
                    token = self._get_fresh_token()
                except KeyboardInterrupt:
                    # Re-raise KeyboardInterrupt immediately to allow clean exit
                    raise
                except Exception as e:
                    # Handle token refresh failure with retry logic
                    cc.step(
                        f"❌ Unable to get token: {str(e)}. Retrying in 30 seconds..."
                    )
                    self._interruptible_sleep(SSH_RETRY_INTERVAL)
                    continue

                # Update SSH configuration
                auth_method = "SSH key" if private_key_path else "token-only"
                log.debug("Updating SSH config with %s authentication", auth_method)

                setup_ssh_config_with_token(
                    service_name, ssh_hostname, token, private_key_path
                )

                token_refresh_count += 1
                cc.step(f"SSH configuration updated (refresh #{token_refresh_count})")

                # Wait for next refresh with countdown
                self._wait_with_countdown(refresh_interval)

        except KeyboardInterrupt:
            # Re-raise KeyboardInterrupt to be handled by the outer method
            raise
        except Exception as e:
            raise CliError(f"SSH error occurred: {str(e)}")

    def _get_fresh_token(self) -> str:
        """Get a fresh authentication token with natural session expiration.

        Creates a connection that will expire naturally according to Snowflake's
        default session timeout, allowing proper token lifecycle management
        without artificial keep-alive mechanisms.
        """
        # Import here to avoid circular dependency
        from snowflake.cli._app.snow_connector import connect_to_snowflake

        fresh_connection = None
        try:
            log.debug("Creating fresh connection for SSH token...")

            current_context = get_cli_context().connection_context

        # Create connection with natural session expiration for SSH token refresh
        fresh_connection = connect_to_snowflake(
            connection_name=current_context.connection_name,
            temporary_connection=current_context.temporary_connection,
            # Allow session to expire naturally - don't keep it alive artificially
            using_session_keep_alive=False,
        )

        # Track the connection and proactively cap the number of live temp connections
        self._enqueue_temp_connection(fresh_connection)

        fresh_connection.cursor().execute(
            "ALTER SESSION SET python_connector_query_result_format = 'JSON'"
        )

        token = fresh_connection.rest.token
        if not token:
            raise RuntimeError("No token available from fresh connection")

        return token

    def _wait_with_countdown(self, duration: int) -> None:
        """Wait with periodic countdown messages. Raises KeyboardInterrupt if interrupted."""
        end_time = time.time() + duration

        log.debug("Next refresh in %d seconds...", duration)

        while time.time() < end_time:
            remaining = int(end_time - time.time())
            if remaining > 0 and remaining % SSH_COUNTDOWN_INTERVAL == 0:
                log.debug(
                    "⏳ Next refresh in %d seconds... (Press Ctrl+C to stop)", remaining
                )
            time.sleep(1)

    def _interruptible_sleep(self, duration: int) -> None:
        """Sleep for the specified duration while allowing KeyboardInterrupt.

        This method sleeps in 1-second intervals, allowing for clean interruption
        via Ctrl+C (KeyboardInterrupt) at any point during the wait period.

        Args:
            duration: Number of seconds to sleep
        """
        end_time = time.time() + duration

        while time.time() < end_time:
            time.sleep(1)

    def list_services(self) -> SnowflakeCursor:
        """List all remote development environment services.

        Returns:
            Cursor containing essential service information: name, status,
            compute pool, and creation date (ordered by most recent first).
        """
        # Use SQL to list services with the SNOW_REMOTE prefix
        query = f"""
        SHOW SERVICES EXCLUDE JOBS
        LIKE '{SERVICE_NAME_PREFIX}_%';
        """
        cur = self.execute_query(query)
        qid = cur.sfqid
        return cur.execute(
            f"""
            select "name", "status", "compute_pool", "created_on"
            FROM TABLE(RESULT_SCAN('{qid}'))
            ORDER BY "created_on" DESC
        """
        )

    def get_service_info(self, name_input: str) -> dict:
        """Get detailed information about a specific remote development service.

        Args:
            name_input: Service name (can be short name or full service name)

        Returns:
            Dictionary containing formatted service information including service
            details, public endpoints, and URLs for easy display.

        Raises:
            CliError: If service is not found or query fails
        """
        service_name = self._resolve_service_name(name_input)
        validate_service_name(service_name)

        # Use DESC SERVICE to get detailed service information
        try:
            desc_cursor = self.execute_query(
                f"DESC SERVICE {service_name}", cursor_class=DictCursor
            )
            # DESC SERVICE returns exactly one row with service details, or none if service doesn't exist
            service_details = desc_cursor.fetchone()

            if not service_details:
                raise CliError(f"Remote service '{name_input}' not found.")

        except Exception as e:
            if "does not exist" in str(e).lower():
                raise CliError(f"Remote service '{name_input}' not found.")
            raise CliError(f"Failed to retrieve service information: {e}")

        # Get public endpoints if service is running
        endpoints = {}
        if service_details.get("status") == "RUNNING":
            try:
                endpoint_urls = self.get_public_endpoint_urls(service_name)
                if endpoint_urls:
                    endpoints = endpoint_urls
            except Exception:
                # Don't fail if we can't get endpoints, just continue without them
                pass

        # Format the information for display
        info = {
            "Service Information": {
                "Name": service_details.get("name", service_name),
                "Status": service_details.get("status", "Unknown"),
                "Database": service_details.get("database_name", "N/A"),
                "Schema": service_details.get("schema_name", "N/A"),
                "Compute Pool": service_details.get("compute_pool", "N/A"),
                "External Access Integrations": service_details.get(
                    "external_access_integrations", "None"
                ),
                "Comment": service_details.get("comment", "None"),
            },
            "Timestamps": {
                "Created": service_details.get("created_on", "N/A"),
                "Updated": service_details.get("updated_on", "N/A"),
                "Resumed": service_details.get("resumed_on", "N/A"),
                "Suspended": service_details.get("suspended_on", "N/A"),
            },
        }

        # Add endpoints section if available
        if endpoints:
            info["Public Endpoints"] = {}
            for endpoint_name, endpoint_url in endpoints.items():
                info["Public Endpoints"][endpoint_name] = endpoint_url

        return info

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
