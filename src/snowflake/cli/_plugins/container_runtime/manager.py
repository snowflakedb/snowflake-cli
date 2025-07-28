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

import json
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import yaml
from snowflake.cli._plugins.container_runtime.container_spec import (
    generate_service_spec,
)
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.sql_execution import SqlExecutionMixin


class ContainerRuntimeManager(SqlExecutionMixin):
    DEFAULT_SERVICE_PREFIX = "SNOW_CR"
    DEFAULT_EAI = "ALLOW_ALL_INTEGRATION"

    def create(
        self,
        compute_pool: str,
        name: Optional[str] = None,
        warehouse: Optional[str] = None,
        external_access: Optional[List[str]] = None,
        stage: Optional[str] = None,
        workspace: Optional[str] = None,
        image_tag: Optional[str] = None,
    ) -> str:
        """
        Creates a new container runtime service with VS Code Server.

        Args:
            compute_pool: Name of the compute pool to use
            name: Optional custom service name
            warehouse: Optional warehouse name (defaults to connection warehouse)
            external_access: List of external access integration names
            stage: Internal Snowflake stage to mount (e.g., @my_stage or @my_stage/folder)
            workspace: Workspace to mount. Can be either a stage path or a Snowflake workspace name
            image_tag: Custom image tag to use

        Returns:
            The endpoint URL for the created service
        """
        # Determine if workspace is a stage path or workspace name
        is_workspace_name = False
        workspace_stage_path = None

        if workspace:
            if workspace.startswith("@") or workspace.startswith("snow://"):
                # Workspace is actually a stage path
                workspace_stage_path = workspace.rstrip("/")
                cc.step(f"Using workspace as stage path: {workspace_stage_path}")
            else:
                # Workspace is a workspace name - use personal database
                is_workspace_name = True
                workspace_stage_path = (
                    f"snow://workspace/USER$.public.{workspace}/versions/live"
                )
                cc.step(f"Using Snowflake workspace: {workspace} in personal database")

                # TODO: Set required session parameters for personal database, including:
                # - ENABLE_SPCS_CREATION_IN_PERSONAL_DB
                # - SNOWSERVICE_USE_CALLING_USER_FOR_AUTH_FOR_TESTING

        # Generate service name if not provided
        if not name:
            username = get_cli_context().connection.user.lower()
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            name = f"{self.DEFAULT_SERVICE_PREFIX}_{username}_{timestamp}"
        else:
            name = f"{self.DEFAULT_SERVICE_PREFIX}_{name}"

        cc.step(f"Using service name: {name}")
        cc.step(f"Using compute pool: {compute_pool}")

        # Use current warehouse if not provided
        if not warehouse:
            warehouse = get_cli_context().connection.warehouse

        # Validate stage input if provided
        if stage:
            if not stage.startswith("@") and not stage.startswith("snow://"):
                raise ValueError(
                    "Stage name must start with '@' (e.g., @my_stage) or 'snow://' (e.g., snow://notebook/db.schema.notebook_name/versions/live)"
                )
            # Remove trailing slash from stage
            stage = stage.rstrip("/")

        # Validate workspace stage path if provided
        if workspace_stage_path:
            if not workspace_stage_path.startswith(
                "@"
            ) and not workspace_stage_path.startswith("snow://"):
                raise ValueError(
                    "Workspace stage path must start with '@' (e.g., @my_stage) or 'snow://' (e.g., snow://workspace/...)"
                )

        # Handle secondary roles for workspace stages
        if stage and stage.startswith("snow://"):
            file_list = self.snowpark_session.sql(f"LIST {stage}").collect()
            cc.step(f"Files in the stage: {file_list}")

        if (
            workspace_stage_path
            and workspace_stage_path.startswith("snow://")
            and workspace_stage_path != stage
        ):
            file_list = self.snowpark_session.sql(
                f"LIST {workspace_stage_path}"
            ).collect()
            cc.step(f"Files in the workspace: {file_list}")

        # Generate a service specification
        spec = self.generate_service_spec(
            compute_pool=compute_pool,
            external_access=external_access,
            stage=stage,
            workspace_stage_path=workspace_stage_path,
            image_tag=image_tag,
        )

        with tempfile.NamedTemporaryFile(
            suffix=".yaml", mode="w+", delete=False
        ) as tmp:
            # Write spec to a temporary file
            cc.step(f"Writing service spec to {tmp.name}")
            yaml.dump(spec, tmp)
            temp_spec_file = Path(tmp.name)
            cc.step(f"Created service spec file: {temp_spec_file}")

            service_manager = ServiceManager()

            # Handle external access integrations
            external_access_integrations = external_access or [self.DEFAULT_EAI]

            spec_content = temp_spec_file.read_text()

            query = f"""\
                CREATE SERVICE IF NOT EXISTS {name}
IN COMPUTE POOL {compute_pool}
FROM SPECIFICATION $$---
{spec_content}
$$
MIN_INSTANCES = 1
MAX_INSTANCES = 1
AUTO_RESUME = TRUE
QUERY_WAREHOUSE = {warehouse}
                """

            if external_access_integrations:
                external_access_integration_list = ",".join(
                    f"{e}" for e in external_access_integrations
                )
                query += f" EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_list})"

            res = self.snowpark_session.sql(query).collect()
            cc.step(f"Created service {name}: {res}")

            # Wait for service to be ready
            self.wait_for_service_ready(name)

            # Get service endpoint
            endpoint_url = self._get_service_endpoint_url(name)
            return endpoint_url

    def generate_service_spec(
        self,
        compute_pool: str,
        external_access: Optional[List[str]] = None,
        stage: Optional[str] = None,
        workspace_stage_path: Optional[str] = None,
        image_tag: Optional[str] = None,
    ) -> dict:
        """Generate a service specification for VS Code Server using the helper modules."""
        # Create a session for spec generation
        session = self.snowpark_session

        # Create environment variables
        environment_vars = {
            "TZ": "Etc/UTC",
            "VSCODE_PORT": "12020",  # Default VS Code server port
        }

        # Generate service spec
        spec = generate_service_spec(
            session=session,
            compute_pool=compute_pool,
            environment_vars=environment_vars,
            enable_metrics=True,  # Enable platform metrics
            stage=stage,
            workspace_stage_path=workspace_stage_path,
            image_tag=image_tag,
        )

        return spec

    def wait_for_service_ready(self, service_name: str, timeout_sec: int = 300) -> bool:
        """Wait for the service to be in READY state."""
        service_manager = ServiceManager()

        start_time = time.time()
        cc.step(f"Waiting for service '{service_name}' to be ready...")

        while time.time() - start_time < timeout_sec:
            status_cursor = service_manager.status(service_name)
            status_row = status_cursor.fetchone()
            cc.step(f"Service status for {service_name}: {status_row}")
            if status_row:
                status_dict = json.loads(status_row[0])
                if status_dict:
                    status = status_dict[0]["status"]
                    if status == "READY":
                        cc.step(f"Service '{service_name}' is ready!")
                        return True
                    elif status in ["FAILED", "UNKNOWN"]:
                        raise RuntimeError(
                            f"Service {service_name} failed to start with status: {status}"
                        )

                # Wait before checking again
                time.sleep(10)

        raise Exception(
            f"Service '{service_name}' did not become ready within {timeout_sec} seconds"
        )

    def _get_service_endpoint_url(self, service_name: str) -> str:
        """Get the URL for the VS Code endpoint."""
        service_manager = ServiceManager()
        endpoints_cursor = service_manager.list_endpoints(service_name)

        for endpoint in endpoints_cursor:
            if endpoint[0] == "server-ui":  # Endpoint name
                return endpoint[5]  # URL column

        raise RuntimeError(f"No VS Code endpoint found for service {service_name}")

    def get_service_endpoint_url(self, service_name: str) -> str:
        """Public method to get service endpoint URL."""
        return self._get_service_endpoint_url(service_name)

    def get_public_endpoint_urls(self, service_name: str) -> dict:
        """Get all public endpoint URLs for the service."""
        service_manager = ServiceManager()
        endpoints_cursor = service_manager.list_endpoints(service_name)

        public_endpoints = {}
        for endpoint in endpoints_cursor:
            endpoint_name = endpoint[0]
            endpoint_url = endpoint[5] if len(endpoint) > 5 else None

            # Only include endpoints that have URLs (public endpoints)
            if endpoint_url:
                public_endpoints[endpoint_name] = endpoint_url

        return public_endpoints

    def list_services(self):
        """List all container runtime services."""
        # Use SQL to list services with the SNOW_CR prefix
        query = f"""
        SHOW SERVICES EXCLUDE JOBS
        LIKE '{self.DEFAULT_SERVICE_PREFIX}%'
        """
        cur = self.execute_query(query)
        qid = cur.sfqid
        return cur.execute(
            f"""
            select "name", "status", "database_name", "schema_name", "compute_pool", "external_access_integrations", "created_on", "updated_on", "resumed_on", "suspended_on", "comment"
            FROM TABLE(RESULT_SCAN('{qid}'))
        """
        )

    def stop(self, service_name: str):
        """Suspend a container runtime service."""
        service_manager = ServiceManager()
        return service_manager.suspend(service_name)

    def start(self, service_name: str):
        """Resume a suspended container runtime service."""
        service_manager = ServiceManager()
        return service_manager.resume(service_name)

    def delete(self, service_name: str):
        """Delete a container runtime service."""
        query = f"DROP SERVICE IF EXISTS {service_name}"
        return self.execute_query(query)
