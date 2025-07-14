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
from snowflake.cli._plugins.container_runtime import constants
from snowflake.cli._plugins.container_runtime.container_payload import (
    create_container_payload,
)
from snowflake.cli._plugins.container_runtime.container_spec import (
    generate_service_spec,
)
from snowflake.cli._plugins.spcs.services.manager import ServiceManager
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.sql_execution import SqlExecutionMixin


class ContainerRuntimeManager(SqlExecutionMixin):
    DEFAULT_COMPUTE_POOL = "E2E_CPU_POOL"
    DEFAULT_TIMEOUT_MIN = 60
    DEFAULT_STORAGE_SIZE_GB = 10
    DEFAULT_SERVICE_PREFIX = "SNOW_CR"
    DEFAULT_EAI = "ALLOW_ALL_INTEGRATION"

    def create(
        self,
        name: Optional[str] = None,
        compute_pool: Optional[str] = None,
        warehouse: Optional[str] = None,
        persistent_storage: bool = False,
        storage_size: int = DEFAULT_STORAGE_SIZE_GB,
        external_access: bool = False,
        timeout: int = DEFAULT_TIMEOUT_MIN,
        extensions: Optional[List[str]] = None,
        stage: Optional[str] = None,
        stage_mount_path: str = constants.USER_STAGE_VOLUME_MOUNT_PATH,
    ) -> str:
        """
        Creates a new container runtime service with VS Code Server.

        Args:
            stage: Internal Snowflake stage to mount (e.g., @my_stage or @my_stage/folder)
            stage_mount_path: Path where the stage will be mounted in the container
        """
        # Generate service name if not provided
        if not name:
            username = get_cli_context().connection.user.lower()
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            name = f"{self.DEFAULT_SERVICE_PREFIX}_{username}_{timestamp}"
        else:
            name = f"{self.DEFAULT_SERVICE_PREFIX}_{name}"

        # Use default compute pool if not provided
        if not compute_pool:
            compute_pool = self.DEFAULT_COMPUTE_POOL

        # Use current warehouse if not provided
        if not warehouse:
            warehouse = get_cli_context().connection.warehouse

        # Validate stage format if provided
        if stage:
            if not stage.startswith("@") and not stage.startswith("snow://"):
                raise ValueError(
                    "Stage name must start with '@' (e.g., @my_stage) or 'snow://' (e.g., snow://notebook/db.schema.notebook_name/versions/live)"
                )
            # Validate stage mount path is absolute
            if not stage_mount_path.startswith("/"):
                raise ValueError(
                    "Stage mount path must be an absolute path (e.g., /mnt/user-stage)"
                )

        # Handle secondary roles for workspace stages
        if stage and stage.startswith("snow://"):
            file_list = self.snowpark_session.sql(f"LIST {stage}").collect()
            cc.step(f"Files in the nested stage: {file_list}")

        # Generate a service specification
        spec = self._generate_service_spec(
            persistent_storage=persistent_storage,
            storage_size=storage_size,
            external_access=external_access,
            timeout=timeout,
            extensions=extensions,
            stage=stage,
            stage_mount_path=stage_mount_path,
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

            external_access_integrations = (
                ["EXTERNAL_ACCESS_INTEGRATION"]
                if external_access
                else [self.DEFAULT_EAI]
            )

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

    def _generate_service_spec(
        self,
        persistent_storage: bool = False,
        storage_size: int = DEFAULT_STORAGE_SIZE_GB,
        external_access: bool = False,
        timeout: int = DEFAULT_TIMEOUT_MIN,
        extensions: Optional[List[str]] = None,
        stage: Optional[str] = None,
        stage_mount_path: str = constants.USER_STAGE_VOLUME_MOUNT_PATH,
    ) -> dict:
        """Generate a service specification for VS Code Server using the helper modules."""
        # Create a session for spec generation
        session = self.snowpark_session

        # Create environment variables
        environment_vars = {
            "TZ": "Etc/UTC",
            "SESSION_TIMEOUT": str(timeout * 60)
            if timeout
            else "3600",  # Convert minutes to seconds
            "VSCODE_PORT": "12020",  # Default VS Code server port
        }

        # Add extensions as environment variable if provided
        if extensions:
            environment_vars["VSCODE_EXTENSIONS"] = ",".join(extensions)

        # Create a container payload
        container_payload = create_container_payload(extensions=extensions)

        # Upload payload to stage (this is for the container payload, not user data)
        stage_path = f"@zzhu_container"  # Use dedicated stage for container payload
        uploaded_payload = container_payload.upload(session, stage_path)

        # Generate service spec
        spec = generate_service_spec(
            session=session,
            compute_pool=self.DEFAULT_COMPUTE_POOL,  # This will be overridden in the create method
            payload=uploaded_payload,
            persistent_storage=persistent_storage,
            storage_size=storage_size,
            environment_vars=environment_vars,
            enable_metrics=True,  # Enable platform metrics
            stage=stage,
            stage_mount_path=stage_mount_path,
        )

        return spec

    def wait_for_service_ready(self, service_name: str, timeout_sec: int = 300) -> bool:
        """Wait for the service to be in READY state."""
        service_manager = ServiceManager()
        start_time = time.time()

        while time.time() - start_time < timeout_sec:
            status_cursor = service_manager.status(service_name)
            status_row = status_cursor.fetchone()
            cc.step(f"Service status for {service_name}: {status_row}")
            if status_row:
                status_dict = json.loads(status_row[0])
                status = status_dict[0]["status"]
                if status == "READY":
                    return True
                elif status in ["FAILED", "UNKNOWN"]:
                    raise RuntimeError(
                        f"Service {service_name} failed to start with status: {status}"
                    )

            # Wait before checking again
            time.sleep(5)

        raise TimeoutError(f"Timeout waiting for service {service_name} to be ready")

    def _get_service_endpoint_url(self, service_name: str) -> str:
        """Get the URL for the VS Code endpoint."""
        service_manager = ServiceManager()
        endpoints_cursor = service_manager.list_endpoints(service_name)

        for endpoint in endpoints_cursor:
            if endpoint[0] == "server-ui":  # Endpoint name
                return endpoint[5]  # URL column

        raise RuntimeError(f"No VS Code endpoint found for service {service_name}")

    def get_service_endpoint_url(self, service_name: str) -> str:
        """Get the public URL for the VS Code endpoint of a service."""
        return self._get_service_endpoint_url(service_name)

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
        # return cur.execute(f"desc result '{qid}'")

    def stop(self, name: str):
        """Stop a container runtime service."""
        service_manager = ServiceManager()
        return service_manager.suspend(name)

    def start(self, name: str):
        """Start a container runtime service."""
        service_manager = ServiceManager()
        return service_manager.resume(name)

    def delete(self, name: str):
        """Delete a container runtime service."""
        query = f"DROP SERVICE IF EXISTS {name}"
        return self.execute_query(query)
