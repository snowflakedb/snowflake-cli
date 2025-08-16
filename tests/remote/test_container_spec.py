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

from unittest.mock import Mock, patch

import pytest
from snowflake import snowpark
from snowflake.cli._plugins.remote.constants import (
    RAY_DASHBOARD_ENDPOINT_NAME,
    SERVER_UI_ENDPOINT_NAME,
    WEBSOCKET_SSH_ENDPOINT_NAME,
)
from snowflake.cli._plugins.remote.container_spec import (
    generate_service_spec,
)


@pytest.fixture
def mock_session():
    """Create a mock Snowflake session."""
    session = Mock(spec=snowpark.Session)

    # Mock compute pool query
    compute_pool_result = Mock()
    compute_pool_result.__getitem__ = Mock(return_value="CPU_X64_S")

    # Mock regions query
    region_result = Mock()
    region_result.region_group = None
    region_result.snowflake_region = "US_WEST_2"
    region_result.cloud = "aws"
    region_result.region = "us-west-2"
    region_result.display_name = "US West 2"

    current_region_result = Mock()
    current_region_result.CURRENT_REGION = "US_WEST_2"

    def mock_sql(query):
        mock_result = Mock()
        if "SHOW REGIONS" in query:
            mock_result.collect.return_value = [region_result]
        elif "SHOW COMPUTE POOLS" in query or "show compute pools" in query:
            mock_result.collect.return_value = [compute_pool_result]
        elif "CURRENT_REGION" in query:
            mock_result.collect.return_value = [current_region_result]
        return mock_result

    session.sql.side_effect = mock_sql
    return session


class TestContainerSpec:
    def test_generate_basic_spec(self, mock_session):
        """Test generating basic service specification."""
        spec = generate_service_spec(session=mock_session, compute_pool="test_pool")

        # Check basic structure
        assert "spec" in spec
        assert "containers" in spec["spec"]
        assert "endpoints" in spec["spec"]

        # Check container
        container = spec["spec"]["containers"][0]
        assert container["name"] == "main"
        assert (
            "/snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:1.7.1"
            in container["image"]
        )

        # Check environment variables
        env = container["env"]
        # Check core environment variables
        assert env["IS_REMOTE_DEV"] == "true"

        # Check Ray configuration is present
        ray_env_vars = [
            "HEAD_CLIENT_SERVER_PORT",
            "HEAD_GCS_PORT",
            "HEAD_DASHBOARD_GRPC_PORT",
            "OBJECT_MANAGER_PORT",
            "NODE_MANAGER_PORT",
            "RUNTIME_ENV_AGENT_PORT",
        ]
        for var in ray_env_vars:
            assert var in env

        # Check endpoints - should include Ray endpoints plus VS Code endpoints
        endpoints = spec["spec"]["endpoints"]
        assert len(endpoints) == 12  # 9 Ray endpoints + 3 public endpoints

        server_ui = next(
            ep for ep in endpoints if ep["name"] == SERVER_UI_ENDPOINT_NAME
        )
        assert server_ui["port"] == 12020
        assert server_ui["public"] is True

        websocket_ssh = next(
            ep for ep in endpoints if ep["name"] == WEBSOCKET_SSH_ENDPOINT_NAME
        )
        assert websocket_ssh["port"] == 12021
        assert websocket_ssh["public"] is True

    def test_generate_spec_with_custom_image_tag(self, mock_session):
        """Test generating spec with custom image tag."""
        spec = generate_service_spec(
            session=mock_session, compute_pool="test_pool", image="v1.2.3"
        )

        container = spec["spec"]["containers"][0]
        assert (
            "/snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:v1.2.3"
            in container["image"]
        )

    def test_generate_spec_with_full_image_path(self, mock_session):
        """Test generating spec with full image path."""
        spec = generate_service_spec(
            session=mock_session,
            compute_pool="test_pool",
            image="myrepo/myimage:custom",
        )

        container = spec["spec"]["containers"][0]
        assert container["image"] == "myrepo/myimage:custom"

    def test_generate_spec_with_registry_image_path(self, mock_session):
        """Test generating spec with registry/repo/image:tag format."""
        spec = generate_service_spec(
            session=mock_session,
            compute_pool="test_pool",
            image="registry.com/myrepo/myimage:v1.0",
        )

        container = spec["spec"]["containers"][0]
        assert container["image"] == "registry.com/myrepo/myimage:v1.0"

    def test_generate_spec_with_ssh_key(self, mock_session):
        """Test generating spec with SSH public key."""
        ssh_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC... test@example.com"
        spec = generate_service_spec(
            session=mock_session, compute_pool="test_pool", ssh_public_key=ssh_key
        )

        container = spec["spec"]["containers"][0]
        assert container["env"]["SSH_PUBLIC_KEY"] == ssh_key

    def test_generate_spec_with_stage(self, mock_session):
        """Test generating spec with stage mount."""
        spec = generate_service_spec(
            session=mock_session, compute_pool="test_pool", stage="@my_stage"
        )

        # Check volume mounts
        container = spec["spec"]["containers"][0]
        volume_mounts = container["volumeMounts"]

        # Should have workspace and vscode-data mounts plus system volumes
        workspace_mount = next(
            vm for vm in volume_mounts if vm["name"] == "user-workspace"
        )
        assert workspace_mount["mountPath"] == "/root/workspace"

        vscode_mount = next(
            vm for vm in volume_mounts if vm["name"] == "user-vscode-data"
        )
        assert vscode_mount["mountPath"] == "/root/.vscode-server"

        # Check volumes
        volumes = spec["spec"]["volumes"]
        workspace_volume = next(v for v in volumes if v["name"] == "user-workspace")
        assert workspace_volume["source"] == "@my_stage/user-default"

        vscode_volume = next(v for v in volumes if v["name"] == "user-vscode-data")
        assert vscode_volume["source"] == "@my_stage/.vscode-server/data"

    def test_generate_spec_with_external_access(self, mock_session):
        """Test generating spec with external access integrations."""
        # External access integrations are handled by the service manager, not in the spec
        spec = generate_service_spec(session=mock_session, compute_pool="test_pool")

        # The spec generation works normally without external access integrations
        assert "spec" in spec

    def test_generate_spec_resource_calculation(self, mock_session):
        """Test that resource requests and limits are calculated correctly."""
        spec = generate_service_spec(session=mock_session, compute_pool="test_pool")

        container = spec["spec"]["containers"][0]
        resources = container["resources"]

        # Should have both requests and limits
        assert "requests" in resources
        assert "limits" in resources

        # Test actual resource values based on CPU_X64_S (3 CPU, 13 GB memory)
        # CPU: 3 * 1000 = 3000m
        assert resources["requests"]["cpu"] == "3000m"
        assert resources["limits"]["cpu"] == "3000m"

        # Memory: 13Gi
        assert resources["requests"]["memory"] == "13Gi"
        assert resources["limits"]["memory"] == "13Gi"

        # Should not have GPU resources for CPU-only instance
        assert "nvidia.com/gpu" not in resources["requests"]
        assert "nvidia.com/gpu" not in resources["limits"]

    def test_generate_spec_memory_volume(self, mock_session):
        """Test that memory volume is correctly configured."""
        spec = generate_service_spec(session=mock_session, compute_pool="test_pool")

        # Check volume mounts
        container = spec["spec"]["containers"][0]
        volume_mounts = container["volumeMounts"]

        memory_mount = next(vm for vm in volume_mounts if vm["name"] == "dshm")
        assert memory_mount["mountPath"] == "/dev/shm"

        # Check volumes
        volumes = spec["spec"]["volumes"]
        memory_volume = next(v for v in volumes if v["name"] == "dshm")
        assert memory_volume["source"] == "memory"
        assert memory_volume["size"].endswith("Gi")

    def test_generate_spec_system_volumes(self, mock_session):
        """Test that system log volumes are correctly configured."""
        spec = generate_service_spec(session=mock_session, compute_pool="test_pool")

        # Check volume mounts
        container = spec["spec"]["containers"][0]
        volume_mounts = container["volumeMounts"]

        # Should have system and user log volumes
        system_mount = next(vm for vm in volume_mounts if vm["name"] == "system-logs")
        assert system_mount["mountPath"] == "/var/log/managedservices/system/remote"

        user_mount = next(vm for vm in volume_mounts if vm["name"] == "user-logs")
        assert user_mount["mountPath"] == "/var/log/managedservices/user/remote"

        # Check volumes
        volumes = spec["spec"]["volumes"]
        system_volume = next(v for v in volumes if v["name"] == "system-logs")
        assert system_volume["source"] == "local"

        user_volume = next(v for v in volumes if v["name"] == "user-logs")
        assert user_volume["source"] == "local"

    def test_generate_service_spec_yaml_format(self, mock_session):
        """Test that the generated spec matches expected YAML format."""
        from snowflake.cli._plugins.remote.container_spec import (
            generate_service_spec_yaml,
        )

        yaml_output = generate_service_spec_yaml(
            session=mock_session,
            compute_pool="test_pool",
            stage="@my_stage",
            ssh_public_key="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC... test@example.com",
        )

        # Verify it's valid YAML
        import yaml

        parsed_spec = yaml.safe_load(yaml_output)

        # Check basic structure
        assert "spec" in parsed_spec
        assert "containers" in parsed_spec["spec"]
        assert "volumes" in parsed_spec["spec"]
        assert "endpoints" in parsed_spec["spec"]

        # Check container details
        container = parsed_spec["spec"]["containers"][0]
        assert container["name"] == "main"
        assert (
            "/snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:1.7.1"
            in container["image"]
        )

        # Check that SSH key is in environment
        assert "SSH_PUBLIC_KEY" in container["env"]
        assert (
            container["env"]["SSH_PUBLIC_KEY"]
            == "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC... test@example.com"
        )

        # Check Ray configuration
        assert "HEAD_CLIENT_SERVER_PORT" in container["env"]
        assert container["env"]["HEAD_CLIENT_SERVER_PORT"] == "10001"

        # Check volumes include stage mounts
        volume_names = [vol["name"] for vol in parsed_spec["spec"]["volumes"]]
        assert "user-workspace" in volume_names
        assert "user-vscode-data" in volume_names
        assert "dshm" in volume_names  # memory volume

        # Check endpoints include Ray and VS Code
        endpoint_names = [ep["name"] for ep in parsed_spec["spec"]["endpoints"]]
        assert SERVER_UI_ENDPOINT_NAME in endpoint_names
        assert WEBSOCKET_SSH_ENDPOINT_NAME in endpoint_names
        assert RAY_DASHBOARD_ENDPOINT_NAME in endpoint_names
        assert "ray-client-server-endpoint" in endpoint_names

        # Verify the YAML is properly formatted
        assert yaml_output.startswith("spec:")
        assert "containers:" in yaml_output
        assert "endpoints:" in yaml_output

    def test_generate_spec_with_invalid_stage(self, mock_session):
        """Test that invalid stage paths raise ValueError."""
        # Test with empty string (should be invalid)
        with pytest.raises(
            ValueError,
            match="Stage path cannot be empty",
        ):
            generate_service_spec(
                session=mock_session,
                compute_pool="test_pool",
                stage="",  # Empty string should raise ValueError
            )

    def test_generate_spec_with_all_ray_endpoints(self, mock_session):
        """Test that all Ray endpoints are properly included."""
        from snowflake.cli._plugins.remote.constants import RAY_ENDPOINTS

        spec = generate_service_spec(session=mock_session, compute_pool="test_pool")

        endpoints = spec["spec"]["endpoints"]
        endpoint_names = [ep["name"] for ep in endpoints]

        # Check that all Ray endpoints are included
        for ray_endpoint in RAY_ENDPOINTS:
            assert ray_endpoint["name"] in endpoint_names

        # Check specific Ray endpoints
        ray_endpoint_names = [
            "ray-client-server-endpoint",
            "ray-gcs-endpoint",
            "ray-dashboard-grpc-endpoint",
            "ray-object-manager-endpoint",
            "ray-node-manager-endpoint",
            "ray-runtime-agent-endpoint",
            "ray-dashboard-agent-grpc-endpoint",
            "ephemeral-port-range",
            "ray-worker-port-range",
        ]

        for ray_name in ray_endpoint_names:
            assert ray_name in endpoint_names

    def test_generate_spec_environment_variables_complete(self, mock_session):
        """Test that all required environment variables are set."""
        from snowflake.cli._plugins.remote.constants import (
            ENABLE_REMOTE_DEV_ENV_VAR,
            RAY_ENV_VARS,
        )

        spec = generate_service_spec(session=mock_session, compute_pool="test_pool")

        container = spec["spec"]["containers"][0]
        env_vars = container["env"]

        # Check core environment variable
        assert env_vars[ENABLE_REMOTE_DEV_ENV_VAR] == "true"

        # Check all Ray environment variables are present
        for ray_env_key in RAY_ENV_VARS.keys():
            assert ray_env_key in env_vars
            assert env_vars[ray_env_key] == RAY_ENV_VARS[ray_env_key]

        # Check ML runtime health check variables
        assert "ML_RUNTIME_HEALTH_CHECK_PORT" in env_vars
        assert "ENABLE_HEALTH_CHECKS" in env_vars

    def test_generate_spec_with_gpu_resources(self, mock_session):
        """Test that GPU resources are properly handled."""
        from snowflake.cli._plugins.remote.constants import ComputeResources

        # Create GPU resources
        gpu_resources = ComputeResources(cpu=8, memory=32, gpu=2)

        # Mock the get_node_resources function in container_spec module to return GPU resources
        with patch(
            "snowflake.cli._plugins.remote.container_spec.get_node_resources",
            return_value=gpu_resources,
        ):
            spec = generate_service_spec(session=mock_session, compute_pool="gpu_pool")

            container = spec["spec"]["containers"][0]

            # Check that GPU image is used (should contain "generic_gpu")
            assert "generic_gpu" in container["image"]
            assert (
                "st_plat/runtime/x86/generic_gpu/runtime_image/snowbooks:1.7.1"
                in container["image"]
            )

            # Check GPU resources in requests and limits
            resources = container["resources"]
            assert resources["requests"]["nvidia.com/gpu"] == 2
            assert resources["limits"]["nvidia.com/gpu"] == 2

            # Check CPU and memory resources are also set correctly
            assert resources["requests"]["cpu"] == "8000m"  # 8 * 1000
            assert resources["limits"]["cpu"] == "8000m"
            assert resources["requests"]["memory"] == "32Gi"
            assert resources["limits"]["memory"] == "32Gi"
