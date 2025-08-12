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

import time
import uuid
from typing import Optional
import pytest


@pytest.mark.integration
def test_container_runtime_lifecycle(runner, test_database):
    """Test the complete lifecycle of a container runtime environment."""
    # Generate unique service name
    unique_id = str(uuid.uuid4())[:8]
    runtime_name = f"test_runtime_{unique_id}"

    try:
        # Create container runtime
        result = runner.invoke_with_connection(
            [
                "container-runtime",
                "create",
                "--compute-pool",
                "CONTAINER_RUNTIME_POOL",
                "--name",
                runtime_name,
            ]
        )

        if result.exit_code != 0:
            # Check if it's a compute pool issue
            if "does not exist or not authorized" in result.output:
                pytest.skip(
                    "CONTAINER_RUNTIME_POOL not available for integration tests"
                )
            elif "unschedulable" in result.output:
                pytest.skip("Compute pool has no available resources")

        assert result.exit_code == 0, result.output
        assert "✓ Container Runtime Environment created successfully!" in result.output
        assert "Access your VS Code Server at:" in result.output

        # List container runtimes - should show our newly created runtime
        result = runner.invoke_with_connection(["container-runtime", "list"])
        assert result.exit_code == 0, result.output
        # The output format may vary, but should contain our runtime name
        assert f"SNOW_CR_{runtime_name}" in result.output

        # Get URLs for the runtime
        result = runner.invoke_with_connection(
            ["container-runtime", "get-url", f"SNOW_CR_{runtime_name}"]
        )
        assert result.exit_code == 0, result.output

        # Stop the runtime
        result = runner.invoke_with_connection(
            ["container-runtime", "stop", f"SNOW_CR_{runtime_name}"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Container runtime 'SNOW_CR_{runtime_name}' suspended successfully."
            in result.output
        )

        # Start the runtime again
        result = runner.invoke_with_connection(
            ["container-runtime", "start", f"SNOW_CR_{runtime_name}"]
        )
        assert result.exit_code == 0, result.output
        assert (
            f"Starting container runtime 'SNOW_CR_{runtime_name}'..." in result.output
        )

        # The start command might not wait for full readiness in CI
        if "started successfully" in result.output:
            assert "Access URL:" in result.output
        elif "Service started but not yet fully ready" in result.output:
            # This is acceptable in CI environments
            pass

    finally:
        # Cleanup: Delete the runtime
        result = runner.invoke_with_connection(
            ["container-runtime", "delete", f"SNOW_CR_{runtime_name}"]
        )
        # Don't assert on cleanup - it might fail if creation failed
        if result.exit_code == 0:
            assert (
                f"Container runtime 'SNOW_CR_{runtime_name}' deleted successfully."
                in result.output
            )


@pytest.mark.integration
def test_container_runtime_create_with_stage(runner, test_database):
    """Test creating container runtime with a stage mount."""
    unique_id = str(uuid.uuid4())[:8]
    runtime_name = f"test_runtime_stage_{unique_id}"
    stage_name = f"test_stage_{unique_id}"

    try:
        # First create a stage for testing
        result = runner.invoke_with_connection(
            ["sql", "-q", f"CREATE STAGE {stage_name}"]
        )
        assert result.exit_code == 0, result.output

        # Create container runtime with stage
        result = runner.invoke_with_connection(
            [
                "container-runtime",
                "create",
                "--compute-pool",
                "CONTAINER_RUNTIME_POOL",
                "--name",
                runtime_name,
                "--stage",
                f"@{stage_name}",
            ]
        )

        if result.exit_code != 0:
            if (
                "does not exist or not authorized" in result.output
                or "unschedulable" in result.output
            ):
                pytest.skip(
                    "CONTAINER_RUNTIME_POOL not available for integration tests"
                )

        assert result.exit_code == 0, result.output
        assert "✓ Container Runtime Environment created successfully!" in result.output
        assert f"Stage '@{stage_name}' mounted:" in result.output

    finally:
        # Cleanup
        runner.invoke_with_connection(
            ["container-runtime", "delete", f"SNOW_CR_{runtime_name}"]
        )
        runner.invoke_with_connection(
            ["sql", "-q", f"DROP STAGE IF EXISTS {stage_name}"]
        )


@pytest.mark.integration
def test_container_runtime_create_with_external_access(runner, test_database):
    """Test creating container runtime with external access integration."""
    unique_id = str(uuid.uuid4())[:8]
    runtime_name = f"test_runtime_eai_{unique_id}"

    try:
        # Create container runtime with external access
        result = runner.invoke_with_connection(
            [
                "container-runtime",
                "create",
                "--compute-pool",
                "CONTAINER_RUNTIME_POOL",
                "--name",
                runtime_name,
                "--eai-name",
                "ALLOW_ALL_INTEGRATION",
            ]
        )

        if result.exit_code != 0:
            if (
                "does not exist or not authorized" in result.output
                or "unschedulable" in result.output
            ):
                pytest.skip(
                    "CONTAINER_RUNTIME_POOL not available for integration tests"
                )

        assert result.exit_code == 0, result.output
        assert "✓ Container Runtime Environment created successfully!" in result.output
        assert "External access integrations: ALLOW_ALL_INTEGRATION" in result.output

    finally:
        # Cleanup
        runner.invoke_with_connection(
            ["container-runtime", "delete", f"SNOW_CR_{runtime_name}"]
        )


@pytest.mark.integration
def test_container_runtime_create_invalid_compute_pool(runner, test_database):
    """Test creating container runtime with invalid compute pool."""
    result = runner.invoke_with_connection(
        ["container-runtime", "create", "--compute-pool", "NONEXISTENT_POOL"]
    )

    assert result.exit_code == 1
    # Should get an error about the compute pool not existing
    assert (
        "does not exist" in result.output
        or "not authorized" in result.output
        or "Error:" in result.output
    )


@pytest.mark.integration
def test_container_runtime_create_invalid_stage(runner, test_database):
    """Test creating container runtime with invalid stage path."""
    result = runner.invoke_with_connection(
        [
            "container-runtime",
            "create",
            "--compute-pool",
            "CONTAINER_RUNTIME_POOL",
            "--stage",
            "invalid_stage_path",
        ]
    )

    assert result.exit_code == 1
    assert "Error:" in result.output


@pytest.mark.integration
def test_container_runtime_operations_on_nonexistent_service(runner, test_database):
    """Test operations on a non-existent container runtime."""
    nonexistent_name = "SNOW_CR_nonexistent_service"

    # Try to get URLs for non-existent service
    result = runner.invoke_with_connection(
        ["container-runtime", "get-url", nonexistent_name]
    )
    # This might succeed with empty results or fail - both are acceptable

    # Try to stop non-existent service
    result = runner.invoke_with_connection(
        ["container-runtime", "stop", nonexistent_name]
    )
    # This might succeed (SPCS is idempotent) or fail - both are acceptable

    # Try to start non-existent service
    result = runner.invoke_with_connection(
        ["container-runtime", "start", nonexistent_name]
    )
    # This should likely fail since we can't start what doesn't exist

    # Try to delete non-existent service
    result = runner.invoke_with_connection(
        ["container-runtime", "delete", nonexistent_name]
    )
    # This should succeed (DROP IF EXISTS)
    assert result.exit_code == 0


@pytest.mark.integration
def test_container_runtime_setup_ssh_no_endpoints(runner, test_database):
    """Test SSH setup on a service with no endpoints."""
    result = runner.invoke_with_connection(
        [
            "container-runtime",
            "setup-ssh",
            "SNOW_CR_nonexistent",
            "--refresh-interval",
            "1",  # Short interval for testing
        ]
    )

    # Should fail because the service doesn't exist or has no endpoints
    assert result.exit_code == 1
    assert (
        "No public endpoints found" in result.output
        or "No websocket-ssh endpoint found" in result.output
        or "Error setting up SSH" in result.output
    )


@pytest.mark.integration
def test_container_runtime_workspace_parameter_disabled(runner, test_database):
    """Test that workspace parameter shows appropriate error message."""
    result = runner.invoke_with_connection(
        [
            "container-runtime",
            "create",
            "--compute-pool",
            "CONTAINER_RUNTIME_POOL",
            "--workspace",
            "my_workspace",
        ]
    )

    assert result.exit_code == 1
    assert "❌ Error: The --workspace parameter is not yet available." in result.output
    assert "This feature is under development" in result.output


@pytest.mark.integration
def test_container_runtime_list_empty(runner, test_database):
    """Test listing container runtimes when none exist."""
    result = runner.invoke_with_connection(["container-runtime", "list"])

    # Should succeed even if no container runtimes exist
    assert result.exit_code == 0


@pytest.mark.integration
def test_container_runtime_create_auto_generated_name(runner, test_database):
    """Test creating container runtime with auto-generated name."""
    service_name = None

    try:
        # Create container runtime without specifying name
        result = runner.invoke_with_connection(
            ["container-runtime", "create", "--compute-pool", "CONTAINER_RUNTIME_POOL"]
        )

        if result.exit_code != 0:
            if (
                "does not exist or not authorized" in result.output
                or "unschedulable" in result.output
            ):
                pytest.skip(
                    "CONTAINER_RUNTIME_POOL not available for integration tests"
                )

        assert result.exit_code == 0, result.output
        assert "✓ Container Runtime Environment created successfully!" in result.output

        # Extract the service name from the output
        lines = result.output.split("\n")
        for line in lines:
            if "Using service name:" in line:
                service_name = line.split("Using service name: ")[1].strip()
                break

        assert service_name is not None, "Could not extract service name from output"
        assert service_name.startswith("SNOW_CR_")

        # Verify the service exists in the list
        result = runner.invoke_with_connection(["container-runtime", "list"])
        assert result.exit_code == 0
        assert service_name in result.output

    finally:
        # Cleanup
        if service_name:
            runner.invoke_with_connection(["container-runtime", "delete", service_name])


@pytest.mark.integration
def test_container_runtime_create_with_custom_image_tag(runner, test_database):
    """Test creating container runtime with custom image tag."""
    unique_id = str(uuid.uuid4())[:8]
    runtime_name = f"test_runtime_image_{unique_id}"

    try:
        # Create container runtime with custom image tag
        result = runner.invoke_with_connection(
            [
                "container-runtime",
                "create",
                "--compute-pool",
                "CONTAINER_RUNTIME_POOL",
                "--name",
                runtime_name,
                "--image-tag",
                "custom:latest",
            ]
        )

        if result.exit_code != 0:
            if (
                "does not exist or not authorized" in result.output
                or "unschedulable" in result.output
            ):
                pytest.skip(
                    "CONTAINER_RUNTIME_POOL not available for integration tests"
                )

        assert result.exit_code == 0, result.output
        assert "✓ Container Runtime Environment created successfully!" in result.output
        assert "Using custom image tag: custom:latest" in result.output

    finally:
        # Cleanup
        runner.invoke_with_connection(
            ["container-runtime", "delete", f"SNOW_CR_{runtime_name}"]
        )
