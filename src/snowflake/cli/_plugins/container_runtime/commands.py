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
import signal
import time
from typing import List, Optional

import typer
from snowflake.cli._plugins.container_runtime import constants
from snowflake.cli._plugins.container_runtime.manager import ContainerRuntimeManager
from snowflake.cli._plugins.container_runtime.utils import setup_ssh_config_with_token
from snowflake.cli.api.commands.flags import identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    QueryResult,
    SingleQueryResult,
)

app = SnowTyperFactory(
    name="container-runtime",
    help="Manages Snowpark Container Services Runtime Environment with VS Code Server.",
    short_help="Manages container runtime environment.",
)

log = logging.getLogger(__name__)

ContainerRuntimeNameArgument = identifier_argument(
    sf_object="container runtime",
    example="SNOW_CR_username_20240411123456",
)

# Define command options
NameOption = typer.Option(
    None,
    "--name",
    help="Custom identifier for the service",
)

ComputePoolOption = typer.Option(
    None,
    "--compute-pool",
    help="Name of the compute pool to use",
)

PersistentStorageOption = typer.Option(
    False,
    "--persistent-storage/--no-persistent-storage",
    help="Enable persistent storage",
    is_flag=True,
)

StorageSizeOption = typer.Option(
    10,
    "--storage-size",
    help="Size of persistent storage (in GB)",
)

ExternalAccessOption = typer.Option(
    False,
    "--external-access/--no-external-access",
    help="Allow network access to external resources",
    is_flag=True,
)

TimeoutOption = typer.Option(
    60,
    "--timeout",
    help="Session timeout in minutes",
)

ExtensionsOption = typer.Option(
    None,
    "--extensions",
    help="Comma-separated list of VS Code extensions to pre-install",
)

StageOption = typer.Option(
    None,
    "--stage",
    help="Internal Snowflake stage to mount (e.g., @my_stage or @my_stage/folder). Maximum 5 stage volumes per service.",
)

StageMountPathOption = typer.Option(
    constants.USER_STAGE_VOLUME_MOUNT_PATH,
    "--stage-mount-path",
    help="Path where the stage will be mounted in the container",
)


@app.command("create", requires_connection=True)
def create(
    name: str = NameOption,
    compute_pool: str = ComputePoolOption,
    persistent_storage: bool = PersistentStorageOption,
    storage_size: int = StorageSizeOption,
    external_access: bool = ExternalAccessOption,
    timeout: int = TimeoutOption,
    extensions: Optional[List[str]] = ExtensionsOption,
    stage: Optional[str] = StageOption,
    stage_mount_path: str = StageMountPathOption,
    **options,
) -> None:
    """
    Creates a new VS Code Server container runtime environment.

    This command deploys a VS Code Server in a Snowpark Container Service
    that provides a web-based development environment.
    """
    cc.step("Creating container runtime environment...")

    try:
        # Split extensions if provided as comma-separated string
        ext_list = None
        if extensions:
            if isinstance(extensions, str):
                ext_list = [ext.strip() for ext in extensions.split(",")]
            else:
                ext_list = extensions

        manager = ContainerRuntimeManager()
        url = manager.create(
            name=name,
            compute_pool=compute_pool,
            persistent_storage=persistent_storage,
            storage_size=storage_size,
            external_access=external_access,
            timeout=timeout,
            extensions=ext_list,
            stage=stage,
            stage_mount_path=stage_mount_path,
        )

        # Display success message with the endpoint URL
        cc.step("✓ Container Runtime Environment created successfully!")
        cc.step(f"Access your VS Code Server at: {url}")
        cc.step(f"Default password: password")
        cc.step(f"Session timeout: {timeout} minutes")
        if stage:
            cc.step(f"Stage '{stage}' mounted at: {stage_mount_path}")
    except Exception as e:
        cc.step(f"Error: {str(e)}")
        raise typer.Exit(code=1)


@app.command("list", requires_connection=True)
def list_runtimes(**options) -> CommandResult:
    """
    Lists all container runtime environments.
    """
    cursor = ContainerRuntimeManager().list_services()
    return QueryResult(cursor)


@app.command("stop", requires_connection=True)
def stop(
    name: str = ContainerRuntimeNameArgument,
    **options,
) -> CommandResult:
    """
    Suspends a container runtime environment.
    """
    manager = ContainerRuntimeManager()
    cursor = manager.stop(name)
    cc.step(f"Container runtime '{name}' suspended successfully.")
    return SingleQueryResult(cursor)


@app.command("start", requires_connection=True)
def start(
    name: str = ContainerRuntimeNameArgument,
    **options,
) -> CommandResult:
    """
    Resumes a suspended container runtime environment.
    """
    manager = ContainerRuntimeManager()
    cursor = manager.start(name)

    cc.step(f"Starting container runtime '{name}'...")
    # Give it some time to start
    time.sleep(5)
    try:
        manager.wait_for_service_ready(name)
        url = manager.get_service_endpoint_url(name)
        cc.step(f"Container runtime '{name}' started successfully.")
        cc.step(f"Access URL: {url}")
    except Exception as e:
        cc.step(f"Service started but not yet fully ready: {str(e)}")
        cc.step(f"Please wait a few more moments and check the service status.")

    return SingleQueryResult(cursor)


@app.command("delete", requires_connection=True)
def delete(
    name: str = ContainerRuntimeNameArgument,
    **options,
) -> CommandResult:
    """
    Deletes a container runtime environment.
    """
    manager = ContainerRuntimeManager()
    cursor = manager.delete(name)
    cc.step(f"Container runtime '{name}' deleted successfully.")
    return SingleQueryResult(cursor)


@app.command("get-url", requires_connection=True)
def get_url(
    name: str = ContainerRuntimeNameArgument,
    **options,
) -> CommandResult:
    """
    Gets the public endpoint URLs for a container runtime environment.
    """
    manager = ContainerRuntimeManager()

    try:
        urls = manager.get_public_endpoint_urls(name)

        if not urls:
            cc.step(f"No public endpoints found for container runtime '{name}'.")
            return SingleQueryResult(None)

        # Format URLs as a list of dictionaries for better output
        formatted_urls = [
            {"endpoint_name": endpoint_name, "url": url}
            for endpoint_name, url in urls.items()
        ]

        return CollectionResult(formatted_urls)

    except Exception as e:
        cc.step(f"Error retrieving URLs for container runtime '{name}': {str(e)}")
        raise typer.Exit(code=1)


@app.command("setup-ssh", requires_connection=True)
def setup_ssh(
    name: str = ContainerRuntimeNameArgument,
    refresh_interval: int = typer.Option(
        300,  # 5 minutes default
        "--refresh-interval",
        help="Token refresh interval in seconds (default: 300 seconds / 5 minutes)",
    ),
    **options,
) -> None:
    """
    Sets up SSH configuration for connecting to a container runtime environment over WebSocket.

    This command:
    1. Checks if websocat is installed (installs it on macOS if needed)
    2. Gets the websocket-ssh endpoint URL for the specified container runtime
    3. Gets the current session token and keeps it refreshed
    4. Continuously updates your SSH config with the latest token

    This is a blocking command that keeps the session alive and refreshes tokens automatically.
    Press Ctrl+C to stop the command and terminate the SSH session management.

    After running this command, you can SSH to the container using:
    ssh snowflake-remote-runtime-{name}
    """
    manager = ContainerRuntimeManager()

    # Flag to handle graceful shutdown
    shutdown_requested = False

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        cc.step("\n🛑 Shutdown requested. Cleaning up SSH configuration...")
        shutdown_requested = True

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Get the websocket-ssh endpoint URL (this should be done once)
        cc.step(f"🔍 Getting endpoint information for container runtime '{name}'...")
        urls = manager.get_public_endpoint_urls(name)

        if not urls:
            cc.step(f"❌ No public endpoints found for container runtime '{name}'.")
            raise typer.Exit(code=1)

        ssh_endpoint_url = urls.get("websocket-ssh")
        if not ssh_endpoint_url:
            cc.step(
                f"❌ No websocket-ssh endpoint found for container runtime '{name}'."
            )
            cc.step("Available endpoints:")
            for endpoint_name, url in urls.items():
                cc.step(f"  - {endpoint_name}: {url}")
            raise typer.Exit(code=1)
        ssh_endpoint_url = f"wss://{ssh_endpoint_url}"

        cc.step(f"✅ Found websocket SSH endpoint: {ssh_endpoint_url}")

        # Ensure session has the correct format for token to work properly
        cc.step("🔧 Configuring session for SSH token compatibility...")
        manager.snowpark_session.sql(
            "ALTER SESSION SET python_connector_query_result_format = 'JSON'"
        ).collect()

        cc.step(f"🚀 Starting SSH token management for container runtime '{name}'...")
        cc.step(f"🔄 Token refresh interval: {refresh_interval} seconds")
        cc.step(f"💡 You can now connect using: ssh snowflake-remote-runtime-{name}")
        cc.step(f"⏹️  Press Ctrl+C to stop this command and end SSH session management")
        cc.step("=" * 70)

        token_refresh_count = 0

        while not shutdown_requested:
            try:
                # Get the current session token
                token = manager.snowpark_session.connection.rest.token
                if not token:
                    cc.step(
                        "❌ Unable to get session token. Please ensure you are properly authenticated."
                    )
                    raise typer.Exit(code=1)

                # Update SSH configuration with current token
                cc.step(
                    f"🔑 Updating SSH configuration with fresh token... (refresh #{token_refresh_count + 1})"
                )
                setup_ssh_config_with_token(name, ssh_endpoint_url, token)

                token_refresh_count += 1
                next_refresh_time = time.time() + refresh_interval

                cc.step(f"✅ SSH configuration updated successfully!")
                cc.step(f"⏰ Next token refresh in {refresh_interval} seconds...")

                # Sleep in small intervals to allow for responsive shutdown
                while time.time() < next_refresh_time and not shutdown_requested:
                    remaining_time = int(next_refresh_time - time.time())
                    if (
                        remaining_time > 0 and remaining_time % 30 == 0
                    ):  # Show countdown every 30 seconds
                        cc.step(
                            f"⏳ Next refresh in {remaining_time} seconds... (Press Ctrl+C to stop)"
                        )
                    time.sleep(1)

            except KeyboardInterrupt:
                # This should be caught by the signal handler, but just in case
                shutdown_requested = True
                break
            except Exception as e:
                cc.step(f"⚠️  Error during token refresh: {str(e)}")
                cc.step(f"🔄 Retrying in 30 seconds...")

                # Wait 30 seconds before retrying, but check for shutdown every second
                retry_end_time = time.time() + 30
                while time.time() < retry_end_time and not shutdown_requested:
                    time.sleep(1)

        if shutdown_requested:
            cc.step("🧹 Performing cleanup...")
            # Note: We intentionally leave the SSH config in place so the last valid token can still be used
            # for a short period. The config will be updated when the command is run again.
            cc.step(f"✅ SSH session management stopped.")
            cc.step(
                f"💡 The last valid token is still in your SSH config and may work for a short time."
            )
            cc.step(f"💡 Run this command again to resume SSH session management.")

        return None

    except Exception as e:
        cc.step(f"❌ Error setting up SSH for container runtime '{name}': {str(e)}")
        raise typer.Exit(code=1)
