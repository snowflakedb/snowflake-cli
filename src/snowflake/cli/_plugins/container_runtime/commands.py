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
from snowflake.cli._plugins.container_runtime.utils import (
    configure_vscode_settings,
    generate_ssh_key_pair,
    get_existing_ssh_key,
    setup_ssh_config_with_token,
)
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


@app.command("create", requires_connection=True)
def create(
    compute_pool: str = typer.Option(
        ...,  # Required parameter
        "--compute-pool",
        help="Name of the compute pool to use (required)",
    ),
    name: Optional[str] = typer.Option(
        None,
        "--name",
        help="Custom identifier for the service",
    ),
    eai_name: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="List of external access integration names to enable network access to external resources",
    ),
    stage: Optional[str] = typer.Option(
        None,
        "--stage",
        help="Internal Snowflake stage to mount (e.g., @my_stage or @my_stage/folder). Maximum 5 stage volumes per service.",
    ),
    workspace: Optional[str] = typer.Option(
        None,
        "--workspace",
        help="[COMING SOON] Workspace to mount for user files. Can be either a stage path (e.g., @my_stage/path) or a Snowflake workspace name for personal database usage. If provided, this overrides the default stage/user-default path for the workspace volume. (This feature is not yet available)",
    ),
    image_tag: Optional[str] = typer.Option(
        None,
        "--image-tag",
        help="Custom image tag to use for the container runtime environment",
    ),
    generate_ssh_key: bool = typer.Option(
        False,
        "--generate-ssh-key",
        help="Generate SSH key pair for secure authentication (recommended for production)",
    ),
    **options,
) -> None:
    """
    Creates a new VS Code Server container runtime environment.

    This command deploys a VS Code Server in a Snowpark Container Service
    that provides a web-based development environment.
    """
    cc.step("Creating container runtime environment...")

    # Check if workspace parameter is used (not yet available)
    if workspace:
        cc.step("❌ Error: The --workspace parameter is not yet available.")
        cc.step(
            "💡 This feature is under development and will be available in a future release."
        )
        cc.step("💡 For now, please use the --stage parameter for persistent storage.")
        raise typer.Exit(code=1)

    try:
        # Handle SSH key generation if requested
        ssh_public_key = None
        if generate_ssh_key:
            cc.step("🔐 Generating SSH key pair for secure authentication...")

            # Generate a unique service name to determine final key name
            temp_manager = ContainerRuntimeManager()
            if not name:
                from datetime import datetime

                from snowflake.cli.api.cli_global_context import get_cli_context

                username = get_cli_context().connection.user.lower()
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                temp_name = f"SNOW_CR_{username}_{timestamp}"
            else:
                temp_name = f"SNOW_CR_{name}"

            # Generate SSH key pair
            private_key_path, ssh_public_key = generate_ssh_key_pair(temp_name)
            cc.step(f"🔑 SSH key pair generated successfully")
            cc.step(f"   Service will be configured for secure SSH key authentication")

        manager = ContainerRuntimeManager()
        service_name, url, was_created = manager.create(
            name=name,
            compute_pool=compute_pool,
            external_access=eai_name,
            stage=stage,
            workspace=workspace,
            image_tag=image_tag,
            ssh_public_key=ssh_public_key,
        )

        # Display essential information only
        if was_created:
            cc.step(
                f"✓ Container Runtime Environment {service_name} created successfully!"
            )
        else:
            cc.step(f"✓ Container Runtime Environment {service_name} already exists!")

        cc.step(f"VS Code Server URL: {url}")

        # Log detailed information at debug level
        if stage:
            log.debug("Stage '%s' mounted:", stage)
            log.debug(
                "  - Workspace: '%s/user-default' → '%s'",
                stage,
                constants.USER_WORKSPACE_VOLUME_MOUNT_PATH,
            )
            log.debug(
                "  - VS Code data: '%s/.vscode-server/data' → '%s'",
                stage,
                constants.USER_VSCODE_DATA_VOLUME_MOUNT_PATH,
            )
        if eai_name:
            log.debug("External access integrations: %s", ", ".join(eai_name))
        if image_tag:
            log.debug("Using custom image tag: %s", image_tag)
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
    vscode_server_path: Optional[str] = typer.Option(
        None,
        "--vscode-server-path",
        help="Path where VS Code server should be installed on the remote container (if not specified, VS Code will use default behavior)",
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
    5. Configures VS Code Remote SSH settings for optimal container integration

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
        # Get the websocket-ssh endpoint URL
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

        # Handle SSH key authentication setup
        private_key_path = None
        ssh_key_result = get_existing_ssh_key(name)

        if ssh_key_result:
            private_key_path, public_key = ssh_key_result
            cc.step(f"🔑 Found existing SSH key pair for service '{name}'")
            cc.step(f"   Using SSH key authentication for enhanced security")
        else:
            cc.step(f"💡 Using token-only authentication (less secure)")
            cc.step(
                f"💡 For enhanced security, recreate the container with --generate-ssh-key"
            )

        # Configure VS Code settings before starting the token management loop
        if vscode_server_path:
            cc.step("🎨 Configuring VS Code Remote SSH settings...")
            configure_vscode_settings(name, vscode_server_path)
        else:
            cc.step(
                "🎨 VS Code Remote SSH settings are already configured or will be configured by default."
            )

        # Ensure session has the correct format for token to work properly
        cc.step("🔧 Configuring session for SSH token compatibility...")

        # Configure session format for token requests
        manager.snowpark_session.sql(
            "ALTER SESSION SET python_connector_query_result_format = 'JSON'"
        ).collect()

        cc.step(f"🚀 Starting SSH session management for container runtime '{name}'...")
        cc.step(f"🔄 Connection refresh interval: {refresh_interval} seconds")
        cc.step(
            "💡 Fresh connections will be created proactively every refresh cycle to ensure token validity"
        )
        if vscode_server_path:
            cc.step(f"📁 VS Code server path: {vscode_server_path}")
        cc.step(f"💡 You can now connect using: ssh snowflake-remote-runtime-{name}")
        cc.step(f"⏹️  Press Ctrl+C to stop this command and end SSH session management")
        cc.step("=" * 70)

        # Keep track of the current connection for token refresh
        current_connection = manager.snowpark_session.connection
        token_refresh_count = 0

        def get_fresh_token():
            """Proactively create a fresh connection and get token every refresh cycle."""
            nonlocal current_connection

            try:
                cc.step("🔄 Creating fresh connection for token refresh...")

                # Import necessary modules
                from snowflake.cli._app.snow_connector import connect_to_snowflake
                from snowflake.cli.api.cli_global_context import get_cli_context

                # Get current connection context
                current_context = get_cli_context().connection_context

                # Always create a fresh connection proactively
                # The old connection will auto-close when it goes out of scope
                fresh_connection = connect_to_snowflake(
                    connection_name=current_context.connection_name,
                    temporary_connection=current_context.temporary_connection,
                )

                # Update our current connection reference
                current_connection = fresh_connection

                current_connection.cursor().execute(
                    "ALTER SESSION SET python_connector_query_result_format = 'JSON'"
                )

                # Get token from the fresh connection
                token = fresh_connection.rest.token
                if token:
                    cc.step("✅ Fresh connection created, token obtained")
                    return token
                else:
                    raise Exception("No token available from fresh connection")

            except Exception as e:
                cc.step(f"❌ Failed to create fresh connection and get token: {str(e)}")
                return None

        while not shutdown_requested:
            try:
                # Get a fresh session token
                cc.step(
                    f"🔑 Creating fresh connection and getting token... (refresh #{token_refresh_count + 1})"
                )

                token = get_fresh_token()
                if not token:
                    cc.step(
                        "❌ Unable to create fresh connection or get token. Please ensure you are properly authenticated."
                    )
                    cc.step("🔄 Will retry in 30 seconds...")

                    # Wait 30 seconds before retrying, but check for shutdown every second
                    retry_end_time = time.time() + 30
                    while time.time() < retry_end_time and not shutdown_requested:
                        time.sleep(1)
                    continue

                # Update SSH configuration with current token
                if private_key_path:
                    cc.step(
                        f"🔧 Updating SSH configuration with fresh token and SSH key... (refresh #{token_refresh_count + 1})"
                    )
                else:
                    cc.step(
                        f"🔧 Updating SSH configuration with fresh token... (refresh #{token_refresh_count + 1})"
                    )
                setup_ssh_config_with_token(
                    name, ssh_endpoint_url, token, private_key_path
                )

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
                cc.step(f"⚠️  Unexpected error during SSH setup: {str(e)}")
                cc.step(f"🔄 Will retry in 30 seconds...")

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
