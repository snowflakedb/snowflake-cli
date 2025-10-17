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

import enum
import logging
from typing import List, Optional

import typer
from snowflake.cli._plugins.remote.constants import ServiceResult
from snowflake.cli._plugins.remote.manager import RemoteManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    QueryResult,
    SingleQueryResult,
)

app = SnowTyperFactory(
    name="remote",
    help="Manages remote development environments on top of Snowpark Container Service.",
    short_help="Manages remote development environments.",
)

log = logging.getLogger(__name__)

# Define argument for remote service names (accepts both customer names and full service names)
RemoteNameArgument = typer.Argument(
    help="Remote service name. Can be either a customer name (e.g., 'myproject') or full service name (e.g., 'SNOW_REMOTE_admin_myproject')",
    show_default=False,
)


class SSHMode(enum.Enum):
    """SSH connection modes for remote services."""

    SSH = "ssh"  # SSH terminal access only
    CODE = "code"  # VS Code IDE connection
    CURSOR = "cursor"  # Cursor IDE connection


# Shared arguments for remote service commands
ServiceNameArgument = typer.Argument(
    None,
    help="Service name to use, or leave empty to create a new service with auto-generated name",
)

ComputePoolOption = typer.Option(
    None,
    "--compute-pool",
    help="Name of the compute pool to use (required for new service creation)",
    show_default=False,
)

EAINameOption = typer.Option(
    None,
    "--eai-name",
    help="List of external access integration names to enable network access to external resources",
)

StageOption = typer.Option(
    None,
    "--stage",
    help="Internal Snowflake stage to mount for persistent storage. "
    "The stage will be mounted at two locations: "
    "1) 'stage/user-default' -> '/root/user-default' (workspace files), "
    "2) 'stage/.vscode-server/data' -> '/root/.vscode-server/data' (VS Code settings). "
    "Example: --stage @my_stage or --stage @my_stage/project",
)

ImageOption = typer.Option(
    None,
    "--image",
    help="Custom image to use (can be full path like 'repo/image:tag' or just tag like '1.7.1')",
)

SSHOption = typer.Option(
    False,
    "--ssh",
    help="Set up SSH configuration for connecting to the remote environment. This is a blocking command that keeps SSH connections alive.",
)

NoSSHKeyOption = typer.Option(
    False,
    "--no-ssh-key",
    help="Skip SSH key generation and use token-only authentication (less secure)",
)


def _display_service_result(
    service_name: str,
    url: str,
    status: str,
    stage: Optional[str] = None,
    eai_name: Optional[List[str]] = None,
    image: Optional[str] = None,
) -> None:
    """Display service operation results and log detailed information.

    Args:
        service_name: Full name of the service
        url: VS Code Server URL
        status: Service operation status (created, resumed, running)
        stage: Optional stage mount path
        eai_name: Optional list of external access integrations
        image: Optional custom image
    """
    # Display appropriate success message based on what happened
    if status == ServiceResult.CREATED.value:
        cc.message(
            f"✓ Remote Development Environment {service_name} created successfully!"
        )
    elif status == ServiceResult.RESUMED.value:
        cc.message(
            f"✓ Remote Development Environment {service_name} resumed successfully!"
        )
    elif status == ServiceResult.RUNNING.value:
        cc.message(
            f"✓ Remote Development Environment {service_name} is already running."
        )

    cc.message(f"VS Code Server URL: {url}")

    # Log detailed information at debug level
    if stage:
        cc.message(f"Stage '{stage}' mounted:")
        cc.message(f"  - Workspace: '{stage}/user-default' -> '/root/user-default'")
        cc.message(
            f"  - VS Code data: '{stage}/.vscode-server/data' -> '/root/.vscode-server/data'"
        )
    if eai_name:
        log.debug("External access integrations: %s", ", ".join(eai_name))
    if image:
        log.debug("Using custom image: %s", image)


def _handle_remote_service(
    name: Optional[str],
    compute_pool: Optional[str],
    eai_name: Optional[List[str]],
    stage: Optional[str],
    image: Optional[str],
    no_ssh_key: bool,
    ssh_mode: Optional[SSHMode] = None,
    ide_message: Optional[str] = None,
    validate_ide: bool = False,
) -> None:
    """Handle remote service operations with common workflow.

    Args:
        name: Service name
        compute_pool: Compute pool name
        eai_name: External access integration names
        stage: Stage mount path
        image: Custom image
        no_ssh_key: Whether to skip SSH key generation
        ssh_mode: SSH setup mode (None for no SSH, or SSHMode enum value)
        ide_message: Additional message for IDE commands
        validate_ide: Whether to validate IDE requirements
    """
    try:
        manager = RemoteManager()

        # Validate IDE requirements if needed
        if validate_ide:
            manager.validate_ide_requirements(name, eai_name)

        # Start the service
        service_name, url, status = manager.start(
            name=name,
            compute_pool=compute_pool,
            external_access=eai_name,
            stage=stage,
            image=image,
            generate_ssh_key=(ssh_mode is not None and not no_ssh_key),
        )

        # Display service result
        _display_service_result(service_name, url, status, stage, eai_name, image)

        # Display IDE-specific message if provided
        if ide_message:
            cc.message(ide_message)

        # Handle SSH/IDE setup based on mode
        if ssh_mode == SSHMode.SSH:
            manager.setup_ssh_connection(service_name)
        elif ssh_mode in (SSHMode.CODE, SSHMode.CURSOR):
            manager.setup_ssh_connection(service_name, ide=ssh_mode.value)

    except ValueError as e:
        raise CliError(f"Error: {e}")
    except Exception as e:
        if ssh_mode is None:
            context = "remote environment"
        else:
            error_context = {
                SSHMode.SSH: "remote environment",
                SSHMode.CODE: "remote environment with VS Code",
                SSHMode.CURSOR: "remote environment with Cursor",
            }
            context = error_context.get(ssh_mode, "remote environment")
        raise CliError(f"Error starting {context}: {e}")


@app.command("start", requires_connection=True)
def start(
    name: Optional[str] = ServiceNameArgument,
    compute_pool: Optional[str] = ComputePoolOption,
    eai_name: Optional[List[str]] = EAINameOption,
    stage: Optional[str] = StageOption,
    image: Optional[str] = ImageOption,
    ssh: bool = SSHOption,
    no_ssh_key: bool = NoSSHKeyOption,
    **options,
) -> None:
    """
    Starts a remote development environment.

    This command creates a new VS Code Server remote development environment if it doesn't exist,
    or starts an existing one if it's suspended. If the environment is already running, it's a no-op.
    The environment is deployed as a Snowpark Container Service that provides
    a web-based development environment.

    Usage examples:
    - Resume existing service: snow remote start myproject
    - Create new service: snow remote start --compute-pool my_pool
    - Create named service: snow remote start myproject --compute-pool my_pool
    - Start with persistent storage: snow remote start --compute-pool my_pool --stage @my_stage
    - Start with SSH setup: snow remote start myproject --ssh
    - Start with SSH (no key): snow remote start myproject --ssh --no-ssh-key

    The --compute-pool parameter is only required when creating a new service. For resuming
    existing services, the compute pool is not needed.

    Stage Mounting:
    When using --stage, the specified stage is mounted at two locations for persistence:
    - Workspace files: @stage/user-default -> /root/user-default
    - VS Code settings: @stage/.vscode-server/data -> /root/.vscode-server/data
    This ensures your code and VS Code configuration persist across service restarts.

    SSH Options:
    - Use --ssh to set up SSH configuration for secure terminal access
    - Use --no-ssh-key with --ssh for token-only authentication (less secure)
    - SSH setup is a blocking command that continuously refreshes authentication tokens

    IDE Connection:
    - Use 'snow remote code <name>' to start and connect with VS Code
    - Use 'snow remote cursor <name>' to start and connect with Cursor
    """
    ssh_mode = SSHMode.SSH if ssh else None
    _handle_remote_service(
        name=name,
        compute_pool=compute_pool,
        eai_name=eai_name,
        stage=stage,
        image=image,
        no_ssh_key=no_ssh_key,
        ssh_mode=ssh_mode,
    )


@app.command("list", requires_connection=True)
def list_services(**options) -> CommandResult:
    """
    Lists all remote development environments.
    """
    cursor = RemoteManager().list_services()
    return QueryResult(cursor)


@app.command("info", requires_connection=True)
def info(
    name: str = RemoteNameArgument,
    **options,
) -> CommandResult:
    """
    Shows detailed information about a remote development environment.

    Displays comprehensive information including service status, configuration,
    compute resources, external access integrations, public endpoints, and timestamps.
    """
    service_info = RemoteManager().get_service_info(name)

    # Format the information for display
    output_lines = []

    for section_name, section_data in service_info.items():
        output_lines.append(f"\n{section_name}:")
        output_lines.append("=" * (len(section_name) + 1))

        for key, value in section_data.items():
            # Format the value for better display
            if value is None or value == "":
                formatted_value = "N/A"
            else:
                formatted_value = str(value)

            output_lines.append(f"  {key}: {formatted_value}")

    return MessageResult("\n".join(output_lines))


@app.command("stop", requires_connection=True)
def stop(
    name: str = RemoteNameArgument,
    **options,
) -> CommandResult:
    """
    Suspends a remote development environment.
    """
    manager = RemoteManager()
    cursor = manager.stop(name)
    return SingleQueryResult(cursor)


@app.command("delete", requires_connection=True)
def delete(
    name: str = RemoteNameArgument,
    **options,
) -> CommandResult:
    """
    Deletes a remote development environment.
    """
    manager = RemoteManager()
    cursor = manager.delete(name)
    return SingleQueryResult(cursor)


@app.command("code", requires_connection=True)
def code(
    name: Optional[str] = ServiceNameArgument,
    compute_pool: Optional[str] = ComputePoolOption,
    eai_name: Optional[List[str]] = EAINameOption,
    stage: Optional[str] = StageOption,
    image: Optional[str] = ImageOption,
    no_ssh_key: bool = NoSSHKeyOption,
    **options,
) -> None:
    """
    Start a remote development environment and open VS Code connected to it.

    This command creates a new VS Code Server remote development environment if it doesn't exist,
    or starts an existing one if it's suspended, then opens VS Code connected to the remote service over SSH.

    Usage examples:
    - Connect to existing service: snow remote code myproject
    - Create new service and connect: snow remote code --compute-pool my_pool --eai-name my_eai
    - Create named service: snow remote code myproject --compute-pool my_pool --eai-name my_eai
    - Connect with persistent storage: snow remote code --compute-pool my_pool --eai-name my_eai --stage @my_stage

    External access integration (--eai-name) is required for IDE launch to enable network access.
    The --compute-pool parameter is only required when creating a new service.
    """
    _handle_remote_service(
        name=name,
        compute_pool=compute_pool,
        eai_name=eai_name,
        stage=stage,
        image=image,
        no_ssh_key=no_ssh_key,
        ssh_mode=SSHMode.CODE,
        ide_message="Opening VS Code connected to the remote environment...",
        validate_ide=True,
    )


@app.command("cursor", requires_connection=True)
def cursor(
    name: Optional[str] = ServiceNameArgument,
    compute_pool: Optional[str] = ComputePoolOption,
    eai_name: Optional[List[str]] = EAINameOption,
    stage: Optional[str] = StageOption,
    image: Optional[str] = ImageOption,
    no_ssh_key: bool = NoSSHKeyOption,
    **options,
) -> None:
    """
    Start a remote development environment and open Cursor connected to it.

    This command creates a new VS Code Server remote development environment if it doesn't exist,
    or starts an existing one if it's suspended, then opens Cursor connected to the remote service over SSH.

    Usage examples:
    - Connect to existing service: snow remote cursor myproject
    - Create new service and connect: snow remote cursor --compute-pool my_pool --eai-name my_eai
    - Create named service: snow remote cursor myproject --compute-pool my_pool --eai-name my_eai
    - Connect with persistent storage: snow remote cursor --compute-pool my_pool --eai-name my_eai --stage @my_stage

    External access integration (--eai-name) is required for IDE launch to enable network access.
    The --compute-pool parameter is only required when creating a new service.
    """
    _handle_remote_service(
        name=name,
        compute_pool=compute_pool,
        eai_name=eai_name,
        stage=stage,
        image=image,
        no_ssh_key=no_ssh_key,
        ssh_mode=SSHMode.CURSOR,
        ide_message="Opening Cursor connected to the remote environment...",
        validate_ide=True,
    )
