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
from typing import List, Optional

import typer
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


@app.command("start", requires_connection=True)
def start(
    name: Optional[str] = typer.Argument(
        None,
        help="Service name to resume, or leave empty to create a new service with auto-generated name",
    ),
    compute_pool: Optional[str] = typer.Option(
        None,
        "--compute-pool",
        help="Name of the compute pool to use (required for new service creation)",
        show_default=False,
    ),
    eai_name: Optional[List[str]] = typer.Option(
        None,
        "--eai-name",
        help="List of external access integration names to enable network access to external resources",
    ),
    stage: Optional[str] = typer.Option(
        None,
        "--stage",
        help="Internal Snowflake stage to mount for persistent storage. "
        "The stage will be mounted at two locations: "
        "1) 'stage/user-default' -> '/root/user-default' (workspace files), "
        "2) 'stage/.vscode-server/data' -> '/root/.vscode-server/data' (VS Code settings). "
        "Example: --stage @my_stage or --stage @my_stage/project",
    ),
    image: Optional[str] = typer.Option(
        None,
        "--image",
        help="Custom image to use (can be full path like 'repo/image:tag' or just tag like '1.7.1')",
    ),
    ssh: bool = typer.Option(
        False,
        "--ssh",
        help="Set up SSH configuration for connecting to the remote environment. This is a blocking command that keeps SSH connections alive.",
    ),
    code: bool = typer.Option(
        False,
        "--code",
        help="Open VS Code connected to the remote service over SSH (mutually exclusive with --ssh and --cursor)",
    ),
    cursor: bool = typer.Option(
        False,
        "--cursor",
        help="Open Cursor connected to the remote service over SSH (mutually exclusive with --ssh and --code)",
    ),
    no_ssh_key: bool = typer.Option(
        False,
        "--no-ssh-key",
        help="When used with --ssh, skip SSH key generation and use token-only authentication (less secure)",
    ),
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
    """
    try:
        manager = RemoteManager()

        # Enforce mutual exclusivity: only one of ssh/code/cursor
        chosen = sum([1 if ssh else 0, 1 if code else 0, 1 if cursor else 0])
        if chosen > 1:
            raise CliError(
                "Options --ssh, --code, and --cursor are mutually exclusive. Choose only one."
            )

        # If launching IDE and creating a new service, enforce EAI
        launching_ide = code or cursor
        if launching_ide:
            manager.validate_ide_requirements(name, eai_name)

        service_name, url, status = manager.start(
            name=name,
            compute_pool=compute_pool,
            external_access=eai_name,
            stage=stage,
            image=image,
            generate_ssh_key=((ssh or launching_ide) and not no_ssh_key),
        )

        # Display appropriate success message based on what happened
        if status == "created":
            cc.message(
                f"✓ Remote Development Environment {service_name} created successfully!"
            )
        elif status == "resumed":
            cc.message(
                f"✓ Remote Development Environment {service_name} resumed successfully!"
            )
        elif status == "running":
            cc.message(
                f"✓ Remote Development Environment {service_name} is already running."
            )

        cc.message(f"VS Code Server URL: {url}")

        # Log detailed information at debug level
        if stage:
            log.debug("Stage '%s' mounted:", stage)
            log.debug(
                "  - Workspace: '%s/user-default' -> '%s'",
                stage,
                "/home/user/workspace",
            )
            log.debug(
                "  - VS Code data: '%s/.vscode-server/data' -> '%s'",
                stage,
                "/home/user/.vscode-server",
            )
        if eai_name:
            log.debug("External access integrations: %s", ", ".join(eai_name))
        if image:
            log.debug("Using custom image: %s", image)

        # Handle SSH/IDE setup if requested - this is a blocking operation
        if ssh:
            manager.setup_ssh_connection(service_name)
        elif launching_ide:
            ide = "code" if code else "cursor"
            manager.setup_ssh_connection(service_name, ide=ide)

    except ValueError as e:
        raise CliError(f"Error: {e}")
    except Exception as e:
        raise CliError(f"Error starting remote environment: {e}")


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
