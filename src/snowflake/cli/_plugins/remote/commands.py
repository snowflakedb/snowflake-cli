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
from snowflake.cli.api.output.types import (
    CommandResult,
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
        help="Internal Snowflake stage to mount (e.g., @my_stage or @my_stage/folder).",
    ),
    image: Optional[str] = typer.Option(
        None,
        "--image",
        help="Custom image to use (can be full path like 'repo/image:tag' or just tag like '1.7.1')",
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

    The --compute-pool parameter is only required when creating a new service. For resuming
    existing services, the compute pool is not needed.
    """
    try:
        manager = RemoteManager()

        service_name, url, status = manager.start(
            name=name,
            compute_pool=compute_pool,
            external_access=eai_name,
            stage=stage,
            image=image,
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
                "  - Workspace: '%s/user-default' → '%s'",
                stage,
                "/home/user/workspace",
            )
            log.debug(
                "  - VS Code data: '%s/.vscode-server/data' → '%s'",
                stage,
                "/home/user/.vscode-server",
            )
        if eai_name:
            log.debug("External access integrations: %s", ", ".join(eai_name))
        if image:
            log.debug("Using custom image: %s", image)

    except ValueError as e:
        cc.warning(f"Error: {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        cc.warning(f"Error starting remote environment: {e}")
        raise typer.Exit(code=1)


@app.command("list", requires_connection=True)
def list_services(**options) -> CommandResult:
    """
    Lists all remote development environments.
    """
    cursor = RemoteManager().list_services()
    return QueryResult(cursor)


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
    cc.message(f"Remote environment '{name}' suspended successfully.")
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
    cc.message(f"Remote environment '{name}' deleted successfully.")
    return SingleQueryResult(cursor)
