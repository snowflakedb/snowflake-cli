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

import time
from typing import List, Optional

import typer
from snowflake.cli._plugins.container_runtime.manager import ContainerRuntimeManager
from snowflake.cli.api.commands.flags import identifier_argument
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.output.types import CommandResult, QueryResult, SingleQueryResult

app = SnowTyperFactory(
    name="container-runtime",
    help="Manages Snowpark Container Services Runtime Environment with VS Code Server.",
    short_help="Manages container runtime environment.",
)

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


@app.command("create", requires_connection=True)
def create(
    name: str = NameOption,
    compute_pool: str = ComputePoolOption,
    persistent_storage: bool = PersistentStorageOption,
    storage_size: int = StorageSizeOption,
    external_access: bool = ExternalAccessOption,
    timeout: int = TimeoutOption,
    extensions: Optional[List[str]] = ExtensionsOption,
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
        )

        # Display success message with the endpoint URL
        cc.step("✓ Container Runtime Environment created successfully!")
        cc.step(f"Access your VS Code Server at: {url}")
        cc.step(f"Default password: password")
        cc.step(f"Session timeout: {timeout} minutes")
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
