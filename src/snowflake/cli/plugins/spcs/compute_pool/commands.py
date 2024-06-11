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

from typing import Optional

import typer
from click import ClickException
from snowflake.cli.api.commands.flags import (
    IfNotExistsOption,
    OverrideableOption,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.output.types import CommandResult, SingleQueryResult
from snowflake.cli.api.project.util import is_valid_object_name
from snowflake.cli.plugins.object.command_aliases import (
    add_object_command_aliases,
)
from snowflake.cli.plugins.object.common import CommentOption
from snowflake.cli.plugins.spcs.common import (
    validate_and_set_instances,
)
from snowflake.cli.plugins.spcs.compute_pool.manager import ComputePoolManager

app = SnowTyperFactory(
    name="compute-pool",
    help="Manages Snowpark Container Services compute pools.",
    short_help="Manages compute pools.",
)


def _compute_pool_name_callback(name: str) -> str:
    """
    Verifies that compute pool name is a single valid identifier.
    """
    if not is_valid_object_name(name, max_depth=0, allow_quoted=False):
        raise ClickException(
            f"'{name}' is not a valid compute pool name. Note that compute pool names must be unquoted identifiers."
        )
    return name


ComputePoolNameArgument = typer.Argument(
    ...,
    help="Name of the compute pool.",
    callback=_compute_pool_name_callback,
    show_default=False,
)


MinNodesOption = OverrideableOption(
    1,
    "--min-nodes",
    help="Minimum number of nodes for the compute pool.",
    min=1,
)
MaxNodesOption = OverrideableOption(
    None,
    "--max-nodes",
    help="Maximum number of nodes for the compute pool.",
    min=1,
)

_AUTO_RESUME_HELP = "The compute pool will automatically resume when a service or job is submitted to it."

AutoResumeOption = OverrideableOption(
    True,
    "--auto-resume/--no-auto-resume",
    help=_AUTO_RESUME_HELP,
)

_AUTO_SUSPEND_SECS_HELP = "Number of seconds of inactivity after which you want Snowflake to automatically suspend the compute pool."
AutoSuspendSecsOption = OverrideableOption(
    3600,
    "--auto-suspend-secs",
    help=_AUTO_SUSPEND_SECS_HELP,
    min=1,
)

_COMMENT_HELP = "Comment for the compute pool."

add_object_command_aliases(
    app=app,
    object_type=ObjectType.COMPUTE_POOL,
    name_argument=ComputePoolNameArgument,
    like_option=like_option(
        help_example='`list --like "my%"` lists all compute pools that begin with “my”.'
    ),
    scope_option=None,
)


@app.command(requires_connection=True)
def create(
    name: str = ComputePoolNameArgument,
    instance_family: str = typer.Option(
        ...,
        "--family",
        help="Name of the instance family. For more information about instance families, refer to the SQL CREATE COMPUTE POOL command.",
        show_default=False,
    ),
    min_nodes: int = MinNodesOption(),
    max_nodes: Optional[int] = MaxNodesOption(),
    auto_resume: bool = AutoResumeOption(),
    initially_suspended: bool = typer.Option(
        False,
        "--init-suspend/--no-init-suspend",
        help="Starts the compute pool in a suspended state.",
    ),
    auto_suspend_secs: int = AutoSuspendSecsOption(),
    comment: Optional[str] = CommentOption(help=_COMMENT_HELP),
    if_not_exists: bool = IfNotExistsOption(),
    **options,
) -> CommandResult:
    """
    Creates a new compute pool.
    """
    max_nodes = validate_and_set_instances(min_nodes, max_nodes, "nodes")
    cursor = ComputePoolManager().create(
        pool_name=name,
        min_nodes=min_nodes,
        max_nodes=max_nodes,
        instance_family=instance_family,
        auto_resume=auto_resume,
        initially_suspended=initially_suspended,
        auto_suspend_secs=auto_suspend_secs,
        comment=comment,
        if_not_exists=if_not_exists,
    )
    return SingleQueryResult(cursor)


@app.command("stop-all", requires_connection=True)
def stop_all(name: str = ComputePoolNameArgument, **options) -> CommandResult:
    """
    Deletes all services running on the compute pool.
    """
    cursor = ComputePoolManager().stop(pool_name=name)
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def suspend(name: str = ComputePoolNameArgument, **options) -> CommandResult:
    """
    Suspends the compute pool by suspending all currently running services and then releasing compute pool nodes.
    """
    return SingleQueryResult(ComputePoolManager().suspend(name))


@app.command(requires_connection=True)
def resume(name: str = ComputePoolNameArgument, **options) -> CommandResult:
    """
    Resumes the compute pool from a SUSPENDED state.
    """
    return SingleQueryResult(ComputePoolManager().resume(name))


@app.command("set", requires_connection=True)
def set_property(
    name: str = ComputePoolNameArgument,
    min_nodes: Optional[int] = MinNodesOption(default=None, show_default=False),
    max_nodes: Optional[int] = MaxNodesOption(show_default=False),
    auto_resume: Optional[bool] = AutoResumeOption(default=None, show_default=False),
    auto_suspend_secs: Optional[int] = AutoSuspendSecsOption(
        default=None, show_default=False
    ),
    comment: Optional[str] = CommentOption(
        help="Comment for the compute pool.", show_default=False
    ),
    **options,
) -> CommandResult:
    """
    Sets one or more properties for the compute pool.
    """
    cursor = ComputePoolManager().set_property(
        pool_name=name,
        min_nodes=min_nodes,
        max_nodes=max_nodes,
        auto_resume=auto_resume,
        auto_suspend_secs=auto_suspend_secs,
        comment=comment,
    )
    return SingleQueryResult(cursor)


@app.command("unset", requires_connection=True)
def unset_property(
    name: str = ComputePoolNameArgument,
    auto_resume: bool = AutoResumeOption(
        default=False,
        param_decls=["--auto-resume"],
        help=f"Reset the AUTO_RESUME property - {_AUTO_RESUME_HELP}",
        show_default=False,
    ),
    auto_suspend_secs: bool = AutoSuspendSecsOption(
        default=False,
        help=f"Reset the AUTO_SUSPEND_SECS property - {_AUTO_SUSPEND_SECS_HELP}",
        show_default=False,
    ),
    comment: bool = CommentOption(
        default=False,
        help=f"Reset the COMMENT property - {_COMMENT_HELP}",
        callback=None,
        show_default=False,
    ),
    **options,
) -> CommandResult:
    """
    Resets one or more properties for the compute pool to their default value(s).
    """
    cursor = ComputePoolManager().unset_property(
        pool_name=name,
        auto_resume=auto_resume,
        auto_suspend_secs=auto_suspend_secs,
        comment=comment,
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def status(pool_name: str = ComputePoolNameArgument, **options) -> CommandResult:
    """
    Retrieves the status of a compute pool along with a relevant message, if one exists.
    """
    cursor = ComputePoolManager().status(pool_name=pool_name)
    return SingleQueryResult(cursor)
