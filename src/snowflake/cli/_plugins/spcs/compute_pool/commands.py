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

from typing import List, Optional

import typer
from click import ClickException
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
)
from snowflake.cli._plugins.object.common import CommentOption, Tag, TagOption
from snowflake.cli._plugins.spcs.common import validate_and_set_instances
from snowflake.cli._plugins.spcs.compute_pool.compute_pool_entity_model import (
    ComputePoolEntityModel,
)
from snowflake.cli._plugins.spcs.compute_pool.manager import ComputePoolManager
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    IfNotExistsOption,
    OverrideableOption,
    entity_argument,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CommandResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.definition_helper import (
    get_entity_from_project_definition,
)
from snowflake.cli.api.project.util import is_valid_object_name

app = SnowTyperFactory(
    name="compute-pool",
    help="Manages Snowpark Container Services compute pools.",
    short_help="Manages compute pools.",
)


def _compute_pool_name_callback(name: FQN) -> FQN:
    """
    Verifies that compute pool name is a single valid identifier.
    """
    if not is_valid_object_name(name.identifier, max_depth=0, allow_quoted=False):
        raise ClickException(
            f"'{name}' is not a valid compute pool name. Note that compute pool names must be unquoted identifiers."
        )
    return name


ComputePoolNameArgument = identifier_argument(
    sf_object="compute pool",
    example="my_compute_pool",
    callback=_compute_pool_name_callback,
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
    False,
    "--auto-resume",
    help=_AUTO_RESUME_HELP,
    mutually_exclusive=["no_auto_resume"],
)

NoAutoResumeOption = OverrideableOption(
    False,
    "--no-auto-resume",
    help=_AUTO_RESUME_HELP,
    mutually_exclusive=["auto_resume"],
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
    name: FQN = ComputePoolNameArgument,
    instance_family: str = typer.Option(
        ...,
        "--family",
        help="Name of the instance family. For more information about instance families, refer to the SQL CREATE COMPUTE POOL command.",
        show_default=False,
    ),
    min_nodes: int = MinNodesOption(),
    max_nodes: Optional[int] = MaxNodesOption(),
    auto_resume: bool = AutoResumeOption(),
    no_auto_resume: bool = NoAutoResumeOption(),
    initially_suspended: bool = typer.Option(
        False,
        "--init-suspend/--no-init-suspend",
        help="Starts the compute pool in a suspended state.",
    ),
    auto_suspend_secs: int = AutoSuspendSecsOption(),
    tags: Optional[List[Tag]] = TagOption(help="Tag for the compute pool."),
    comment: Optional[str] = CommentOption(help=_COMMENT_HELP),
    if_not_exists: bool = IfNotExistsOption(),
    **options,
) -> CommandResult:
    """
    Creates a new compute pool.
    """
    resume_option = True if auto_resume else False if no_auto_resume else True
    max_nodes = validate_and_set_instances(min_nodes, max_nodes, "nodes")
    cursor = ComputePoolManager().create(
        pool_name=name.identifier,
        min_nodes=min_nodes,
        max_nodes=max_nodes,
        instance_family=instance_family,
        auto_resume=resume_option,
        initially_suspended=initially_suspended,
        auto_suspend_secs=auto_suspend_secs,
        tags=tags,
        comment=comment,
        if_not_exists=if_not_exists,
    )
    return SingleQueryResult(cursor)


@app.command("deploy", requires_connection=True)
@with_project_definition()
def deploy(
    entity_id: str = entity_argument("compute-pool"),
    upgrade: bool = typer.Option(
        False,
        "--upgrade",
        help="Updates the existing compute pool. Can update min_nodes, max_nodes, auto_resume, auto_suspend_seconds and comment.",
    ),
    **options,
):
    """
    Deploys a compute pool from the project definition file.
    """
    compute_pool: ComputePoolEntityModel = get_entity_from_project_definition(
        entity_type=ObjectType.COMPUTE_POOL, entity_id=entity_id
    )
    max_nodes = validate_and_set_instances(
        compute_pool.min_nodes, compute_pool.max_nodes, "nodes"
    )

    cursor = ComputePoolManager().deploy(
        pool_name=compute_pool.fqn.identifier,
        min_nodes=compute_pool.min_nodes,
        max_nodes=max_nodes,
        instance_family=compute_pool.instance_family,
        auto_resume=compute_pool.auto_resume,
        initially_suspended=compute_pool.initially_suspended,
        auto_suspend_seconds=compute_pool.auto_suspend_seconds,
        tags=compute_pool.tags,
        comment=compute_pool.comment,
        upgrade=upgrade,
    )

    return SingleQueryResult(cursor)


@app.command("stop-all", requires_connection=True)
def stop_all(name: FQN = ComputePoolNameArgument, **options) -> CommandResult:
    """
    Deletes all services running on the compute pool.
    """
    cursor = ComputePoolManager().stop(pool_name=name.identifier)
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def suspend(name: FQN = ComputePoolNameArgument, **options) -> CommandResult:
    """
    Suspends the compute pool by suspending all currently running services and then releasing compute pool nodes.
    """
    return SingleQueryResult(ComputePoolManager().suspend(name.identifier))


@app.command(requires_connection=True)
def resume(name: FQN = ComputePoolNameArgument, **options) -> CommandResult:
    """
    Resumes the compute pool from a SUSPENDED state.
    """
    return SingleQueryResult(ComputePoolManager().resume(name.identifier))


@app.command("set", requires_connection=True)
def set_property(
    name: FQN = ComputePoolNameArgument,
    min_nodes: Optional[int] = MinNodesOption(default=None, show_default=False),
    max_nodes: Optional[int] = MaxNodesOption(show_default=False),
    auto_resume: bool = AutoResumeOption(default=None, show_default=False),
    no_auto_resume: bool = NoAutoResumeOption(default=None, show_default=False),
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
    resume_option = True if auto_resume else False if no_auto_resume else None
    cursor = ComputePoolManager().set_property(
        pool_name=name.identifier,
        min_nodes=min_nodes,
        max_nodes=max_nodes,
        auto_resume=resume_option,
        auto_suspend_secs=auto_suspend_secs,
        comment=comment,
    )
    return SingleQueryResult(cursor)


@app.command("unset", requires_connection=True)
def unset_property(
    name: FQN = ComputePoolNameArgument,
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
        pool_name=name.identifier,
        auto_resume=auto_resume,
        auto_suspend_secs=auto_suspend_secs,
        comment=comment,
    )
    return SingleQueryResult(cursor)


@app.command(requires_connection=True)
def status(pool_name: FQN = ComputePoolNameArgument, **options) -> CommandResult:
    """
    Retrieves the status of a compute pool along with a relevant message, if one exists.
    """
    cursor = ComputePoolManager().status(pool_name=pool_name.identifier)
    return SingleQueryResult(cursor)
