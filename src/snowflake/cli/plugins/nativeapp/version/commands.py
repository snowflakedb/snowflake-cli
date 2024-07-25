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
from typing import Optional

import typer
from click import MissingParameter
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    with_project_definition,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult, QueryResult
from snowflake.cli.api.project.project_verification import assert_project_type
from snowflake.cli.plugins.nativeapp.common_flags import ForceOption, InteractiveOption
from snowflake.cli.plugins.nativeapp.policy import (
    AllowAlwaysPolicy,
    AskAlwaysPolicy,
    DenyAlwaysPolicy,
)
from snowflake.cli.plugins.nativeapp.run_processor import NativeAppRunProcessor
from snowflake.cli.plugins.nativeapp.v2_conversions.v2_to_v1_decorator import (
    nativeapp_definition_v2_to_v1,
)
from snowflake.cli.plugins.nativeapp.version.version_processor import (
    NativeAppVersionCreateProcessor,
    NativeAppVersionDropProcessor,
)

app = SnowTyperFactory(
    name="version",
    help="Manages versions defined in an application package",
)

log = logging.getLogger(__name__)


@app.command(requires_connection=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def create(
    version: Optional[str] = typer.Argument(
        None,
        help=f"""Version to define in your application package. If the version already exists, an auto-incremented patch is added to the version instead. Defaults to the version specified in the `manifest.yml` file.""",
    ),
    patch: Optional[int] = typer.Option(
        None,
        "--patch",
        help=f"""The patch number you want to create for an existing version.
        Defaults to undefined if it is not set, which means the Snowflake CLI either uses the patch specified in the `manifest.yml` file or automatically generates a new patch number.""",
    ),
    skip_git_check: Optional[bool] = typer.Option(
        False,
        "--skip-git-check",
        help="When enabled, the Snowflake CLI skips checking if your project has any untracked or stages files in git. Default: unset.",
        is_flag=True,
    ),
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Adds a new patch to the provided version defined in your application package. If the version does not exist, creates a version with patch 0.
    """

    assert_project_type("native_app")

    if version is None and patch is not None:
        raise MissingParameter("Cannot provide a patch without version!")

    is_interactive = False
    if force:
        policy = AllowAlwaysPolicy()
    elif interactive:
        is_interactive = True
        policy = AskAlwaysPolicy()
    else:
        policy = DenyAlwaysPolicy()

    if skip_git_check:
        git_policy = DenyAlwaysPolicy()
    else:
        git_policy = AllowAlwaysPolicy()

    processor = NativeAppVersionCreateProcessor(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )

    # We need build_bundle() to (optionally) find version in manifest.yml and create an application package
    bundle_map = processor.build_bundle()
    processor.process(
        bundle_map=bundle_map,
        version=version,
        patch=patch,
        policy=policy,
        git_policy=git_policy,
        is_interactive=is_interactive,
    )
    return MessageResult(f"Version create is now complete.")


@app.command("list", requires_connection=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def version_list(
    **options,
) -> CommandResult:
    """
    Lists all versions defined in an application package.
    """

    assert_project_type("native_app")

    processor = NativeAppRunProcessor(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )
    cursor = processor.get_all_existing_versions()
    return QueryResult(cursor)


@app.command(requires_connection=True)
@with_project_definition()
@nativeapp_definition_v2_to_v1
def drop(
    version: Optional[str] = typer.Argument(
        None,
        help="Version defined in an application package that you want to drop. Defaults to the version specified in the `manifest.yml` file.",
    ),
    interactive: bool = InteractiveOption,
    force: Optional[bool] = ForceOption,
    **options,
) -> CommandResult:
    """
    Drops a version defined in your application package. Versions can either be passed in as an argument to the command or read from the `manifest.yml` file.
    Dropping patches is not allowed.
    """

    assert_project_type("native_app")

    is_interactive = False
    if force:
        policy = AllowAlwaysPolicy()
    elif interactive:
        is_interactive = True
        policy = AskAlwaysPolicy()
    else:
        policy = DenyAlwaysPolicy()

    processor = NativeAppVersionDropProcessor(
        project_definition=cli_context.project_definition.native_app,
        project_root=cli_context.project_root,
    )
    processor.process(version, policy, is_interactive)
    return MessageResult(f"Version drop is now complete.")
