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

import click
import typer
from snowflake.cli.api.commands.flags import (
    deprecated_flag_callback,
    deprecated_flag_callback_enum,
)
from snowflake.cli.plugins.snowpark.models import YesNoAsk


def deprecated_allow_native_libraries_option(old_flag_name: str):
    return typer.Option(
        YesNoAsk.NO.value,
        old_flag_name,
        help="Allows native libraries, when using packages installed through PIP",
        hidden=True,
        callback=deprecated_flag_callback_enum(
            f"{old_flag_name} flag is deprecated. Use --allow-shared-libraries flag instead."
        ),
    )


def resolve_allow_shared_libraries_yes_no_ask(allow_shared_libraries: YesNoAsk) -> bool:
    if allow_shared_libraries == YesNoAsk.ASK:
        return click.confirm("Continue with package installation?", default=False)
    else:
        return allow_shared_libraries == YesNoAsk.YES


AllowSharedLibrariesOption: bool = typer.Option(
    False,
    "--allow-shared-libraries",
    help="Allows shared (.so) libraries, when using packages installed through PIP.",
)

DeprecatedCheckAnacondaForPyPiDependencies: bool = typer.Option(
    True,
    "--check-anaconda-for-pypi-deps/--no-check-anaconda-for-pypi-deps",
    "-a",
    help="""Checks if any of missing Anaconda packages dependencies can be imported directly from Anaconda. Valid values include: `true`, `false`, Default: `true`.""",
    hidden=True,
    callback=deprecated_flag_callback(
        "--check-anaconda-for-pypi-deps flag is deprecated. Use --ignore-anaconda flag instead."
    ),
)

IgnoreAnacondaOption: bool = typer.Option(
    False,
    "--ignore-anaconda",
    help="Does not lookup packages on Snowflake Anaconda channel.",
)

SkipVersionCheckOption: bool = typer.Option(
    False,
    "--skip-version-check",
    help="Skip comparing versions of dependencies between requirements and Anaconda.",
)

IndexUrlOption: str | None = typer.Option(
    None,
    "--index-url",
    help="Base URL of the Python Package Index to use for package lookup. This should point to "
    " a repository compliant with PEP 503 (the simple repository API) or a local directory laid"
    " out in the same format.",
    show_default=False,
)

ReturnsOption: str = typer.Option(
    ...,
    "--returns",
    "-r",
    help="Data type for the procedure to return.",
)

OverwriteOption: bool = typer.Option(
    False,
    "--overwrite",
    "-o",
    help="Replaces an existing procedure with this one.",
)
