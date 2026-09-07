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

import typer
from snowflake.cli.api.feature_flags import FeatureFlag

AllowSharedLibrariesOption: bool = typer.Option(
    False,
    "--allow-shared-libraries",
    help="Allows shared (.so) libraries, when using packages installed through PIP.",
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

SkipDependenciesOption: bool = typer.Option(
    False,
    "--skip-dependencies",
    help="Does not resolve dependencies from requirements.txt or pyproject.toml and"
    " does not create dependencies.zip. Only the code of the project is packaged. Use"
    " it when packages are declared in the project definition file and installed by"
    " Snowflake, for example from an artifact repository.",
    hidden=not FeatureFlag.ENABLE_SNOWPARK_SKIP_DEPENDENCIES.is_enabled(),
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
