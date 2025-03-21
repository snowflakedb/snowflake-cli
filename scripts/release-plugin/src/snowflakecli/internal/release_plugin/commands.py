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
import os
import re
import subprocess
import sys
from functools import cache
from pathlib import Path

import typer
from click.exceptions import ClickException
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory

app = SnowTyperFactory(
    name="release",
    help="Internal release helper",
)


def _check_version_format_callback(version: str) -> str:
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ClickException(f"version not in format 'X.Y.Z': {version}")
    return version


VersionArgument = typer.Argument(
    help="version in format X.Y.Z",
    callback=_check_version_format_callback,
)


def subprocess_run(command, *args, **kwargs):
    result = subprocess.run(command, *args, **kwargs)
    if result.returncode != 0:
        raise ClickException(
            f""""Command finished with non-zero exit code: {result.returncode}
            stdout:
            {result.stdout}
            stderr:
            {result.stderr}
            """
        )


def release_branch_name(version: str) -> str:
    return f"release-v{version}"


@cache
def get_repo_home() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    )
    return Path(result.stdout)


@app.command(name="init")
def init_release(version: str = VersionArgument):
    """Update release notes and version on branch `main`, create branch release-vX.Y.Z."""

    os.chdir(get_repo_home())
    subprocess.run(["git", "fetch", "--all"])
    subprocess.run(["git", "checkout", "origin/main"])
    subprocess.run([sys.executable, "scripts/main.py", "update-release-notes", version])

    subprocess.run(["git", "checkout", "-b", f"update-release-notes-for-{version}"])
    subprocess.run(["git", "add", "."])
    subprocess.run(["git", "commit", "-m", f"release notes update for {version}"])
    subprocess.run(["git", "push", "origin"])

    # can we open PR from python?


@app.command()
def create_rc(version: str = VersionArgument, **options):

    # create release branch

    # update version on branch ("hatch version rc")
    pass
