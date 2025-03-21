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
from git import Repo
from github import Auth, Github
from pathlib import Path

import typer
from click.exceptions import ClickException
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult

app = SnowTyperFactory(
    name="release",
    help="Internal release helper",
)

UPDATE_RELEASE_NOTES_SCRIPT ="scripts/main.py"
GITHUB_TOKEN_ENV = "SNOWCLI_GITHUB_TOKEN"
SNOWFLAKE_CLI_REPO = "snowflakedb/snowflake-cli"


def _check_version_format_callback(version: str) -> str:
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ClickException(f"version not in format 'X.Y.Z': {version}")
    return version


VersionArgument = typer.Argument(
    help="version in format X.Y.Z",
    callback=_check_version_format_callback,
)


def subprocess_run(command, *args, capture_output=True, text=True, **kwargs):
    result = subprocess.run(
        command, *args, capture_output=capture_output, text=text, **kwargs
    )
    if result.returncode != 0:
        raise ClickException(
            f""""Command finished with non-zero exit code: {result.returncode}
            stdout:
            {result.stdout}
            stderr:
            {result.stderr}
            """
        )
    return result.stdout


def release_branch_name(version: str) -> str:
    return f"release-v{version}"


@cache
def get_origin_url() -> str:
    return subprocess_run(["git", "ls-remote", "--get-url", "origin"]).stdout.strip()

@cache
def get_github_token() -> str:
    token = os.environ.get(GITHUB_TOKEN_ENV)

    if not token:
        raise ClickException("No github token set. Please set SNOWCLI_GITHUB_TOKEN environment variable.")

    return token


@cache
def get_repo_home() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    )
    return Path(result.stdout)


@app.command(name="init")
def init_release(version: str = VersionArgument):
    """Update release notes and version on branch `main`, create branch release-vX.Y.Z."""
    branch_name = release_branch_name(version)
    message = f"Update release notes for {version}"
    repo = Repo(get_repo_home())
    origin = repo.remotes.origin
    origin.fetch()

    repo.git.checkout('main')
    repo.git.pull('origin', 'main')
    repo.git.checkout('-b', branch_name)

    subprocess_run([sys.executable, UPDATE_RELEASE_NOTES_SCRIPT, "update-release-notes", version])

    repo.git.add(A=True)
    repo.index.commit(message)
    origin.push(branch_name)

    pr = create_pull_request(message, message, branch_name)

    return MessageResult(f"PR created at {pr.html_url}")


def create_pull_request(title, body, source_branch, target_branch= "main"):
    token = get_github_token()
    auth = Auth.Token(token)
    github = Github(auth)

    repo = github.get_repo(SNOWFLAKE_CLI_REPO)
    pr = repo.create_pull(title=title, body=body, head=source_branch, base=target_branch)

    return pr




    # can we open PR from python?


@app.command()
def create_rc(version: str = VersionArgument, **options):
    """
    Creates a release candidate branch for the given version.
    """

    # create release branch

    # update version on branch ("hatch version rc")
    pass
