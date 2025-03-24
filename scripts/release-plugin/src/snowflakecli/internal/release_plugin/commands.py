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
import logging
import os
import re
import subprocess
import sys
from functools import cache, cached_property
from pathlib import Path
from typing import Optional

import click
import typer
from click.exceptions import ClickException
from git import Repo
from github import Auth, Github
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.output.types import CollectionResult, MessageResult

app = SnowTyperFactory(
    name="release",
    help="Internal release helper",
)

log = logging.getLogger(__name__)

UPDATE_RELEASE_NOTES_SCRIPT = "scripts/main.py"
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


def subprocess_run(
    command, *args, capture_output=True, text=True, allow_fail=False, **kwargs
):
    result = subprocess.run(
        command, *args, capture_output=capture_output, text=text, **kwargs
    )
    if result.returncode != 0:
        if allow_fail:
            return None
        raise ClickException(
            f"""Command '{command}' finished with non-zero exit code: {result.returncode}
            stdout:
            {result.stdout}
            stderr:
            {result.stderr}
            """
        )
    return result.stdout


def branch_exists(branch_name: str) -> bool:
    return subprocess_run(["git", "show-ref", branch_name]) is not None


class ReleaseInfo:
    def __init__(self, version):
        subprocess_run(["git", "fetch", "--all"])
        self.version = version

    @property
    def release_branch_name(self) -> str:
        return f"release-v{self.version}"

    @property
    def final_tag_name(self) -> str:
        return f"v{self.version}"

    def rc_tag_name(self, number: int) -> str:
        return f"{self.final_tag_name}-rc{number}"

    @cached_property
    def last_released_rc(self) -> Optional[int]:
        last_tag = max(self._existing_tags, default=None)
        if last_tag is None or last_tag == self.final_tag_name:
            return None
        rc_number = last_tag.removeprefix(f"{self.final_tag_name}-rc")
        return int(rc_number)

    @property
    def next_rc(self) -> int:
        if self.last_released_rc is None:
            return 0
        return self.last_released_rc + 1

    def charrypick_branch_name(self, number: int) -> str:
        return f"test-cherrypicks-{self.rc_tag_name(number)}"

    @cached_property
    def _existing_tags(self):
        all_tags = subprocess_run(["git", "tag"]).split()
        return [tag for tag in all_tags if self.version in tag]

    def tag_exists(self, tag_name: str) -> bool:
        return tag_name in self._existing_tags

    def check_status(self):
        release_branch = self.release_branch_name
        if not branch_exists(self.release_branch_name):
            release_branch = None

        return {
            "version": self.version,
            "branch": release_branch,
            "last released rc": self.last_released_rc,
            "next rc": self.next_rc,
            "next rc cherrypick branch": self.charrypick_branch_name(self.next_rc),
        }


@cache
def get_origin_url() -> str:
    return subprocess_run(["git", "ls-remote", "--get-url", "origin"]).stdout.strip()


@cache
def get_github_token() -> str:
    token = os.environ.get(GITHUB_TOKEN_ENV)

    if not token:
        raise ClickException(
            "No github token set. Please set SNOWCLI_GITHUB_TOKEN environment variable."
        )

    return token


@cache
def get_repo_home() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    )
    return Path(result.stdout.strip())


@app.command(name="init")
def init_release(version: str = VersionArgument, **options):
    """Update release notes and version on branch `main`, create branch release-vX.Y.Z."""
    release_info = ReleaseInfo(version)
    branch_name = release_info.release_branch_name
    message = f"Update release notes for {version}"
    repo = Repo(get_repo_home())
    origin = repo.remotes.origin
    origin.fetch()

    repo.git.checkout("main")
    repo.git.pull("origin", "main")
    repo.git.checkout("-b", branch_name)

    subprocess_run(
        [
            sys.executable,
            str(get_repo_home() / UPDATE_RELEASE_NOTES_SCRIPT),
            "update-release-notes",
            version,
        ]
    )

    repo.git.add(A=True)
    repo.index.commit(message)
    origin.push(branch_name)

    pr = create_pull_request(message, message, branch_name)

    return MessageResult(f"PR created at {pr.html_url}")


def create_pull_request(title, body, source_branch, target_branch="main"):
    token = get_github_token()
    auth = Auth.Token(token)
    github = Github(auth)

    repo = github.get_repo(SNOWFLAKE_CLI_REPO)
    pr = repo.create_pull(
        title=title, body=body, head=source_branch, base=target_branch
    )

    return pr

    # can we open PR from python?


@app.command()
def init_rc(version: str = VersionArgument, **options):
    """
    Creates a cherry-pick branch with appropriate release candidate version.
    """
    release_info = ReleaseInfo(version)
    branch_name = release_info.charrypick_branch_name(release_info.next_rc)
    if branch_exists(branch_name):
        return MessageResult(f"Branch {branch_name} already exists.")
    if not branch_exists(release_info.release_branch_name):
        raise ClickException(
            f"Branch `{release_info.release_branch_name}` does not exist. Did you call 'snow release init'?"
        )

    os.chdir(get_repo_home())
    with cli_console.phase("checking out to release branch"):
        subprocess_run(["git", "checkout", release_info.release_branch_name])

    # draft-bump version
    with cli_console.phase("bump version"):
        version_info = subprocess_run(["hatch", "version", "rc"])
        new_version = version_info.split("\n")[1].removeprefix("New:").strip()
        cli_console.step("New version: {}".format(new_version))

    changes = subprocess_run(["git", "diff"])
    commit = click.confirm(
        f"This will create the branch `{branch_name}` with the following changes:\n{changes}\nDo you want to continue?"
    )
    if not commit:
        subprocess_run("git", "checkout", str(get_repo_home()))
        return MessageResult("Aborted. Changes reverted.")

    with cli_console.phase("Creating cherrypick branch"):
        cli_console.step("Creating", branch_name)
        subprocess_run(["git", "checkout", "-b", branch_name])
        cli_console.step("Committing changes to git")
        subprocess_run(["git", "add", str(get_repo_home())])
        subprocess_run(["git", "commit", "-m", f"Bump version to {new_version}"])
        cli_console.step("Pushing changes")
        subprocess_run(["git", "push", "--set-upstream", "origin", branch_name])

    # TODO: create pull request
    return MessageResult(f"Branch {branch_name} successfully created.")


@app.command()
def status(version: str = VersionArgument, **options):
    """Check current release status."""
    release_info = ReleaseInfo(version)

    def _row(key, value):
        return {"check": key, "status": value}

    return CollectionResult(_row(*info) for info in release_info.check_status().items())
