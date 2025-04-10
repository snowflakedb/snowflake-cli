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
import contextlib
import logging
import os
import re
import subprocess
import sys
from functools import cache, cached_property
from pathlib import Path
from typing import Optional

import git
import typer
from click.exceptions import ClickException
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

FinalOption = typer.Option(False, "--final", help="Use final release instead of -rc")


def _check_version_format_callback(version: str) -> str:
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ClickException(f"version not in format 'X.Y.Z': {version}")
    return version


VersionArgument = typer.Argument(
    help="version in format X.Y.Z",
    callback=_check_version_format_callback,
)


def subprocess_run(command, *args, capture_output=True, text=True, **kwargs) -> str:
    result = subprocess.run(
        command, *args, capture_output=capture_output, text=text, **kwargs
    )
    if result.returncode != 0:
        raise ClickException(
            f"""Command '{command}' finished with non-zero exit code: {result.returncode}
            ----- stdout -----
            {result.stdout}
            ===== stderr =====
            {result.stderr}
            """
        )
    return result.stdout


class Repo(git.Repo):
    """Repository manager."""

    def __init__(self):
        self.home_path = Path(
            subprocess_run(["git", "rev-parse", "--show-toplevel"]).strip()
        )
        super().__init__(self.home_path)
        self.remotes.origin.fetch()

    def exists(self, ref: str) -> bool:
        return any(ref == r.name for r in self.references)

    @contextlib.contextmanager
    def tmp_checkout(self, ref: str):
        """Contextmanager returning to current branch after execution."""
        current_ref = self.head.reference
        try:
            with cli_console.phase(f"checking out to {ref}"):
                self.git.checkout(ref)
                self.git.pull()
            yield
        finally:
            with cli_console.phase(f"checking out back to {current_ref.name}"):
                self.git.checkout(current_ref.name)


class ReleaseInfo:
    """Class providing information about release."""

    def __init__(self, version, repo: Repo):
        subprocess_run(["git", "fetch", "--all"])
        self.repo = repo
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
    def _existing_tags(self):
        return [tag.name for tag in self.repo.tags if self.version in tag.name]

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

    @staticmethod
    def cherrypick_branch_name(tag_name: str) -> str:
        return f"cherrypicks-{tag_name}"

    def check_status(self):
        def _show_branch_if_exists(branch_name: str) -> Optional[str]:
            return branch_name if self.repo.exists(branch_name) else None

        return {
            "version": self.version,
            "version released": self.repo.exists(self.final_tag_name),
            "branch": _show_branch_if_exists(self.release_branch_name),
            "last released rc": self.last_released_rc,
            "next rc": self.next_rc,
            "next rc cherrypick branch": _show_branch_if_exists(
                self.cherrypick_branch_name(self.rc_tag_name(self.next_rc))
            ),
            "final cherrypicks branch": _show_branch_if_exists(
                self.cherrypick_branch_name(self.final_tag_name)
            ),
        }

    def assert_release_branch_exists(self):
        if not self.repo.exists(self.release_branch_name):
            raise ClickException(
                f"Branch `{self.release_branch_name}` does not exist. Did you call 'snow release init'?"
            )

    def assert_release_branch_not_exists(self):
        if self.repo.exists(self.release_branch_name):
            raise ClickException(f"Branch `{self.release_branch_name}` already exists.")


# ===== not yet used stuff automatically creating PRs =====
@cache
def get_github_token() -> str:
    token = os.environ.get(GITHUB_TOKEN_ENV)

    if not token:
        raise ClickException(
            "No github token set. Please set SNOWCLI_GITHUB_TOKEN environment variable."
        )

    return token


def create_pull_request(title, body, source_branch, target_branch="main"):
    token = get_github_token()
    auth = Auth.Token(token)
    github = Github(auth)

    repo = github.get_repo(SNOWFLAKE_CLI_REPO)
    pr = repo.create_pull(
        title=title, body=body, head=source_branch, base=target_branch
    )

    return pr


# ===== not yet used stuff automatically creating PRs =====


def get_pr_url(source_branch: str) -> str:
    return f"https://github.com/snowflakedb/snowflake-cli/pull/new/{source_branch}"


def _commit_update_release_notes(repo: Repo, version: str) -> None:
    cli_console.step(f"Creating release notes for {version}")
    subprocess_run(
        [
            sys.executable,
            str(repo.home_path / UPDATE_RELEASE_NOTES_SCRIPT),
            "update-release-notes",
            version,
        ]
    )
    repo.git.add(A=True)
    repo.git.commit(m=f"Update release notes for {version}")


def _commit_bump_dev_version(repo: Repo, version: str) -> None:
    cli_console.step(f"Bumping version on main")
    major, minor, patch = version.split(".")
    new_version = f"{major}.{int(minor)+1}.{patch}"
    subprocess_run(["hatch", "version", f"{new_version}.dev0"])
    repo.git.add(A=True)
    repo.git.commit(m=f"Bump dev version to {new_version}")


def _commit_bump_version(repo: Repo, hatch_version: str, tag_name: str) -> None:
    cli_console.step(f"Bumping version to {tag_name}")
    subprocess_run(["hatch", "version", hatch_version])
    repo.git.add(A=True)
    repo.git.commit(m=f"Bump version to {tag_name}")


@app.command(name="init")
def init_release(version: str = VersionArgument, **options):
    """Update release notes and version on branch `main`, create branch release-vX.Y.Z and release tag rc0."""
    repo = Repo()
    release_info = ReleaseInfo(version, repo)
    if repo.exists(release_info.release_branch_name):
        raise ClickException(
            f'Branch "{release_info.release_branch_name}" already exists'
        )

    with repo.tmp_checkout("main"):
        # create release branch and update release notes
        repo.git.checkout(release_info.release_branch_name, b=True)
        _commit_update_release_notes(repo, version)
        repo.git.push("origin", release_info.release_branch_name, set_upstream=True)

        # create separate brunch bumping main version
        bump_release_notes_main_branch = f"bump-release-notes-{version}"
        repo.git.checkout(bump_release_notes_main_branch, b=True)
        _commit_bump_dev_version(repo, version)
        repo.git.push("origin", bump_release_notes_main_branch, set_upstream=True)

        # create rc0 branch
        rc0_branch = release_info.cherrypick_branch_name(
            release_info.rc_tag_name(
                release_info.next_rc,
            )
        )
        repo.git.checkout(release_info.release_branch_name)
        repo.git.checkout(rc0_branch, b=True)
        _commit_bump_version(repo, "rc", release_info.rc_tag_name(0))
        repo.git.push("origin", rc0_branch, set_upstream=True)

    main_pr_url = get_pr_url(bump_release_notes_main_branch)
    rc0_pr_url = get_pr_url(rc0_branch)

    return MessageResult(
        f"""Release branch successfully initialized.
    create PR to 'main': {main_pr_url}
    create PR to '{release_info.release_branch_name}': {rc0_pr_url}"""
    )

    # pr = create_pull_request(message, message, branch_name)

    # return MessageResult(f"PR created at {pr.html_url}")

    # can we open PR from python?


@app.command()
def cherrypick_branch(
    version: str = VersionArgument, final: bool = FinalOption, **options
):
    """
    Creates a cherry-pick branch with appropriate release candidate version.
    """
    repo = Repo()
    release_info = ReleaseInfo(version, repo)
    release_info.assert_release_branch_exists()

    if final:
        next_tag_name = release_info.final_tag_name
    else:
        next_tag_name = release_info.rc_tag_name(release_info.next_rc)
    cherrypick_branch_name = release_info.cherrypick_branch_name(next_tag_name)
    if repo.exists(cherrypick_branch_name):
        return MessageResult(f"Branch {cherrypick_branch_name} already exists.")

    with (
        repo.tmp_checkout(release_info.release_branch_name),
        cli_console.phase(f"Creating branch `{cherrypick_branch_name}`"),
    ):
        repo.git.checkout(cherrypick_branch_name, b=True)
        _commit_bump_version(repo, version if final else "rc", next_tag_name)
        cli_console.step(f"Publishing branch `{cherrypick_branch_name}`")
        repo.git.push("origin", cherrypick_branch_name, set_upstream=True)
        pr_url = get_pr_url(cherrypick_branch_name)

    return MessageResult(
        f"""Branch `{cherrypick_branch_name}` successfully created.
    create PR to `{release_info.release_branch_name}`: {pr_url}"""
    )


@app.command()
def tag(version: str = VersionArgument, final: bool = FinalOption, **options):
    """Publish release tag."""
    repo = Repo()
    release_info = ReleaseInfo(version, repo)
    release_info.assert_release_branch_exists()

    with repo.tmp_checkout(release_info.release_branch_name):
        if final:
            tag_name = release_info.final_tag_name
        else:
            tag_name = release_info.rc_tag_name(release_info.next_rc)

        with cli_console.phase("validating version"):
            current_version = subprocess_run(["hatch", "version"]).strip()
            expected_version = tag_name.replace("-", "").removeprefix("v")
            if current_version != expected_version:
                raise ClickException(
                    f"Published version does not match version on release branch:\n"
                    f"expected version: {expected_version}\nversion on branch: {current_version}"
                )
            cli_console.step(
                f"OK - Version on release branch ({current_version}) matches the tag."
            )

        typer.confirm(
            f"This command is going to publish tag `{tag_name}`. This cannot be undone. Do you want to continue?",
            abort=True,
        )
        with cli_console.phase(f"Publishing tag `{tag_name}`"):
            repo.git.tag(tag_name)
            repo.git.push("origin", tag_name)

    return MessageResult(f"Tag `{tag_name}` successfully published.")


@app.command()
def status(version: str = VersionArgument, **options):
    """Check current release status."""
    release_info = ReleaseInfo(version, repo=Repo())

    def _row(key, value):
        return {"check": key, "status": value}

    return CollectionResult(_row(*info) for info in release_info.check_status().items())
