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
import re
import sys
import tempfile
from pathlib import Path
from typing import List

import typer
from click.exceptions import ClickException
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.output.types import CollectionResult, MessageResult

from snowflakecli_internal_release_plugin.repo_manager import (
    ReleaseInfo,
    RepositoryManager,
)
from snowflakecli_internal_release_plugin.utils import subprocess_run

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


def get_pr_url(source_branch: str) -> str:
    return f"https://github.com/snowflakedb/snowflake-cli/pull/new/{source_branch}"


def _commit_update_release_notes(repo: RepositoryManager, version: str) -> None:
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


def _commit_bump_dev_version(repo: RepositoryManager, version: str) -> None:
    cli_console.step(f"Bumping version on main")
    major, minor, patch = version.split(".")
    new_version = f"{major}.{int(minor)+1}.{patch}"
    subprocess_run(["hatch", "version", f"{new_version}.dev0"])
    repo.git.add(A=True)
    repo.git.commit(m=f"Bump dev version to {new_version}")


def _commit_bump_version(
    repo: RepositoryManager, hatch_version: str, tag_name: str
) -> None:
    cli_console.step(f"Bumping version to {tag_name}")
    subprocess_run(["hatch", "version", hatch_version])
    repo.git.add(A=True)
    repo.git.commit(m=f"Bump version to {tag_name}")


@app.command(name="init")
def init_release(version: str = VersionArgument, **options):
    """Update release notes and version on branch `main`, create branch release-vX.Y.Z and release tag rc0."""
    repo = RepositoryManager()
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


@app.command()
def cherrypick_branch(
    version: str = VersionArgument, final: bool = FinalOption, **options
):
    """
    Creates a cherry-pick branch with appropriate release candidate version.
    """
    repo = RepositoryManager()
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
    repo = RepositoryManager()
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


@contextlib.contextmanager
def snow_executable(tag: str):
    with tempfile.TemporaryDirectory() as tmpdir:
        with cli_console.phase(f"Installing snowflake-cli from tag `{tag}`"):
            cli_console.step(f"creating virtualenv")
            subprocess_run(["python", "-m", "venv", tmpdir])
            python = Path(tmpdir) / "bin" / "python"

            cli_console.step(f"installing snowflake-cli")
            subprocess_run(
                [
                    python,
                    "-m",
                    "pip",
                    "install",
                    f"git+https://github.com/snowflakedb/snowflake-cli.git@{tag}",
                ]
            )

        yield Path(tmpdir) / "bin" / "snow"


@app.command()
def validate_pip_installation(version: str = VersionArgument, **options):
    """Validate pip installation from latest tag."""
    from subprocess import run

    commands: List[List[str]] = [
        ["--version"],
        [],
        ["connection", "test"],
        ["sql", "-q", "select 42"],
    ]

    release_info = ReleaseInfo(version, repo=RepositoryManager())
    if release_info.latest_released_tag is None:
        raise ClickException(f"There is no tag released for version {version} yet.")

    with snow_executable(release_info.latest_released_tag) as snow_cmd:
        results = []
        for command in commands:
            cli_console.step(f"$> running `snow {' '.join(command)}`")
            completed = run([str(snow_cmd)] + command)
            results.append(
                {
                    "command": " ".join(["snow"] + command),
                    "status": "OK" if completed.returncode == 0 else "ERROR",
                }
            )

    return CollectionResult(results)


@app.command()
def status(version: str = VersionArgument, **options):
    """Check current release status."""
    release_info = ReleaseInfo(version, repo=RepositoryManager())

    def _row(key, value):
        return {"check": key, "status": value}

    return CollectionResult(_row(*info) for info in release_info.check_status().items())


def _extract_release_notes_from_file(file: Path, version: str) -> str:
    result: List[str] = []

    def _is_a_version_title(line: str) -> bool:
        return line.startswith(f"# ")

    def _is_current_version_title(line: str) -> bool:
        return _is_a_version_title(line) and version in line

    with file.open("r") as contents:
        extract_lines = False
        for line in contents:
            should_stop = extract_lines and _is_a_version_title(line)
            if should_stop:
                break
            if _is_current_version_title(line):
                extract_lines = True
            if extract_lines:
                result.append(line.rstrip())

    return "\n".join(result)


@app.command()
def release_notes(version: str = VersionArgument, **options):
    """Extract release notes for the chosen version."""
    repo = RepositoryManager()
    release_info = ReleaseInfo(version, repo=repo)
    release_info.assert_release_branch_exists()
    with repo.tmp_checkout(release_info.release_branch_name):
        release_notes = repo.home_path / "RELEASE-NOTES.md"
        return MessageResult(_extract_release_notes_from_file(release_notes, version))
