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

"""Git and GitHub helpers for publishing into snowflake-prod-docs."""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path

from prod_docs_common import die

RunCommand = Callable[..., subprocess.CompletedProcess[str]]


def run_command(
    command: Sequence[str],
    *,
    cwd: Path,
    runner: RunCommand,
) -> subprocess.CompletedProcess[str]:
    result = runner(
        list(command),
        cwd=str(cwd),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        die(f"Command failed ({result.returncode}): {' '.join(command)}\n{detail}")
    return result


def docs_repo_is_dirty(docs_repo: Path, *, runner: RunCommand) -> bool:
    result = run_command(["git", "status", "--porcelain"], cwd=docs_repo, runner=runner)
    return bool(result.stdout.strip())


def branch_name(version: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"cli-docs-{version}-{stamp}"


def prepare_docs_branch(
    docs_repo: Path,
    *,
    version: str,
    runner: RunCommand,
    allow_dirty: bool = False,
) -> str:
    """Create or reset the publish branch from origin/main before copying files."""
    branch = branch_name(version)
    run_command(["git", "fetch", "origin"], cwd=docs_repo, runner=runner)
    if allow_dirty:
        run_command(
            ["git", "checkout", "-B", branch, "origin/main"],
            cwd=docs_repo,
            runner=runner,
        )
    else:
        run_command(["git", "checkout", "main"], cwd=docs_repo, runner=runner)
        run_command(
            ["git", "reset", "--hard", "origin/main"],
            cwd=docs_repo,
            runner=runner,
        )
        run_command(["git", "checkout", "-B", branch], cwd=docs_repo, runner=runner)
    return branch


def abandon_docs_branch(
    docs_repo: Path,
    branch: str,
    *,
    runner: RunCommand,
    reset: bool = False,
) -> None:
    if reset:
        run_command(["git", "reset", "--hard"], cwd=docs_repo, runner=runner)
    run_command(["git", "checkout", "main"], cwd=docs_repo, runner=runner)
    run_command(["git", "branch", "-D", branch], cwd=docs_repo, runner=runner)


def _format_pr_body(changed: Sequence[Path], version: str) -> str:
    command_ref = [path for path in changed if "command-reference" in path.as_posix()]
    release_notes = [path for path in changed if path not in command_ref]

    lines = [f"Update Snowflake CLI docs for v{version}.", ""]
    if command_ref:
        lines.append("Command reference:")
        lines.extend(f"- `{path.as_posix()}`" for path in command_ref)
        lines.append("")
    if release_notes:
        lines.append("Release notes:")
        lines.extend(f"- `{path.as_posix()}`" for path in release_notes)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def commit_push_and_open_pr(
    docs_repo: Path,
    changed: Sequence[Path],
    *,
    version: str,
    runner: RunCommand,
) -> str:
    title = f"Update Snowflake CLI docs for v{version}"
    body = _format_pr_body(changed, version)
    run_command(
        ["git", "add", "--", *[str(path) for path in changed]],
        cwd=docs_repo,
        runner=runner,
    )
    run_command(["git", "commit", "-m", title], cwd=docs_repo, runner=runner)
    run_command(["git", "push", "-u", "origin", "HEAD"], cwd=docs_repo, runner=runner)
    created = run_command(
        [
            "gh",
            "pr",
            "create",
            "--draft",
            "--title",
            title,
            "--body",
            body,
        ],
        cwd=docs_repo,
        runner=runner,
    )
    url = (created.stdout or "").strip()
    if url:
        print(url)
    return url
