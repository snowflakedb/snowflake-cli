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

"""Publish Snowflake CLI docs into snowflake-prod-docs and open a draft PR.

Runs ``publish_command_docs`` and ``publish_release_notes``, then commits and
opens a single draft PR in the docs repo.

Example::

    hatch run python scripts/publish_prod_docs.py \\
        --docs-repo /path/to/snowflake-prod-docs \\
        --release-date 2026-09-28
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

import publish_command_docs as command_docs
import publish_release_notes as release_notes
from prod_docs_common import parse_release_date, resolve_release_version
from prod_docs_git import (
    RunCommand,
    abandon_docs_branch,
    commit_push_and_open_pr,
    docs_repo_is_dirty,
    prepare_docs_branch,
)
from publish_command_docs import DOCS_REPO_ENV, resolve_docs_repo

CLI_ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish CLI command-reference pages and release notes into "
            "snowflake-prod-docs and open a draft PR."
        )
    )
    parser.add_argument(
        "--docs-repo",
        type=Path,
        help=f"Path to a snowflake-prod-docs git checkout. Overrides {DOCS_REPO_ENV}.",
    )
    parser.add_argument(
        "--release-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Ship date for release notes (heading, <Release>, and monthly table).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run writers and print planned updates without git or a PR.",
    )
    parser.add_argument(
        "--no-pr",
        action="store_true",
        help="Write files into the docs repo but do not commit, push, or open a PR.",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help=(
            "Run when the docs repo is not clean. "
            "Commit includes prior staged files and paths this script updates; "
            "other unstaged edits are not added."
        ),
    )
    parser.add_argument(
        "--skip-command-docs",
        action="store_true",
        help="Publish release notes only.",
    )
    parser.add_argument(
        "--skip-release-notes",
        action="store_true",
        help="Publish command-reference pages only.",
    )
    parser.add_argument(
        "--mapping",
        type=Path,
        default=command_docs.DEFAULT_MAPPING,
        help="YAML mapping of generated pages to docs-repo paths.",
    )
    return parser.parse_args(argv)


def run(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    runner: RunCommand = subprocess.run,
    cli_root: Path = CLI_ROOT,
) -> None:
    args = parse_args(argv)
    environ = env if env is not None else os.environ
    docs_repo = resolve_docs_repo(args.docs_repo, environ)
    version = resolve_release_version()
    release_date = parse_release_date(args.release_date)

    if (
        not args.dry_run
        and not args.no_pr
        and not args.allow_dirty
        and docs_repo_is_dirty(docs_repo, runner=runner)
    ):
        _die(
            f"Docs repo has uncommitted changes: {docs_repo}. "
            "Commit or stash them, use a fresh clone, or pass --allow-dirty to continue."
        )

    branch: str | None = None
    if not args.dry_run and not args.no_pr:
        branch = prepare_docs_branch(
            docs_repo,
            version=version,
            runner=runner,
            allow_dirty=args.allow_dirty,
        )

    try:
        changed = _collect_changes(args, docs_repo, environ, release_date, cli_root)
    except BaseException:
        if branch is not None:
            abandon_docs_branch(
                docs_repo,
                branch,
                runner=runner,
                reset=not args.allow_dirty,
            )
        raise

    if not changed:
        if branch is not None:
            abandon_docs_branch(docs_repo, branch, runner=runner)
        print("No prod-docs files changed.")
        return
    if args.dry_run or args.no_pr:
        return

    commit_push_and_open_pr(docs_repo, changed, version=version, runner=runner)


def main() -> None:
    run()


def _collect_changes(
    args: argparse.Namespace,
    docs_repo: Path,
    environ: Mapping[str, str],
    release_date,
    cli_root: Path,
) -> list[Path]:
    changed: list[Path] = []
    docs_flag = ["--docs-repo", str(docs_repo)]
    if args.dry_run:
        docs_flag.append("--dry-run")

    if not args.skip_command_docs:
        cmd_argv = [*docs_flag, "--mapping", str(args.mapping)]
        changed.extend(command_docs.run(cmd_argv, env=environ, cli_root=cli_root))

    if not args.skip_release_notes:
        rn_argv = [
            *docs_flag,
            "--release-date",
            release_date.isoformat(),
        ]
        changed.extend(release_notes.run(rn_argv, env=environ, cli_root=cli_root))

    return changed


def _die(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
