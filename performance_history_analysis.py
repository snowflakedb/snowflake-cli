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

import subprocess
from decimal import Decimal
from timeit import default_timer as timer
from typing import List, Optional, Tuple

import typer
from git import Commit, Repo
from rich import print


def _reinstall_snowcli() -> None:
    subprocess.run(
        ["pip", "install", "--quiet", "--disable-pip-version-check", "."],
        stdout=subprocess.PIPE,
    )


def _run_snowcli_x_times_and_gather_times(sample_amount: int) -> List[float]:
    results = []
    for _ in range(sample_amount):
        start = timer()
        subprocess.run(["snow", "--help"], stdout=subprocess.PIPE)
        end = timer()
        results.append(end - start)
    results.sort()
    return results


def _print_results_for_single_commit(
    commit: Commit,
    chosen_result: float,
    all_results: List[float],
    print_all_resutls: bool,
) -> None:
    print(
        f"Result [{chosen_result}] for commit {commit} ({commit.authored_datetime}) -> {commit.message.splitlines()[0]}"
    )
    if print_all_resutls:
        print(all_results.__str__() + "\n")


def _print_summary_performance_descending(
    commits_with_results: List[Tuple[Commit, float]]
) -> None:
    commits_with_diffs: List[Tuple[Commit, Decimal]] = []
    last_result: Optional[Decimal] = None
    for commit, result in reversed(commits_with_results):
        if last_result:
            result_diff = Decimal(result) - last_result
            if result_diff > 0:
                commits_with_diffs.append((commit, result_diff))
        last_result = Decimal(result)
    sorted_list_of_diffs: List[Tuple[Commit, Decimal]] = sorted(
        commits_with_diffs, key=lambda e: e[1], reverse=True
    )
    print("\nCommits causing performance descending:")
    for commit, diff in sorted_list_of_diffs:
        print(
            f"Diff [{diff}] after commit {commit} ({commit.authored_datetime}) -> {commit.message.splitlines()[0]}"
        )


def _analyse_performance_history(
    rev: str, limit_commits: int, sample_amount: int, print_all_results: bool
):
    repo = Repo()
    active_branch = repo.active_branch

    commits_with_results: List[Tuple[Commit, float]] = []
    for commit in repo.iter_commits(rev=rev, max_count=limit_commits):
        repo.git.checkout(commit)
        head = repo.head.commit

        _reinstall_snowcli()
        all_results = _run_snowcli_x_times_and_gather_times(sample_amount)

        chosen_result = all_results[int(sample_amount * 0.9)]
        commits_with_results.append((commit, chosen_result))
        _print_results_for_single_commit(
            head, chosen_result, all_results, print_all_results
        )

    _print_summary_performance_descending(commits_with_results)
    repo.git.checkout(active_branch)


def main(
    rev: str = "HEAD",
    limit_commits: int = 50,
    sample_amount: int = 20,
    print_all_results: bool = True,
):
    _analyse_performance_history(
        rev=rev,
        limit_commits=limit_commits,
        sample_amount=sample_amount,
        print_all_results=print_all_results,
    )


if __name__ == "__main__":
    typer.run(main)
