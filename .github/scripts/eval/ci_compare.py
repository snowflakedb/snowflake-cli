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

"""CI orchestration script for the E2E reviewer A/B eval.

Wraps :func:`compare.main` to add GitHub PR comment lifecycle management:

- deletes any previous eval-bot comment on the PR (``<!-- cortex-eval-bot -->`` marker)
- posts the comparison report (``comparison.md``) as a new PR comment
- truncates to GitHub's 65 000-character comment limit

All :mod:`compare` CLI flags are accepted and passed through verbatim. The only
additions are ``--pr-number`` and ``--repo`` for GitHub comment management.

Exit codes mirror :func:`compare.main`:

  0 = success, no regressions
  1 = error (agent failure, missing results dir, etc.)
  2 = regressions detected — PR comment is prefixed with a warning banner;
      the job exits non-zero so the check is visible, but the caller (workflow)
      decides whether to block the PR on this.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_EVAL_DIR = Path(__file__).resolve().parent
if str(_EVAL_DIR) not in sys.path:
    sys.path.insert(0, str(_EVAL_DIR))

import compare  # noqa: E402
import scorer  # noqa: E402

# HTML marker used to find and delete the previous eval comment on re-runs
COMMENT_MARKER = "<!-- cortex-eval-bot -->"

# GitHub's maximum comment body size.  Anything over this is silently rejected,
# so we truncate with an explicit notice.
MAX_COMMENT_LEN = 65_000


# ---------------------------------------------------------------------------
# GitHub comment helpers
# ---------------------------------------------------------------------------


def delete_previous_comment(repo: str, pr_number: int) -> None:
    """Delete any previous eval-bot comment on *pr_number*."""
    try:
        out = subprocess.run(
            [
                "gh",
                "api",
                f"repos/{repo}/issues/{pr_number}/comments",
                "--paginate",
                "--jq",
                f'.[] | select(.body | contains("{COMMENT_MARKER}")) | .id',
            ],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        for comment_id in out.strip().splitlines():
            if not comment_id.strip():
                continue
            subprocess.run(
                [
                    "gh",
                    "api",
                    "-X",
                    "DELETE",
                    f"repos/{repo}/issues/comments/{comment_id.strip()}",
                ],
                capture_output=True,
                timeout=15,
            )
            print(f"  Deleted previous eval comment {comment_id.strip()}")
    except Exception as e:
        print(f"  Warning: could not delete previous comment: {e}", file=sys.stderr)


def post_comment(repo: str, pr_number: int, body: str) -> None:
    """Post *body* as a new PR comment, truncating if it exceeds GitHub's limit."""
    if len(body) > MAX_COMMENT_LEN:
        body = body[:MAX_COMMENT_LEN] + "\n\n… [comment truncated]"
    try:
        subprocess.run(
            [
                "gh",
                "pr",
                "comment",
                str(pr_number),
                "--repo",
                repo,
                "--body",
                body,
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to post comment: {e}", file=sys.stderr)


def trim_to_overall(markdown: str) -> str:
    """Keep only the header, Regressions, and Overall sections.

    Slice breakdowns (``## By <slice>``) are omitted from the PR comment to
    keep it concise; the full report is preserved in the artifact file.
    """
    # render_markdown() appends slice sections as "## By <slice_type>".
    # Everything before the first such heading is the part we want.
    marker = "\n## By "
    idx = markdown.find(marker)
    return markdown[:idx].rstrip() if idx != -1 else markdown.rstrip()


def build_comment_body(md_path: Path, exit_code: int, run_url: str) -> str:
    """Assemble the PR comment body from the comparison markdown.

    Only the Regressions and Overall sections are included; slice breakdowns
    are available in the full artifact.
    """
    if md_path.exists():
        body = trim_to_overall(md_path.read_text())
        if exit_code == 2:
            header = "> ⚠️ **Regressions detected** — review the metrics below.\n\n"
            body = header + body
        elif exit_code not in (0, 2):
            body = (
                f"> ⚠️ **Eval partially failed (exit {exit_code})** — "
                f"see [workflow logs]({run_url}).\n\n"
            ) + body
    else:
        body = (
            f"⚠️ **E2E reviewer eval could not produce results.**  "
            f"Check the [workflow logs]({run_url}) for details."
        )
    footer = (
        f"\n\n---\n_Full results (per-case scores, slice breakdowns): "
        f"[workflow artifacts]({run_url})_"
    )
    return f"{COMMENT_MARKER}\n{body}{footer}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(
        description=(
            "CI wrapper: run the A/B eval (compare.py) and post/update the "
            "GitHub PR comment with the comparison report."
        )
    )

    # GitHub comment management — CI-specific additions
    p.add_argument(
        "--pr-number",
        type=int,
        default=None,
        help=(
            "GitHub PR number to post the comment on.  "
            "Comment management is skipped when absent (e.g. workflow_dispatch)."
        ),
    )
    p.add_argument(
        "--repo",
        default=os.environ.get("GITHUB_REPOSITORY", ""),
        help="GitHub repository in owner/name form.  Default: $GITHUB_REPOSITORY.",
    )

    # compare.main() pass-through flags
    p.add_argument(
        "-i",
        "--case",
        action="append",
        dest="patterns",
        metavar="PATTERN",
        help="case name or glob to run (repeatable; default: all)",
    )
    p.add_argument("--base-ref", default=compare.runner.DEFAULT_BASE_REF)
    p.add_argument("--baseline-model", default=None)
    p.add_argument("--candidate-model", default=None)
    p.add_argument("--connection", default=compare.runner.DEFAULT_CONNECTION)
    p.add_argument("--venv-dir", default=None)
    p.add_argument("--results-dir", default="eval_results")
    p.add_argument("--timeout", type=int, default=compare.runner.rev.AGENT_TIMEOUT_SEC)
    p.add_argument("--no-cache", action="store_true")
    p.add_argument(
        "--judge",
        action="store_true",
        help="Run the LLM judge (requires trulens-providers-cortex installed).",
    )
    p.add_argument("--judge-model", default=scorer.DEFAULT_JUDGE_MODEL)

    args = p.parse_args(argv)

    results_dir = Path(args.results_dir).resolve()
    results_dir.mkdir(parents=True, exist_ok=True)
    md_out = results_dir / "comparison.md"
    json_out = results_dir / "comparison.json"

    # Build the argv list for compare.main()
    compare_argv: list[str] = [
        "--base-ref",
        args.base_ref,
        "--connection",
        args.connection,
        "--results-dir",
        str(results_dir),
        "--markdown-out",
        str(md_out),
        "--json-out",
        str(json_out),
        "--timeout",
        str(args.timeout),
    ]
    for pat in args.patterns or []:
        compare_argv += ["-i", pat]
    if args.baseline_model:
        compare_argv += ["--baseline-model", args.baseline_model]
    if args.candidate_model:
        compare_argv += ["--candidate-model", args.candidate_model]
    if args.venv_dir:
        compare_argv += ["--venv-dir", args.venv_dir]
    if args.no_cache:
        compare_argv.append("--no-cache")
    if args.judge:
        compare_argv += ["--judge", "--judge-model", args.judge_model]

    exit_code = compare.main(compare_argv)

    # Post / update the PR comment when we have a PR number to target.
    if args.pr_number:
        run_url = (
            f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
            f"{args.repo}/actions/runs/"
            f"{os.environ.get('GITHUB_RUN_ID', '')}"
        )
        body = build_comment_body(md_out, exit_code, run_url)
        delete_previous_comment(args.repo, args.pr_number)
        post_comment(args.repo, args.pr_number, body)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
