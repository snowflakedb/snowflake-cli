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

"""
Cortex AI End-to-End PR Review Script

Uses Snowflake Cortex to analyze PR changes, suggest verification commands,
execute them in a sandbox, and post a comprehensive review comment.

Intended to run inside a GitHub Actions workflow with:
  - `snow` CLI installed from the PR branch (`pip install .`)
  - Snowflake credentials in SNOWFLAKE_CONNECTIONS_INTEGRATION_* env vars
  - GH_TOKEN for `gh` CLI access
  - PR_NUMBER and PR_REPO env vars
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import textwrap
import time
import traceback
from dataclasses import dataclass

import snowflake.connector
from snowflake.cli._app.snow_connector import update_connection_details_with_private_key

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALLOWED_SNOW_SUBCOMMANDS = frozenset(
    [
        "cortex",
        "sql",
        "object",
        "stage",
        "git",
        "streamlit",
        "snowpark",
        "connection",
        "helpers",
        "notebook",
    ]
)

MAX_DIFF_CHARS = 40_000
MAX_COMMANDS_TO_RUN = 8
COMMAND_TIMEOUT_SECONDS = 45
CORTEX_REQUEST_TIMEOUT = 120

# Models to try in order of preference if the primary model is unavailable
MODEL_FALLBACK_CHAIN = [
    "claude-4-opus",
    "claude-4-sonnet",
    "llama3.1-405b",
    "llama3.1-70b",
]

_ENV_PREFIX = "SNOWFLAKE_CONNECTIONS_INTEGRATION"

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

REVIEWER_SYSTEM_PROMPT = textwrap.dedent(
    """\
    You are an expert code reviewer for the snowflake-cli project, a Python CLI
    tool that wraps the Snowflake REST and SQL APIs. Your analysis must be
    precise and grounded in the actual diff provided.

    The CLI is invoked as `snow <subcommand> [options]`. You know its full
    command tree: cortex, sql, object, stage, git, streamlit, snowpark,
    connection, helpers, notebook, etc.

    When asked to suggest test commands, follow these rules strictly:
    1. Every command MUST start with the literal word `snow`.
    2. Only use subcommands from this allowlist: cortex, sql, object, stage,
       git, streamlit, snowpark, connection, helpers, notebook.
    3. Commands MUST be read-only or idempotent. Never use drop, delete,
       truncate, remove, overwrite, replace, or undeploy.
    4. Each command must be on its own line, prefixed with CMD: exactly.
    5. Suggest at most 6 commands.
    6. Do NOT use shell pipes, redirections, or variable substitution.
    7. Commands may use --help to verify that a new or changed subcommand is
       present and has the expected interface.
    8. For commands that need a connection, use: --connection integration
"""
)

REVIEWER_USER_PROMPT_TEMPLATE = textwrap.dedent(
    """\
    ## Pull Request: {pr_title}

    ### PR Description
    {pr_body}

    ### Changed Files
    {changed_files}

    ### Diff
    ```diff
    {diff}
    ```

    ## Your Tasks

    ### Part 1 - Code Review Analysis
    Provide a structured review covering:
    - **Purpose**: What does this PR accomplish? Does the description match
      the diff?
    - **Correctness**: Are there obvious bugs, missed edge cases, or broken
      logic?
    - **CLI Contract**: Does the PR change any command name, option, or output
      format? If so, is it backward-compatible?
    - **Test Coverage**: Does the diff include or update tests? Are the tests
      adequate?
    - **Security**: Any hardcoded credentials, unsafe subprocess calls, or
      SQL injection risks?

    ### Part 2 - Suggested Test Commands
    List concrete `snow` CLI commands that would validate the changes
    end-to-end. For each command, prefix the line with CMD: and follow the
    rules in your system prompt. After each CMD line, add a line starting
    with EXPECT: describing what a successful output looks like.

    Example format:
    CMD: snow cortex complete "Is 2+2 equal to 4? Answer yes or no." --model llama3.1-8b --connection integration
    EXPECT: Output contains "yes" (case-insensitive), exit code 0.
"""
)

FINAL_ASSESSOR_SYSTEM_PROMPT = textwrap.dedent(
    """\
    You are a CI automation bot that synthesizes code review analysis with
    actual command execution results to produce a final PR review comment.

    Be factual. Base your verdict only on evidence: the original analysis and
    the execution results. Do not hallucinate command outputs. If a command
    was not run (rejected or timed out), say so explicitly and explain why.

    Format your output as valid GitHub Markdown. Use the exact section headers
    below. Do not add extra sections.
"""
)

FINAL_ASSESSOR_USER_PROMPT_TEMPLATE = textwrap.dedent(
    """\
    ## Original Review Analysis
    {review_analysis}

    ## Command Execution Results
    {execution_results_formatted}

    ## Instructions
    Produce the final GitHub PR review comment with these exact sections:

    ### Summary
    One paragraph (3-5 sentences) summarizing what the PR does and the
    overall verdict.

    ### Code Review Findings
    Bullet list of issues, concerns, or praise from the static analysis.
    Label each bullet as one of: [BUG], [CONCERN], [SUGGESTION], [PRAISE].

    ### E2E Test Results
    For each command that was run: show the command, its exit code, whether
    the actual output matched the expected output, and a one-line verdict.
    For commands that were rejected or timed out: explain why.

    ### Verdict
    One of: APPROVE / REQUEST_CHANGES / NEEDS_DISCUSSION
    Followed by one sentence justifying the verdict.

    ### Caveats
    Note that this review was generated automatically by Snowflake Cortex AI
    and should be treated as a first-pass analysis, not a substitute for
    human review.
"""
)

# ---------------------------------------------------------------------------
# CortexClient
# ---------------------------------------------------------------------------


@dataclass
class CortexClient:
    connection: snowflake.connector.SnowflakeConnection
    model: str

    def complete(self, messages: list[dict]) -> str:
        """Call Cortex COMPLETE, trying fallback models if unavailable."""
        models_to_try = [self.model] + [
            m for m in MODEL_FALLBACK_CHAIN if m != self.model
        ]
        last_err = None
        for model in models_to_try:
            try:
                result = self._complete_sql(messages, model)
                if model != self.model:
                    print(f"  Using fallback model: {model}")
                    self.model = model
                return result
            except Exception as e:
                err_str = str(e)
                print(f"  Model '{model}' failed: {err_str[:200]}")
                if (
                    "unavailable" in err_str.lower()
                    or "not supported" in err_str.lower()
                ):
                    last_err = e
                    continue
                # Non-model error — don't try other models, just raise
                raise
        raise RuntimeError(f"All models failed. Last error: {last_err}")

    def _complete_sql(self, messages: list[dict], model: str) -> str:
        """Call SNOWFLAKE.CORTEX.COMPLETE via SQL."""
        conversation = json.dumps(messages)
        escaped = conversation.replace("\\", "\\\\").replace("'", "\\'")
        query = (
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE("
            f"'{model}', PARSE_JSON('{escaped}'), {{}}"
            f") AS CORTEX_RESULT"
        )
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("Cortex SQL returned no rows")
            raw_result = str(row[0])
            json_result = json.loads(raw_result)
            return json_result["choices"][0]["messages"]
        finally:
            cursor.close()

    def _complete_rest(self, messages: list[dict]) -> str:
        """Call Cortex COMPLETE via the REST backend (CortexInferenceService)."""
        from snowflake.core import Root
        from snowflake.core.cortex.inference_service import CortexInferenceService
        from snowflake.core.cortex.inference_service._generated.models import (
            CompleteRequest,
        )
        from snowflake.core.cortex.inference_service._generated.models.complete_request_messages_inner import (
            CompleteRequestMessagesInner,
        )

        root = Root(self.connection)
        req = CompleteRequest(
            model=self.model,
            messages=[CompleteRequestMessagesInner(**m) for m in messages],
            stream=True,
        )
        service = CortexInferenceService(root=root)
        raw = service.complete(complete_request=req)

        result = ""
        for event in raw.events():
            try:
                chunk = json.loads(event.data)
            except json.JSONDecodeError:
                raise RuntimeError(f"Cortex returned unparsable response: {event.data}")
            try:
                result += chunk["choices"][0]["delta"]["content"]
            except (KeyError, IndexError):
                if chunk.get("error"):
                    raise RuntimeError(f"Cortex mid-stream error: {event.data}")
        return result

    @classmethod
    def from_env(cls) -> CortexClient:
        """Build connection from SNOWFLAKE_CONNECTIONS_INTEGRATION_* env vars."""
        config: dict = {
            "application": "CORTEX_PR_REVIEW",
            "authenticator": os.environ.get(
                f"{_ENV_PREFIX}_AUTHENTICATOR", "SNOWFLAKE_JWT"
            ),
            "account": os.environ[f"{_ENV_PREFIX}_ACCOUNT"],
            "user": os.environ[f"{_ENV_PREFIX}_USER"],
        }

        optional_keys = {
            "host": f"{_ENV_PREFIX}_HOST",
            "database": f"{_ENV_PREFIX}_DATABASE",
            "warehouse": f"{_ENV_PREFIX}_WAREHOUSE",
            "role": f"{_ENV_PREFIX}_ROLE",
            "private_key_raw": f"{_ENV_PREFIX}_PRIVATE_KEY_RAW",
            "private_key_file": f"{_ENV_PREFIX}_PRIVATE_KEY_FILE",
        }
        for param, env_key in optional_keys.items():
            val = os.environ.get(env_key)
            if val:
                config[param] = val

        # Also check PRIVATE_KEY_PATH as an alias for private_key_file
        if "private_key_file" not in config:
            pk_path = os.environ.get(f"{_ENV_PREFIX}_PRIVATE_KEY_PATH")
            if pk_path:
                config["private_key_file"] = pk_path

        config = {k: v for k, v in config.items() if v is not None}
        update_connection_details_with_private_key(config)

        conn = snowflake.connector.connect(**config)
        model = os.environ.get("CORTEX_MODEL", "claude-4-opus")
        return cls(connection=conn, model=model)


# ---------------------------------------------------------------------------
# PRFetcher
# ---------------------------------------------------------------------------


@dataclass
class PRMetadata:
    number: int
    title: str
    body: str
    diff: str
    changed_files: list[str]
    base_sha: str
    head_sha: str


class PRFetcher:
    def __init__(self, repo: str, pr_number: int):
        self.repo = repo
        self.pr_number = pr_number

    def fetch(self) -> PRMetadata:
        meta = self._gh_json(
            [
                "pr",
                "view",
                str(self.pr_number),
                "--json",
                "title,body,headRefOid,baseRefOid,files",
            ]
        )
        diff_raw = self._gh(["pr", "diff", str(self.pr_number)])
        diff = diff_raw[:MAX_DIFF_CHARS]
        if len(diff_raw) > MAX_DIFF_CHARS:
            diff += f"\n... [diff truncated at {MAX_DIFF_CHARS} chars]"

        return PRMetadata(
            number=self.pr_number,
            title=meta.get("title", ""),
            body=meta.get("body", "") or "",
            diff=diff,
            changed_files=[f["path"] for f in meta.get("files", [])],
            base_sha=meta.get("baseRefOid", ""),
            head_sha=meta.get("headRefOid", ""),
        )

    def _gh(self, args: list[str]) -> str:
        result = subprocess.run(
            ["gh", "--repo", self.repo] + args,
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
        return result.stdout

    def _gh_json(self, args: list[str]) -> dict:
        return json.loads(self._gh(args))


# ---------------------------------------------------------------------------
# CommandSandbox
# ---------------------------------------------------------------------------


@dataclass
class ExecutionResult:
    command: str
    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool = False
    rejected: bool = False
    rejection_reason: str = ""


class CommandSandbox:
    """
    Executes `snow` CLI commands in subprocess with strict safety controls:
    - Allowlist on subcommands
    - Block list on destructive keywords
    - No shell=True; arguments parsed via shlex
    - Per-command timeout
    - Output size caps
    """

    BLOCKED_OPERATION_PATTERNS = re.compile(
        r"\b(drop|delete|truncate|overwrite|replace|undeploy|remove)\b",
        re.IGNORECASE,
    )

    def validate_command(self, raw: str) -> tuple[bool, str, list[str]]:
        """Returns (is_valid, rejection_reason, argv_list)."""
        try:
            argv = shlex.split(raw)
        except ValueError as e:
            return False, f"Cannot parse command: {e}", []

        if not argv or argv[0] != "snow":
            return False, "Command must start with 'snow'", []

        if len(argv) < 2 or argv[1] not in ALLOWED_SNOW_SUBCOMMANDS:
            subcommand = argv[1] if len(argv) > 1 else ""
            return (
                False,
                f"Subcommand '{subcommand}' not in allowlist",
                [],
            )

        if self.BLOCKED_OPERATION_PATTERNS.search(raw):
            return False, "Command contains a blocked destructive keyword", []

        for arg in argv[2:]:
            if any(c in arg for c in (";", "|", "&", "`", "$", ">(", "<(")):
                return False, f"Suspicious character in argument: {arg!r}", []

        return True, "", argv

    def run(self, commands: list[str]) -> list[ExecutionResult]:
        results = []
        for raw_cmd in commands[:MAX_COMMANDS_TO_RUN]:
            valid, reason, argv = self.validate_command(raw_cmd)
            if not valid:
                results.append(
                    ExecutionResult(
                        command=raw_cmd,
                        stdout="",
                        stderr="",
                        exit_code=-1,
                        rejected=True,
                        rejection_reason=reason,
                    )
                )
                continue

            try:
                proc = subprocess.run(
                    argv,
                    capture_output=True,
                    text=True,
                    timeout=COMMAND_TIMEOUT_SECONDS,
                )
                results.append(
                    ExecutionResult(
                        command=raw_cmd,
                        stdout=proc.stdout[:4000],
                        stderr=proc.stderr[:2000],
                        exit_code=proc.returncode,
                    )
                )
            except subprocess.TimeoutExpired:
                results.append(
                    ExecutionResult(
                        command=raw_cmd,
                        stdout="",
                        stderr="",
                        exit_code=-1,
                        timed_out=True,
                    )
                )
        return results


# ---------------------------------------------------------------------------
# ReviewPipeline
# ---------------------------------------------------------------------------


class ReviewPipeline:
    def __init__(
        self,
        cortex: CortexClient,
        pr: PRMetadata,
        sandbox: CommandSandbox,
        repo: str,
    ):
        self.cortex = cortex
        self.pr = pr
        self.sandbox = sandbox
        self.repo = repo

    def run(self) -> str:
        """Execute the review pipeline and return the final comment body."""

        # Step 3: First Cortex call - review + command generation
        print("[Step 3] Sending diff to Cortex for review analysis...")
        user_prompt = REVIEWER_USER_PROMPT_TEMPLATE.format(
            pr_title=self.pr.title,
            pr_body=(self.pr.body[:3000] or "(no description provided)"),
            changed_files="\n".join(f"- {f}" for f in self.pr.changed_files),
            diff=self.pr.diff,
        )
        review_response = self.cortex.complete(
            [
                {"role": "system", "content": REVIEWER_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ]
        )
        print(f"  Review analysis received ({len(review_response)} chars)")

        # Step 4: Parse CMD: lines and execute them
        print("[Step 4] Executing suggested commands in sandbox...")
        commands = self._parse_commands(review_response)
        print(f"  Found {len(commands)} commands to execute")
        results = self.sandbox.run(commands)
        for r in results:
            status = (
                "REJECTED"
                if r.rejected
                else "TIMEOUT"
                if r.timed_out
                else f"exit={r.exit_code}"
            )
            print(f"  [{status}] {r.command}")

        # Step 5: Second Cortex call - final assessment
        print("[Step 5] Sending execution results to Cortex for final assessment...")
        results_text = self._format_results(results)
        final_response = self.cortex.complete(
            [
                {"role": "system", "content": FINAL_ASSESSOR_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": FINAL_ASSESSOR_USER_PROMPT_TEMPLATE.format(
                        review_analysis=review_response,
                        execution_results_formatted=results_text
                        or "(no commands were suggested or all were rejected)",
                    ),
                },
            ]
        )

        return self._compose_comment(final_response)

    def _parse_commands(self, review_text: str) -> list[str]:
        """Extract CMD: lines from the review response."""
        return [
            line.split("CMD:", 1)[1].strip()
            for line in review_text.splitlines()
            if line.strip().startswith("CMD:")
        ]

    def _format_results(self, results: list[ExecutionResult]) -> str:
        parts = []
        for r in results:
            if r.rejected:
                parts.append(
                    f"COMMAND: `{r.command}`\n"
                    f"STATUS: REJECTED\n"
                    f"REASON: {r.rejection_reason}"
                )
            elif r.timed_out:
                parts.append(
                    f"COMMAND: `{r.command}`\n"
                    f"STATUS: TIMED OUT (>{COMMAND_TIMEOUT_SECONDS}s)"
                )
            else:
                parts.append(
                    f"COMMAND: `{r.command}`\n"
                    f"EXIT CODE: {r.exit_code}\n"
                    f"STDOUT:\n{r.stdout}\n"
                    f"STDERR:\n{r.stderr}"
                )
        return "\n\n---\n\n".join(parts)

    def _compose_comment(self, final_text: str) -> str:
        header = (
            "<!-- cortex-review-bot -->\n"
            "## Cortex AI PR Review\n\n"
            f"> Model: `{self.cortex.model}` | "
            f"Reviewed at: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n\n"
        )
        return header + final_text


# ---------------------------------------------------------------------------
# Comment helpers
# ---------------------------------------------------------------------------


def _post_comment(repo: str, pr_number: int, body: str) -> None:
    subprocess.run(
        ["gh", "pr", "comment", str(pr_number), "--repo", repo, "--body", body],
        check=True,
    )


def _post_error_comment(repo: str, pr_number: int, message: str) -> None:
    body = (
        "<!-- cortex-review-bot -->\n"
        "## Cortex AI PR Review - Error\n\n"
        "The automated review encountered an error and could not complete:\n\n"
        f"{message}\n\n"
        "_This is an automated message. Please check the workflow logs for details._"
    )
    subprocess.run(
        ["gh", "pr", "comment", str(pr_number), "--repo", repo, "--body", body],
        check=False,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    pr_number = int(os.environ["PR_NUMBER"])
    repo = os.environ["PR_REPO"]

    # Step 1: Fetch PR metadata
    print(f"[Step 1] Fetching PR #{pr_number} metadata from {repo}...")
    fetcher = PRFetcher(repo=repo, pr_number=pr_number)
    try:
        pr = fetcher.fetch()
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to fetch PR metadata: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"  Title: {pr.title}")
    print(f"  Changed files: {len(pr.changed_files)}")
    print(f"  Diff size: {len(pr.diff)} chars")

    # Step 2: Verify CLI build
    print("[Step 2] Verifying CLI build...")
    result = subprocess.run(["snow", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR: `snow` binary not found after pip install", file=sys.stderr)
        sys.exit(1)
    print(f"  CLI version: {result.stdout.strip()}")

    # Connect to Snowflake for Cortex access
    print("[Step 2b] Connecting to Snowflake for Cortex access...")
    try:
        cortex = CortexClient.from_env()
    except Exception as e:
        _post_error_comment(
            repo, pr_number, f"Failed to connect to Snowflake:\n\n```\n{e}\n```"
        )
        sys.exit(1)

    # Steps 3-5: Run review pipeline
    sandbox = CommandSandbox()
    pipeline = ReviewPipeline(cortex=cortex, pr=pr, sandbox=sandbox, repo=repo)
    try:
        comment_body = pipeline.run()
    except Exception as e:
        tb = traceback.format_exc()
        _post_error_comment(
            repo,
            pr_number,
            f"Review pipeline failed:\n\n```\n{e}\n\n{tb}\n```",
        )
        sys.exit(1)
    finally:
        cortex.connection.close()

    # Step 6: Post the review comment
    print("[Step 6] Posting review comment to PR...")
    _post_comment(repo, pr_number, comment_body)
    print("Done.")


if __name__ == "__main__":
    main()
