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
from dataclasses import dataclass, field

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
    "claude-opus-4-6",
    "claude-sonnet-4-6",
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
    You are an end-to-end verification bot for the snowflake-cli project,
    a Python CLI tool invoked as `snow <subcommand> [options]`.

    Your ONLY job is to determine whether the PR changes CLI behavior that
    can be verified end-to-end, and if so, suggest concrete test commands.

    You do NOT perform static code review. No commenting on code style,
    security, test coverage, or architecture. Only dynamic E2E verification.

    ## Decision: Does this PR need E2E testing?

    Answer NEEDS_E2E: YES only if the PR changes:
    - A CLI command's behavior, options, arguments, or output format
    - SQL queries or API calls that the CLI makes
    - Connection handling, authentication, or session logic
    - Any user-facing functionality of the `snow` CLI

    Answer NEEDS_E2E: NO if the PR only changes:
    - CI/CD workflows, GitHub Actions, or automation scripts
    - Documentation, README, release notes
    - Tests only (no production code changes)
    - Internal refactoring with no behavior change
    - Build/packaging configuration

    ## If NEEDS_E2E: YES, suggest test commands following these rules:
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
    9. Only suggest commands that directly test the CHANGED behavior.
       Do not suggest generic smoke tests unrelated to the diff.
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

    ## Your Task

    First, determine whether this PR changes CLI behavior that can be
    verified end-to-end.

    Output exactly one of:
    NEEDS_E2E: YES
    NEEDS_E2E: NO

    If NO, explain in one sentence why no E2E testing is needed, then stop.

    If YES, explain what behavioral change needs verification, then list
    test commands. For each command, prefix the line with CMD: and follow
    the rules in your system prompt. After each CMD line, add a line
    starting with EXPECT: describing what a successful output looks like.

    Example format:
    NEEDS_E2E: YES
    The PR modifies the `snow cortex complete` command to accept a new --temperature flag.

    CMD: snow cortex complete "Hello" --model llama3.1-8b --temperature 0.5 --connection integration
    EXPECT: Output contains a response, exit code 0.
    CMD: snow cortex complete --help
    EXPECT: Output lists --temperature as an available option.
"""
)

EXECUTION_RESULTS_PROMPT = textwrap.dedent(
    """\
    I executed the commands you suggested. Here are the results:

    {execution_results_formatted}

    Based on these results, suggest read-only SQL verification queries to
    confirm the expected side effects actually happened in Snowflake.

    Rules:
    1. Only SELECT, SHOW, DESCRIBE, LIST, or CALL (for read-only procedures).
    2. Never use INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, GRANT, REVOKE.
    3. Each query on its own line, prefixed with QUERY: exactly.
    4. After each QUERY line, add VERIFY: describing what the result should
       show if the command worked correctly.
    5. At most 6 queries.
    6. If no commands had verifiable side effects, say so and skip queries.
"""
)

VERIFICATION_RESULTS_PROMPT = textwrap.dedent(
    """\
    I executed the verification queries. Here are the results:

    {verification_results_formatted}

    Now produce the final GitHub PR comment with these exact sections:

    ### Summary
    One paragraph summarizing what the PR does and whether E2E verification
    passed. If no E2E testing was needed, explain why.

    ### E2E Test Results
    For each command that was run: show the command, its exit code, whether
    the actual output matched the expected output, and a one-line verdict.
    For commands that were rejected or timed out: explain why.

    ### Side-Effect Verification
    For each verification query: show the query, the result, whether it
    matched expectations, and a one-line verdict. If no verification
    queries were run, state why.

    ### Verdict
    One of: PASS / FAIL / SKIP
    - PASS: all tests passed and side effects verified
    - FAIL: one or more tests failed unexpectedly
    - SKIP: no E2E testing was needed for this PR
    Followed by one sentence justifying the verdict.
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
                    or "not allowed" in err_str.lower()
                    or "not found" in err_str.lower()
                    or "unknown model" in err_str.lower()
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
        model = os.environ.get("CORTEX_MODEL", "claude-opus-4-6")
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
# SQLVerifier
# ---------------------------------------------------------------------------

MAX_VERIFICATION_QUERIES = 6
QUERY_TIMEOUT_SECONDS = 30

ALLOWED_SQL_PREFIXES = (
    "SELECT",
    "SHOW",
    "DESCRIBE",
    "DESC",
    "LIST",
    "CALL",
    "WITH",
)

BLOCKED_SQL_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|GRANT|REVOKE|TRUNCATE|MERGE|COPY|PUT|GET|REMOVE)\b",
    re.IGNORECASE,
)


@dataclass
class VerificationResult:
    query: str
    expected: str
    rows: list[dict] = field(default_factory=list)
    error: str = ""
    rejected: bool = False
    rejection_reason: str = ""


class SQLVerifier:
    """Executes read-only SQL verification queries against Snowflake."""

    def validate_query(self, raw: str) -> tuple[bool, str]:
        stripped = raw.strip().rstrip(";")
        upper = stripped.upper().lstrip()
        if not any(upper.startswith(p) for p in ALLOWED_SQL_PREFIXES):
            return (
                False,
                f"Query must start with one of: {', '.join(ALLOWED_SQL_PREFIXES)}",
            )
        if BLOCKED_SQL_PATTERNS.search(stripped):
            return False, "Query contains a blocked DDL/DML keyword"
        return True, ""

    def run(
        self,
        queries: list[tuple[str, str]],
        connection: snowflake.connector.SnowflakeConnection,
    ) -> list[VerificationResult]:
        results = []
        for raw_query, expected in queries[:MAX_VERIFICATION_QUERIES]:
            valid, reason = self.validate_query(raw_query)
            if not valid:
                results.append(
                    VerificationResult(
                        query=raw_query,
                        expected=expected,
                        rejected=True,
                        rejection_reason=reason,
                    )
                )
                continue

            cursor = connection.cursor()
            try:
                cursor.execute(raw_query, timeout=QUERY_TIMEOUT_SECONDS)
                columns = (
                    [desc[0] for desc in cursor.description]
                    if cursor.description
                    else []
                )
                rows = []
                for row in cursor.fetchmany(20):
                    rows.append(dict(zip(columns, row)))
                results.append(
                    VerificationResult(
                        query=raw_query,
                        expected=expected,
                        rows=rows,
                    )
                )
            except Exception as e:
                results.append(
                    VerificationResult(
                        query=raw_query,
                        expected=expected,
                        error=str(e)[:500],
                    )
                )
            finally:
                cursor.close()
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
        """Execute the review pipeline in a single Cortex conversation."""

        # Single conversation history maintained across all turns
        conversation: list[dict] = [
            {"role": "system", "content": REVIEWER_SYSTEM_PROMPT},
        ]

        # Turn 1: Analyze diff and decide if E2E is needed
        print("[Turn 1] Sending diff to Cortex for E2E analysis...")
        user_prompt = REVIEWER_USER_PROMPT_TEMPLATE.format(
            pr_title=self.pr.title,
            pr_body=(self.pr.body[:3000] or "(no description provided)"),
            changed_files="\n".join(f"- {f}" for f in self.pr.changed_files),
            diff=self.pr.diff,
        )
        conversation.append({"role": "user", "content": user_prompt})
        turn1_response = self.cortex.complete(conversation)
        conversation.append({"role": "assistant", "content": turn1_response})
        print(f"  Analysis received ({len(turn1_response)} chars)")

        # Check if Cortex determined E2E testing is needed
        needs_e2e = "NEEDS_E2E: YES" in turn1_response
        if not needs_e2e:
            print("  Cortex determined: no E2E testing needed")
            return self._compose_comment(
                "### Summary\n\n"
                + turn1_response.split("NEEDS_E2E: NO")[-1].strip()
                + "\n\n### Verdict\n**SKIP** — No CLI behavioral changes "
                "detected in this PR; E2E testing is not applicable."
            )

        print("  Cortex determined: E2E testing IS needed")

        # Execute suggested commands
        print("[Execute] Running suggested commands in sandbox...")
        commands = self._parse_commands(turn1_response)
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

        # Turn 2: Feed execution results, ask for verification queries
        print("[Turn 2] Feeding results, requesting verification queries...")
        results_text = self._format_results(results)
        conversation.append(
            {
                "role": "user",
                "content": EXECUTION_RESULTS_PROMPT.format(
                    execution_results_formatted=results_text
                    or "(no commands were executed)",
                ),
            }
        )
        turn2_response = self.cortex.complete(conversation)
        conversation.append({"role": "assistant", "content": turn2_response})

        # Execute verification queries
        verification_results: list[VerificationResult] = []
        queries = self._parse_verification_queries(turn2_response)
        if queries:
            print(f"[Execute] Running {len(queries)} verification queries...")
            verifier = SQLVerifier()
            verification_results = verifier.run(queries, self.cortex.connection)
            for v in verification_results:
                status = (
                    "REJECTED"
                    if v.rejected
                    else "ERROR"
                    if v.error
                    else f"{len(v.rows)} rows"
                )
                print(f"  [{status}] {v.query[:80]}")
        else:
            print("  No verification queries suggested")

        # Turn 3: Feed verification results, get final report
        print("[Turn 3] Requesting final report...")
        verification_text = self._format_verification_results(verification_results)
        conversation.append(
            {
                "role": "user",
                "content": VERIFICATION_RESULTS_PROMPT.format(
                    verification_results_formatted=verification_text
                    or "(no verification queries were needed or executed)",
                ),
            }
        )
        final_response = self.cortex.complete(conversation)

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

    def _parse_verification_queries(self, text: str) -> list[tuple[str, str]]:
        """Extract QUERY:/VERIFY: pairs from Cortex response."""
        lines = text.splitlines()
        queries = []
        current_query = None
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("QUERY:"):
                current_query = stripped.split("QUERY:", 1)[1].strip()
            elif stripped.startswith("VERIFY:") and current_query:
                expected = stripped.split("VERIFY:", 1)[1].strip()
                queries.append((current_query, expected))
                current_query = None
        # Handle trailing QUERY: without VERIFY:
        if current_query:
            queries.append((current_query, "(no expectation provided)"))
        return queries

    def _format_verification_results(self, results: list[VerificationResult]) -> str:
        parts = []
        for v in results:
            if v.rejected:
                parts.append(
                    f"QUERY: `{v.query}`\n"
                    f"EXPECTED: {v.expected}\n"
                    f"STATUS: REJECTED\n"
                    f"REASON: {v.rejection_reason}"
                )
            elif v.error:
                parts.append(
                    f"QUERY: `{v.query}`\n"
                    f"EXPECTED: {v.expected}\n"
                    f"STATUS: ERROR\n"
                    f"ERROR: {v.error}"
                )
            else:
                rows_str = json.dumps(v.rows, indent=2, default=str)[:3000]
                parts.append(
                    f"QUERY: `{v.query}`\n"
                    f"EXPECTED: {v.expected}\n"
                    f"ROWS RETURNED: {len(v.rows)}\n"
                    f"RESULTS:\n{rows_str}"
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

    # Steps 3-6: Run review pipeline
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

    # Step 7: Post the review comment
    print("[Step 7] Posting review comment to PR...")
    _post_comment(repo, pr_number, comment_body)
    print("Done.")


if __name__ == "__main__":
    main()
