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

MAX_DIFF_CHARS = 40_000
COMMAND_TIMEOUT_SECONDS = 60
QUERY_TIMEOUT_SECONDS = 30

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

AGENT_SYSTEM_PROMPT = textwrap.dedent(
    """\
    You are an autonomous end-to-end verification agent for the snowflake-cli
    project, a Python CLI tool invoked as `snow <subcommand> [options]`.

    You do NOT perform static code review. No commenting on code style,
    security, test coverage, or architecture. Your only job is dynamic E2E
    verification of CLI behavioral changes.

    ## Your environment

    You have a dedicated Snowflake playground database: {playground_db}
    This database is yours — you can CREATE, DROP, INSERT, ALTER anything
    in it freely. It will be destroyed after your run completes.

    The `snow` CLI is installed from the PR branch and configured with
    `--connection integration` pointing to this playground database.

    ## Tools

    ACTION: CMD <command>
      Executes any shell command on the runner. Typically `snow ...` CLI
      commands, but you can also use standard tools (e.g. `snow --help`,
      `snow sql -q "..."`, etc.). You will receive stdout, stderr, and
      exit code.

    ACTION: QUERY <sql>
      Executes any SQL statement against Snowflake (using the playground
      database). You will receive result rows or error messages.
      You can CREATE tables, stages, functions — anything needed to set
      up test scenarios and verify side effects.

    ACTION: REPORT
      Signals you are done. Everything after this line is your final
      report in GitHub Markdown.

    You can request multiple actions in one response (one per line). After
    each batch, I will execute them and show you the results. You can then
    request more actions based on what you learned.

    ## Workflow

    1. Analyze the PR diff to determine if CLI behavior changed.
    2. If NO behavioral changes: immediately output ACTION: REPORT with a
       short summary and verdict SKIP.
    3. If YES:
       - Set up any test fixtures you need (tables, stages, data) using
         ACTION: QUERY in the playground database.
       - Run CLI commands with ACTION: CMD to exercise the changed behavior.
       - Verify side effects with ACTION: QUERY.
       - Run follow-up commands if results are unexpected.
       - Clean up is optional — the database will be dropped automatically.
    4. When satisfied, output ACTION: REPORT with your findings.

    ## CMD tips
    - For commands needing a Snowflake connection: --connection integration
    - You can run `snow <command> --help` to inspect interfaces.
    - You have full access — create objects, deploy apps, run any snow command.

    ## REPORT format
    ACTION: REPORT

    ### Summary
    One paragraph on what the PR does and whether E2E verification passed.

    ### E2E Test Results
    Table or list of commands run, exit codes, whether output matched
    expectations, and a verdict per command.

    ### Side-Effect Verification
    Any SQL verification queries run and their results. If none needed,
    say so.

    ### Verdict
    One of: PASS / FAIL / SKIP
    Followed by one sentence justifying the verdict.
"""
)

AGENT_USER_PROMPT_TEMPLATE = textwrap.dedent(
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

    Analyze this PR. If CLI behavior changed, use your tools to verify it.
    If not, report SKIP immediately.
"""
)

AGENT_RESULTS_TEMPLATE = textwrap.dedent(
    """\
    Here are the results of your requested actions:

    {results}

    Continue your analysis. You may request more actions or output your
    final ACTION: REPORT.
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
# CommandRunner (unrestricted)
# ---------------------------------------------------------------------------


class CommandRunner:
    """Executes shell commands via subprocess. No restrictions — the agent
    operates in an ephemeral CI runner with a playground database."""

    def run(self, raw_cmd: str) -> tuple[str, str, int, bool]:
        """Returns (stdout, stderr, exit_code, timed_out)."""
        try:
            argv = shlex.split(raw_cmd)
        except ValueError as e:
            return "", f"Cannot parse command: {e}", -1, False

        try:
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=COMMAND_TIMEOUT_SECONDS,
            )
            return proc.stdout[:8000], proc.stderr[:4000], proc.returncode, False
        except subprocess.TimeoutExpired:
            return "", "", -1, True


# ---------------------------------------------------------------------------
# QueryRunner (unrestricted)
# ---------------------------------------------------------------------------


class QueryRunner:
    """Executes any SQL against Snowflake. The agent operates in an
    ephemeral playground database that is dropped after the run."""

    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self.connection = connection

    def run(self, raw_query: str) -> tuple[list[dict], str]:
        """Returns (rows, error). Rows is a list of dicts, error is empty on success."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(raw_query, timeout=QUERY_TIMEOUT_SECONDS)
            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            rows = []
            for row in cursor.fetchmany(50):
                rows.append(dict(zip(columns, row)))
            return rows, ""
        except Exception as e:
            return [], str(e)[:1000]
        finally:
            cursor.close()


# ---------------------------------------------------------------------------
# AgentLoop
# ---------------------------------------------------------------------------

MAX_AGENT_TURNS = 50


class AgentLoop:
    """Cortex-driven agent loop: Cortex decides what to run, script executes."""

    def __init__(
        self,
        cortex: CortexClient,
        pr: PRMetadata,
        cmd_runner: CommandRunner,
        query_runner: QueryRunner,
        playground_db: str,
    ):
        self.cortex = cortex
        self.pr = pr
        self.cmd_runner = cmd_runner
        self.query_runner = query_runner
        self.playground_db = playground_db
        self.conversation: list[dict] = [
            {
                "role": "system",
                "content": AGENT_SYSTEM_PROMPT.format(
                    playground_db=playground_db,
                ),
            },
        ]

    def run(self) -> str:
        """Run the agent loop until Cortex outputs ACTION: REPORT."""

        # Initial turn: send the PR diff
        user_prompt = AGENT_USER_PROMPT_TEMPLATE.format(
            pr_title=self.pr.title,
            pr_body=(self.pr.body[:3000] or "(no description provided)"),
            changed_files="\n".join(f"- {f}" for f in self.pr.changed_files),
            diff=self.pr.diff,
        )
        self.conversation.append({"role": "user", "content": user_prompt})

        for turn in range(1, MAX_AGENT_TURNS + 1):
            print(f"[Turn {turn}] Calling Cortex...")
            response = self.cortex.complete(self.conversation)
            self.conversation.append({"role": "assistant", "content": response})
            print(f"  Response: {len(response)} chars")

            # Parse actions from response
            actions = self._parse_actions(response)

            # Check for REPORT action — we're done
            report = self._extract_report(response)
            if report is not None:
                print(f"  Agent produced final report (turn {turn})")
                return self._compose_comment(report)

            if not actions:
                # No actions and no report — nudge the agent
                print("  No actions found, nudging agent...")
                self.conversation.append(
                    {
                        "role": "user",
                        "content": "No actions detected. Please use ACTION: CMD, "
                        "ACTION: QUERY, or ACTION: REPORT to proceed.",
                    }
                )
                continue

            # Execute all requested actions
            print(f"  Executing {len(actions)} actions...")
            results_parts = []
            for action_type, action_value in actions:
                if action_type == "CMD":
                    result = self._execute_cmd(action_value)
                    results_parts.append(result)
                elif action_type == "QUERY":
                    result = self._execute_query(action_value)
                    results_parts.append(result)

            # Feed results back
            results_text = "\n\n---\n\n".join(results_parts)
            self.conversation.append(
                {
                    "role": "user",
                    "content": AGENT_RESULTS_TEMPLATE.format(results=results_text),
                }
            )

        # Max turns reached — ask for final report
        print(f"  Max turns ({MAX_AGENT_TURNS}) reached, forcing report...")
        self.conversation.append(
            {
                "role": "user",
                "content": "Maximum turns reached. Please output ACTION: REPORT "
                "with your findings so far.",
            }
        )
        response = self.cortex.complete(self.conversation)
        report = self._extract_report(response)
        if report:
            return self._compose_comment(report)
        return self._compose_comment(response)

    def _parse_actions(self, text: str) -> list[tuple[str, str]]:
        """Extract ACTION: CMD and ACTION: QUERY lines."""
        actions = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("ACTION: CMD "):
                cmd = stripped[len("ACTION: CMD ") :].strip()
                actions.append(("CMD", cmd))
            elif stripped.startswith("ACTION: QUERY "):
                query = stripped[len("ACTION: QUERY ") :].strip()
                actions.append(("QUERY", query))
        return actions

    def _extract_report(self, text: str) -> str | None:
        """Extract everything after ACTION: REPORT."""
        marker = "ACTION: REPORT"
        idx = text.find(marker)
        if idx == -1:
            return None
        return text[idx + len(marker) :].strip()

    def _execute_cmd(self, raw_cmd: str) -> str:
        stdout, stderr, exit_code, timed_out = self.cmd_runner.run(raw_cmd)
        if timed_out:
            print(f"  [TIMEOUT] {raw_cmd}")
            return (
                f"**CMD:** `{raw_cmd}`\n"
                f"**STATUS:** TIMED OUT (>{COMMAND_TIMEOUT_SECONDS}s)"
            )
        print(f"  [exit={exit_code}] {raw_cmd}")
        return (
            f"**CMD:** `{raw_cmd}`\n"
            f"**EXIT CODE:** {exit_code}\n"
            f"**STDOUT:**\n```\n{stdout}\n```\n"
            f"**STDERR:**\n```\n{stderr}\n```"
        )

    def _execute_query(self, raw_query: str) -> str:
        rows, error = self.query_runner.run(raw_query)
        if error:
            print(f"  [ERROR] {raw_query[:60]}: {error[:100]}")
            return (
                f"**QUERY:** `{raw_query}`\n"
                f"**STATUS:** ERROR\n"
                f"**ERROR:** {error}"
            )
        rows_str = json.dumps(rows, indent=2, default=str)[:3000]
        print(f"  [{len(rows)} rows] {raw_query[:60]}")
        return (
            f"**QUERY:** `{raw_query}`\n"
            f"**ROWS:** {len(rows)}\n"
            f"**RESULTS:**\n```json\n{rows_str}\n```"
        )

    def _compose_comment(self, final_text: str) -> str:
        header = (
            "<!-- cortex-review-bot -->\n"
            "## Cortex AI E2E Review\n\n"
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

    # Create playground database
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    playground_db = f"CORTEX_REVIEW_PR{pr_number}_{run_id}"
    print(f"[Step 3] Creating playground database: {playground_db}")
    cursor = cortex.connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {playground_db}")
        cursor.execute(f"USE DATABASE {playground_db}")
        cursor.execute("USE SCHEMA PUBLIC")
    finally:
        cursor.close()

    # Run agent loop
    cmd_runner = CommandRunner()
    query_runner = QueryRunner(cortex.connection)
    agent = AgentLoop(
        cortex=cortex,
        pr=pr,
        cmd_runner=cmd_runner,
        query_runner=query_runner,
        playground_db=playground_db,
    )
    try:
        comment_body = agent.run()
    except Exception as e:
        tb = traceback.format_exc()
        _post_error_comment(
            repo,
            pr_number,
            f"Review pipeline failed:\n\n```\n{e}\n\n{tb}\n```",
        )
        sys.exit(1)
    finally:
        # Always clean up playground database
        print(f"[Cleanup] Dropping playground database: {playground_db}")
        cleanup_cursor = cortex.connection.cursor()
        try:
            cleanup_cursor.execute(f"DROP DATABASE IF EXISTS {playground_db}")
        except Exception as cleanup_err:
            print(f"  Warning: cleanup failed: {cleanup_err}")
        finally:
            cleanup_cursor.close()
        cortex.connection.close()

    # Post the review comment
    print("[Post] Posting review comment to PR...")
    _post_comment(repo, pr_number, comment_body)
    print("Done.")


if __name__ == "__main__":
    main()
