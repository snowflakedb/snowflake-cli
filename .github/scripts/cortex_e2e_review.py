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

Uses Cortex Code CLI as an autonomous agent to verify PR changes.
Cortex Code handles all tool execution (file reading, command running,
SQL queries) natively - this script just orchestrates the setup,
passes the prompt, and posts the result as a PR comment.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import traceback

import snowflake.connector
from snowflake.cli._app.snow_connector import update_connection_details_with_private_key

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ENV_PREFIX = "SNOWFLAKE_CONNECTIONS_INTEGRATION"

AGENT_PROMPT_TEMPLATE = """\
You are an autonomous end-to-end verification agent for the snowflake-cli project.

You do NOT perform static code review. No commenting on code style, security,
test coverage, or architecture. Your only job is dynamic E2E verification of
CLI behavioral changes.

## Project structure

src/snowflake/cli/_app/       - core app framework, connectors, config
src/snowflake/cli/_plugins/   - CLI command plugins (cortex, sql, stage, etc.)
src/snowflake/cli/api/        - public API surface, shared utilities
tests/                        - unit tests (mirrors src/ structure)
tests_integration/            - integration tests (require Snowflake)
tests_e2e/                    - end-to-end tests

## Your environment

- The `snow` CLI is installed from the PR branch.
- You have a dedicated Snowflake playground database: {playground_db}
  You can CREATE, DROP, INSERT, ALTER anything in it. It will be destroyed
  after your run completes.
- Connection: --connection integration (points to the playground database)
- You have `gh` CLI available to fetch PR details.

## How to investigate changes

1. **Understand scope** - Fetch the PR diff and changed files using `gh`.
   Read changed files fully. Search (grep) for callers and imports of
   changed functions across the codebase. Check existing tests.

2. **Test happy path** - Run the changed CLI commands with typical inputs.

3. **Test edge cases** - Empty inputs, special characters, missing objects,
   invalid values, very long strings, null/None.

4. **Identify breaking changes** - Check if the PR changes any of these:
   - Command names, subcommand names, or aliases (renamed/removed)
   - CLI option names, short flags, or their defaults
   - Output format (JSON structure, table columns, message text that
     scripts may parse)
   - Exit codes or error messages
   - Required arguments or their order
   - Behavior of existing flags (e.g. a flag that was no-op now does
     something, or vice versa)
   - Environment variable names or config file keys
   For each breaking change found, verify by running the old usage
   pattern and confirming it fails or behaves differently.

5. **Check interactions** - Does the change affect shared utilities used by
   other plugins? Could it break a seemingly unrelated command? Search
   for shared functions, base classes, or decorators that were modified.

6. **Verify side effects** - After running commands, query Snowflake to
   confirm objects were created/modified as expected.

## Your task

Review PR #{pr_number} in the {pr_repo} repository.

Start by fetching the PR details, diff, and changed files using `gh`.
Then determine if CLI behavior changed. If not, report SKIP.
If it did, investigate thoroughly and report your findings.

IMPORTANT: Your final report MUST begin with the exact marker line
`<!-- E2E_REPORT -->` on its own line, immediately followed by the report.
Do NOT include any text, reasoning, or preamble before this marker.

Output your final report as GitHub Markdown with these sections:

### Summary
One paragraph on what the PR does and whether E2E verification passed.

### E2E Test Results
Commands run, exit codes, whether output matched expectations.

### Breaking Changes
Any changes that break backward compatibility: renamed/removed commands
or options, changed output formats, different exit codes, altered defaults.
If none, say "No breaking changes detected."

### Side-Effect Verification
SQL queries run and their results. If none needed, say so.

### Potential Risks
Things you noticed but couldn't fully verify. Interaction risks, edge cases
not testable in the playground. If none, say so.

### Verdict
One of: PASS / FAIL / SKIP with one sentence justification.
"""

# ---------------------------------------------------------------------------
# Snowflake connection (for playground setup/teardown only)
# ---------------------------------------------------------------------------


def connect_snowflake() -> snowflake.connector.SnowflakeConnection:
    """Connect using SNOWFLAKE_CONNECTIONS_INTEGRATION_* env vars."""
    config: dict = {
        "application": "CORTEX_PR_REVIEW",
        "authenticator": os.environ.get(
            f"{_ENV_PREFIX}_AUTHENTICATOR", "SNOWFLAKE_JWT"
        ),
        "account": os.environ[f"{_ENV_PREFIX}_ACCOUNT"],
        "user": os.environ[f"{_ENV_PREFIX}_USER"],
    }
    for param, env_key in {
        "host": f"{_ENV_PREFIX}_HOST",
        "database": f"{_ENV_PREFIX}_DATABASE",
        "warehouse": f"{_ENV_PREFIX}_WAREHOUSE",
        "role": f"{_ENV_PREFIX}_ROLE",
        "private_key_raw": f"{_ENV_PREFIX}_PRIVATE_KEY_RAW",
        "private_key_file": f"{_ENV_PREFIX}_PRIVATE_KEY_FILE",
    }.items():
        val = os.environ.get(env_key)
        if val:
            config[param] = val
    if "private_key_file" not in config:
        pk_path = os.environ.get(f"{_ENV_PREFIX}_PRIVATE_KEY_PATH")
        if pk_path:
            config["private_key_file"] = pk_path
    config = {k: v for k, v in config.items() if v is not None}
    update_connection_details_with_private_key(config)
    return snowflake.connector.connect(**config)


# ---------------------------------------------------------------------------
# Comment helpers
# ---------------------------------------------------------------------------


def post_comment(repo: str, pr_number: int, body: str) -> None:
    try:
        subprocess.run(
            ["gh", "pr", "comment", str(pr_number), "--repo", repo, "--body", body],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to post comment: {e}", file=sys.stderr)


def delete_previous_comment(repo: str, pr_number: int) -> None:
    """Delete the previous cortex-review-bot comment if it exists."""
    try:
        comments_raw = subprocess.run(
            [
                "gh",
                "api",
                f"repos/{repo}/issues/{pr_number}/comments",
                "--paginate",
                "--jq",
                '.[] | select(.body | contains("<!-- cortex-review-bot -->")) | .id',
            ],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        for comment_id in comments_raw.strip().splitlines():
            if comment_id.strip():
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
                print(f"  Deleted previous bot comment {comment_id.strip()}")
    except Exception as e:
        print(f"  Warning: could not delete previous comment: {e}")


def post_error_comment(repo: str, pr_number: int, message: str) -> None:
    body = (
        "<!-- cortex-review-bot -->\n"
        "## Cortex AI E2E Review - Error\n\n"
        "The automated review encountered an error and could not complete:\n\n"
        f"{message}\n\n"
        "_This is an automated message. Check workflow logs for details._"
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
    model = os.environ.get("CORTEX_MODEL", "claude-opus-4-6")

    # Step 1: Verify CLI build
    print("[Step 1] Verifying CLI build...")
    result = subprocess.run(["snow", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR: snow CLI not found", file=sys.stderr)
        sys.exit(1)
    print(f"  CLI version: {result.stdout.strip()}")

    # Step 2: Verify cortex CLI is installed
    print("[Step 2] Verifying Cortex Code CLI...")
    # Ensure ~/.local/bin is in PATH (cortex install location)
    local_bin = os.path.expanduser("~/.local/bin")
    if local_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{local_bin}:{os.environ.get('PATH', '')}"
    result = subprocess.run(["cortex", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR: cortex CLI not found", file=sys.stderr)
        sys.exit(1)
    print(f"  Cortex version: {result.stdout.strip()}")

    # Step 3: Create playground database
    print("[Step 3] Creating playground database...")
    conn = connect_snowflake()
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    playground_db = f"CORTEX_REVIEW_PR{pr_number}_{run_id}"
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {playground_db}")
        cursor.execute(f"USE DATABASE {playground_db}")
        cursor.execute("USE SCHEMA PUBLIC")
    finally:
        cursor.close()
    print(f"  Created: {playground_db}")

    try:
        # Point snow CLI at the playground
        os.environ[f"{_ENV_PREFIX}_DATABASE"] = playground_db

        # Step 4: Configure cortex connection
        print("[Step 4] Configuring cortex connection...")
        snowflake_home = os.path.expanduser("~/.snowflake")
        os.makedirs(snowflake_home, exist_ok=True)
        connections_toml = os.path.join(snowflake_home, "connections.toml")

        # Write private key to a file if provided as raw
        private_key_raw = os.environ.get(f"{_ENV_PREFIX}_PRIVATE_KEY_RAW", "")
        private_key_file = os.environ.get(
            f"{_ENV_PREFIX}_PRIVATE_KEY_FILE",
            os.environ.get(f"{_ENV_PREFIX}_PRIVATE_KEY_PATH", ""),
        )
        if private_key_raw and not private_key_file:
            private_key_file = os.path.join(snowflake_home, "rsa_key.p8")
            with open(private_key_file, "w") as f:
                f.write(private_key_raw)
            os.chmod(private_key_file, 0o600)
            print(f"  Wrote private key to {private_key_file}")

        # Build connection config
        conn_config = {
            "account": os.environ.get(f"{_ENV_PREFIX}_ACCOUNT", ""),
            "user": os.environ.get(f"{_ENV_PREFIX}_USER", ""),
            "authenticator": os.environ.get(f"{_ENV_PREFIX}_AUTHENTICATOR", ""),
            "host": os.environ.get(f"{_ENV_PREFIX}_HOST", ""),
            "database": playground_db,
            "warehouse": os.environ.get(f"{_ENV_PREFIX}_WAREHOUSE", ""),
            "role": os.environ.get(f"{_ENV_PREFIX}_ROLE", ""),
        }
        if private_key_file:
            conn_config["private_key_file"] = private_key_file

        # Write TOML — use multi-line string for private key path
        toml_lines = ["[integration]"]
        for key, val in conn_config.items():
            if val:
                escaped = val.replace("\\", "\\\\").replace('"', '\\"')
                toml_lines.append(f'{key} = "{escaped}"')
        with open(connections_toml, "w") as f:
            f.write("\n".join(toml_lines) + "\n")
        os.chmod(connections_toml, 0o600)
        print(f"  Wrote {connections_toml}")
        # Debug: show the config (redact sensitive fields)
        for line in toml_lines:
            if "private_key" not in line.lower():
                print(f"    {line}")

        # Step 5: Build the prompt
        prompt = AGENT_PROMPT_TEMPLATE.format(
            playground_db=playground_db,
            pr_number=pr_number,
            pr_repo=repo,
        )

        # Step 6: Run Cortex Code CLI agent
        print("[Step 6] Running Cortex Code CLI agent...")
        agent_start = time.monotonic()
        try:
            agent_result = subprocess.run(
                [
                    "cortex",
                    "-p",
                    prompt,
                    "--model",
                    model,
                    "--connection",
                    "integration",
                    "--workdir",
                    os.getcwd(),
                    "--plan",
                    "--auto-accept-plans",
                    "--bypass",
                    "--output-format",
                    "stream-json",
                    "--no-auto-update",
                    "--config-file",
                    connections_toml,
                ],
                capture_output=True,
                text=True,
                timeout=3000,  # 50 minutes
            )
            # Parse stream-json output — take only the last message (the report)
            agent_output = _parse_stream_json(agent_result.stdout)
            if not agent_output:
                # Fallback: post raw output so the user sees something
                agent_output = (
                    "_Could not parse structured output from Cortex agent. "
                    "Raw output below:_\n\n```\n" + agent_result.stdout[:8000] + "\n```"
                )
            agent_duration = time.monotonic() - agent_start
            print(
                f"  Agent finished (exit={agent_result.returncode},"
                f" {len(agent_output)} chars, {agent_duration:.0f}s)"
            )
            if agent_result.returncode != 0:
                print(f"  Stderr: {agent_result.stderr[:1000]}")
        except subprocess.TimeoutExpired:
            print("  Agent timed out")
            post_error_comment(
                repo, pr_number, "Cortex agent timed out after 50 minutes."
            )
            sys.exit(1)
        except Exception as e:
            tb = traceback.format_exc()
            post_error_comment(
                repo, pr_number, f"Cortex agent failed:\n\n```\n{e}\n\n{tb}\n```"
            )
            sys.exit(1)

        # Step 7: Post the review comment
        print("[Step 7] Posting review comment...")
        delete_previous_comment(repo, pr_number)
        head_sha_result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
        )
        head_sha = head_sha_result.stdout.strip() or "unknown"
        duration_min = int(agent_duration // 60)
        duration_sec = int(agent_duration % 60)
        header = (
            "<!-- cortex-review-bot -->\n"
            "## Cortex AI E2E Review\n\n"
            f"> Model: `{model}` | "
            f"Commit: `{head_sha}` | "
            f"Duration: {duration_min}m {duration_sec}s | "
            f"Reviewed at: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n\n"
        )
        comment_body = header + agent_output
        if len(comment_body) > 65000:
            comment_body = comment_body[:65000] + "\n\n... [comment truncated]"
        post_comment(repo, pr_number, comment_body)
    finally:
        _cleanup(conn, playground_db)
    print("Done.")


def _parse_stream_json(raw: str) -> str:
    """Extract the final report from Cortex Code CLI stream-json output.

    Strategy (in priority order):
    1. If a ``result`` message exists, use it (final agent output).
    2. Look for the ``<!-- E2E_REPORT -->`` delimiter the prompt asks for.
    3. Fallback: find the LAST ``### Summary`` heading across all messages.
    """
    import re

    report_marker = "<!-- E2E_REPORT -->"

    result_text = ""
    all_text: list[str] = []

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            content = ""
            if obj.get("type") == "result" and obj.get("result"):
                result_text = obj["result"]
            elif obj.get("type") == "text" and obj.get("content"):
                content = obj["content"]
            elif obj.get("type") == "assistant" and isinstance(obj.get("content"), str):
                content = obj["content"]
            if content:
                all_text.append(content)
        except json.JSONDecodeError:
            continue

    # 1. Prefer ``result`` message — typically contains only the final output
    if result_text:
        if report_marker in result_text:
            return result_text.split(report_marker, 1)[1].strip()
        m = list(re.finditer(r"^#{1,4}\s+Summary", result_text, re.MULTILINE))
        if m:
            return result_text[m[-1].start() :]
        return result_text

    full_output = "\n".join(all_text)

    # 2. Look for the explicit report delimiter
    if report_marker in full_output:
        return full_output.split(report_marker, 1)[1].strip()

    # 3. Fallback: last Summary heading
    matches = list(re.finditer(r"^#{1,4}\s+Summary", full_output, re.MULTILINE))
    if matches:
        return full_output[matches[-1].start() :]

    return full_output


def _cleanup(conn: snowflake.connector.SnowflakeConnection, playground_db: str):
    """Drop the playground database."""
    print(f"[Cleanup] Dropping {playground_db}...")
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {playground_db}")
    except Exception as e:
        print(f"  Warning: cleanup failed: {e}")
    finally:
        cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
