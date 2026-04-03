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

MAX_DIFF_CHARS = 40_000
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

## How to investigate changes

1. **Understand scope** - Read changed files fully. Search (grep) for callers
   and imports of changed functions across the codebase. Check existing tests.

2. **Test happy path** - Run the changed CLI commands with typical inputs.

3. **Test edge cases** - Empty inputs, special characters, missing objects,
   invalid values, very long strings, null/None.

4. **Test backward compatibility** - Does old usage still work?

5. **Check interactions** - Does the change affect shared utilities used by
   other plugins? Could it break a seemingly unrelated command?

6. **Verify side effects** - After running commands, query Snowflake to
   confirm objects were created/modified as expected.

## Pull Request to verify

**Title:** {pr_title}

**Description:**
{pr_body}

**Changed files:**
{changed_files}

**Diff:**
```diff
{diff}
```

## Your task

If this PR does NOT change CLI behavior (e.g. CI-only, docs-only, test-only),
output a short summary explaining why and verdict SKIP.

If it DOES change CLI behavior, investigate thoroughly using the process above.
You have full access to run snow commands, execute SQL, read files, and search
the codebase. Use as many steps as you need.

Output your final report as GitHub Markdown with these sections:

### Summary
One paragraph on what the PR does and whether E2E verification passed.

### E2E Test Results
Commands run, exit codes, whether output matched expectations.

### Side-Effect Verification
SQL queries run and their results. If none needed, say so.

### Potential Risks
Things you noticed but couldn't fully verify. Interaction risks, edge cases
not testable in the playground. If none, say so.

### Verdict
One of: PASS / FAIL / SKIP with one sentence justification.
"""

# ---------------------------------------------------------------------------
# PR Fetcher
# ---------------------------------------------------------------------------


def fetch_pr(repo: str, pr_number: int) -> dict:
    """Fetch PR metadata via gh CLI."""
    meta_raw = subprocess.run(
        [
            "gh",
            "--repo",
            repo,
            "pr",
            "view",
            str(pr_number),
            "--json",
            "title,body,headRefOid,baseRefOid,files",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout
    meta = json.loads(meta_raw)

    diff_raw = subprocess.run(
        ["gh", "--repo", repo, "pr", "diff", str(pr_number)],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout
    diff = diff_raw[:MAX_DIFF_CHARS]
    if len(diff_raw) > MAX_DIFF_CHARS:
        diff += f"\n... [diff truncated at {MAX_DIFF_CHARS} chars]"

    return {
        "title": meta.get("title", ""),
        "body": meta.get("body", "") or "",
        "diff": diff,
        "changed_files": [f["path"] for f in meta.get("files", [])],
        "head_sha": meta.get("headRefOid", ""),
    }


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
    subprocess.run(
        ["gh", "pr", "comment", str(pr_number), "--repo", repo, "--body", body],
        check=True,
    )


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

    # Step 1: Fetch PR metadata
    print(f"[Step 1] Fetching PR #{pr_number} metadata...")
    try:
        pr = fetch_pr(repo, pr_number)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to fetch PR: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"  Title: {pr['title']}")
    print(f"  Changed files: {len(pr['changed_files'])}")

    # Step 2: Verify CLI build
    print("[Step 2] Verifying CLI build...")
    result = subprocess.run(["snow", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR: snow CLI not found", file=sys.stderr)
        sys.exit(1)
    print(f"  CLI version: {result.stdout.strip()}")

    # Step 3: Verify cortex CLI is installed
    print("[Step 3] Verifying Cortex Code CLI...")
    # Ensure ~/.local/bin is in PATH (cortex install location)
    local_bin = os.path.expanduser("~/.local/bin")
    if local_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{local_bin}:{os.environ.get('PATH', '')}"
    result = subprocess.run(["cortex", "--version"], capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR: cortex CLI not found", file=sys.stderr)
        sys.exit(1)
    print(f"  Cortex version: {result.stdout.strip()}")

    # Dump full cortex --help for debugging
    help_result = subprocess.run(["cortex", "--help"], capture_output=True, text=True)
    full_help = help_result.stdout + help_result.stderr
    print(f"  cortex --help (full):\n{full_help}")

    # Step 4: Create playground database
    print("[Step 4] Creating playground database...")
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

    # Point snow CLI at the playground
    os.environ[f"{_ENV_PREFIX}_DATABASE"] = playground_db

    # Step 5: Configure cortex connection
    print("[Step 5] Configuring cortex connection...")
    snowflake_home = os.path.expanduser("~/.snowflake")
    os.makedirs(snowflake_home, exist_ok=True)
    connections_toml = os.path.join(snowflake_home, "connections.toml")
    # Build connection config from env vars
    conn_config = {
        "account": os.environ.get(f"{_ENV_PREFIX}_ACCOUNT", ""),
        "user": os.environ.get(f"{_ENV_PREFIX}_USER", ""),
        "authenticator": os.environ.get(f"{_ENV_PREFIX}_AUTHENTICATOR", ""),
        "host": os.environ.get(f"{_ENV_PREFIX}_HOST", ""),
        "database": playground_db,
        "warehouse": os.environ.get(f"{_ENV_PREFIX}_WAREHOUSE", ""),
        "role": os.environ.get(f"{_ENV_PREFIX}_ROLE", ""),
    }
    private_key_raw = os.environ.get(f"{_ENV_PREFIX}_PRIVATE_KEY_RAW", "")
    if private_key_raw:
        conn_config["private_key_raw"] = private_key_raw
    # Write TOML
    toml_lines = ["[integration]"]
    for key, val in conn_config.items():
        if val:
            # Escape for TOML string
            escaped = val.replace("\\", "\\\\").replace('"', '\\"')
            toml_lines.append(f'{key} = "{escaped}"')
    with open(connections_toml, "w") as f:
        f.write("\n".join(toml_lines) + "\n")
    os.chmod(connections_toml, 0o600)
    print(f"  Wrote {connections_toml}")

    # Step 6: Build the prompt
    prompt = AGENT_PROMPT_TEMPLATE.format(
        playground_db=playground_db,
        pr_title=pr["title"],
        pr_body=pr["body"][:3000] or "(no description)",
        changed_files="\n".join(f"- {f}" for f in pr["changed_files"]),
        diff=pr["diff"],
    )

    # Step 7: Run Cortex Code CLI agent
    print("[Step 7] Running Cortex Code CLI agent...")
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
        # Parse stream-json output
        agent_output = _parse_stream_json(agent_result.stdout)
        if not agent_output:
            agent_output = agent_result.stdout[:10000]
        print(
            f"  Agent finished (exit={agent_result.returncode},"
            f" {len(agent_output)} chars)"
        )
        if agent_result.returncode != 0:
            print(f"  Stderr (full):\n{agent_result.stderr}")
            print(f"  Stdout (full):\n{agent_result.stdout[:3000]}")
    except subprocess.TimeoutExpired:
        agent_output = "Agent timed out after 50 minutes."
        print("  Agent timed out")
    except Exception as e:
        tb = traceback.format_exc()
        post_error_comment(
            repo, pr_number, f"Cortex agent failed:\n\n```\n{e}\n\n{tb}\n```"
        )
        _cleanup(conn, playground_db)
        sys.exit(1)

    # Step 8: Post the review comment
    print("[Step 8] Posting review comment...")
    head_sha = pr["head_sha"][:8]
    header = (
        "<!-- cortex-review-bot -->\n"
        "## Cortex AI E2E Review\n\n"
        f"> Model: `{model}` | "
        f"Commit: `{head_sha}` | "
        f"Reviewed at: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n\n"
    )
    comment_body = header + agent_output
    # GitHub comment limit is 65536 chars
    if len(comment_body) > 65000:
        comment_body = comment_body[:65000] + "\n\n... [comment truncated]"
    post_comment(repo, pr_number, comment_body)

    # Step 9: Cleanup
    _cleanup(conn, playground_db)
    print("Done.")


def _parse_stream_json(raw: str) -> str:
    """Extract text content from Cortex Code CLI stream-json output."""
    text_parts = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if obj.get("type") == "text":
                text_parts.append(obj.get("content", ""))
            elif obj.get("type") == "result":
                text_parts.append(obj.get("result", ""))
            elif isinstance(obj.get("content"), str):
                text_parts.append(obj["content"])
        except json.JSONDecodeError:
            continue
    return "\n".join(text_parts)


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
