"""Tests for the eval-facing API of cortex_e2e_review.py.

All tests are offline — no Snowflake connection, no cortex CLI.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# The script lives under .github/scripts which is not a package, so add it to sys.path.
sys.path.insert(0, str(Path(__file__).parents[2] / ".github" / "scripts"))

import cortex_e2e_review as rev  # noqa: E402

# ---------------------------------------------------------------------------
# Golden test — production prompt must not change silently
# ---------------------------------------------------------------------------

PRODUCTION_PROMPT_GOLDEN = """\
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
- You have a dedicated Snowflake playground database: MY_DB
  You can CREATE, DROP, INSERT, ALTER anything in it. It will be destroyed
  after your run completes.
- Connection: --connection e2ereviewer (points to the playground database)
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

Review PR #42 in the myorg/myrepo repository.

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


def test_production_prompt_golden():
    """Catch any accidental change to the production prompt."""
    rendered = rev.AGENT_PROMPT_TEMPLATE.format(
        playground_db="MY_DB",
        diff_instructions=rev.PR_DIFF_INSTRUCTIONS.format(
            pr_number=42, pr_repo="myorg/myrepo"
        ),
    )
    assert rendered == PRODUCTION_PROMPT_GOLDEN, (
        "Production prompt changed unexpectedly. "
        "If intentional, update PRODUCTION_PROMPT_GOLDEN in this test."
    )


def test_local_diff_prompt_renders():
    """Lock the eval's local-diff path (the reason for the refactor).

    The template must render cleanly with LOCAL_DIFF_INSTRUCTIONS: it points the
    agent at `git diff origin/main`, frames it as a local working copy, and leaves
    no unfilled `{...}` placeholders.
    """
    rendered = rev.AGENT_PROMPT_TEMPLATE.format(
        playground_db="MY_DB",
        diff_instructions=rev.LOCAL_DIFF_INSTRUCTIONS,
    )
    assert "git diff origin/main" in rendered
    assert "local changes in your working directory" in rendered
    assert "{" not in rendered and "}" not in rendered


# ---------------------------------------------------------------------------
# parse_verdict
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "report, expected",
    [
        ("### Verdict\nPASS — no breaking changes.", "PASS"),
        ("### Verdict\nFAIL — option --query was removed.", "FAIL"),
        ("### Verdict\nSKIP — no CLI behaviour changed.", "SKIP"),
        ("### Verdict\npass with lowercase", "PASS"),
        ("### Verdict\nThe result is: FAIL.", "FAIL"),
        # Verdict buried after other sections
        (
            "### Summary\nAll good.\n### Verdict\nSKIP — docs only.",
            "SKIP",
        ),
        # No verdict section at all
        ("### Summary\nLooks fine.", None),
        # Empty report
        ("", None),
    ],
)
def test_parse_verdict(report, expected):
    assert rev.parse_verdict(report) == expected


# ---------------------------------------------------------------------------
# write_connections_toml
# ---------------------------------------------------------------------------


def test_write_connections_toml_content_and_permissions(tmp_path):
    config = {
        "account": "myaccount",
        "user": "myuser",
        "database": "MYDB",
        "private_key_file": "/path/to/key.p8",
    }
    path = str(tmp_path / "connections.toml")
    with patch.object(rev.os, "chmod") as mock_chmod:
        rev.write_connections_toml(config, path)
    text = Path(path).read_text()
    assert "[e2ereviewer]" in text
    assert 'account = "myaccount"' in text
    assert 'user = "myuser"' in text
    assert 'database = "MYDB"' in text
    assert 'private_key_file = "/path/to/key.p8"' in text
    mock_chmod.assert_called_once_with(path, 0o600)


def test_write_connections_toml_skips_empty_values():
    config = {"account": "acct", "user": "", "database": "DB"}
    with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as f:
        path = f.name
    rev.write_connections_toml(config, path)
    text = Path(path).read_text()
    assert "user" not in text
    assert 'account = "acct"' in text


def test_write_connections_toml_escapes_special_chars():
    config = {"account": 'ac"ct', "user": "u\\ser"}
    with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as f:
        path = f.name
    rev.write_connections_toml(config, path)
    text = Path(path).read_text()
    assert r"ac\"ct" in text
    assert r"u\\ser" in text


def test_write_connections_toml_redacts_secrets(capsys):
    config = {
        "account": "myacct",
        "password": "PW_DO_NOT_LOG",
        "token": "TOK_DO_NOT_LOG",
        "private_key_raw": "PK_DO_NOT_LOG",
    }
    with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as f:
        path = f.name
    with patch.object(rev.os, "chmod"):
        rev.write_connections_toml(config, path)
    out = capsys.readouterr().out
    assert "myacct" in out  # non-secret is echoed for debugging
    for secret in ("PW_DO_NOT_LOG", "TOK_DO_NOT_LOG", "PK_DO_NOT_LOG"):
        assert secret not in out  # secrets are redacted from stdout
    # ...but all values are still written to the (0600) connection file.
    assert "PW_DO_NOT_LOG" in Path(path).read_text()


# ---------------------------------------------------------------------------
# run_agent
# ---------------------------------------------------------------------------

_SAMPLE_STREAM_JSON = '{"type":"result","result":"<!-- E2E_REPORT -->\\n### Summary\\nOK.\\n### Verdict\\nPASS"}\n'


def test_run_agent_returns_parsed_report(tmp_path):
    config_file = tmp_path / "connections.toml"
    config_file.write_text("")
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = _SAMPLE_STREAM_JSON
    mock_result.stderr = ""
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        result = rev.run_agent(
            prompt="test prompt",
            model="claude-opus-4-6",
            workdir="/repo",
            connection="e2ereviewer",
            config_file=str(config_file),
        )
    assert result.exit_code == 0
    assert result.duration >= 0
    assert "### Summary" in result.report
    assert "PASS" in result.report
    assert result.stdout == _SAMPLE_STREAM_JSON  # raw stdout captured for trajectory
    # Confirm exact production argv is used
    argv = mock_run.call_args[0][0]
    assert argv[0] == "cortex"
    assert "-p" in argv
    assert "--plan" in argv
    assert "--auto-accept-plans" in argv
    assert "--bypass" in argv
    assert "--output-format" in argv
    assert "stream-json" in argv
    assert "--config-file" in argv


def test_run_agent_fallback_on_empty_output(tmp_path):
    config_file = tmp_path / "connections.toml"
    config_file.write_text("")
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ""
    mock_result.stderr = ""
    with patch("subprocess.run", return_value=mock_result):
        result = rev.run_agent(
            prompt="p",
            model="m",
            workdir="/w",
            connection="c",
            config_file=str(config_file),
        )
    assert "Raw output" in result.report


def test_run_agent_propagates_timeout(tmp_path):
    config_file = tmp_path / "connections.toml"
    config_file.write_text("")
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("cortex", 10)):
        with pytest.raises(subprocess.TimeoutExpired):
            rev.run_agent(
                prompt="p",
                model="m",
                workdir="/w",
                connection="c",
                config_file=str(config_file),
            )
