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

"""Tests for the eval runner. Offline — git/pip/cortex/Snowflake are mocked."""

from __future__ import annotations

import contextlib
import json
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

_EVAL_DIR = Path(__file__).parents[2] / ".github" / "scripts" / "eval"
sys.path.insert(0, str(_EVAL_DIR))

import runner  # noqa: E402

# ---------------------------------------------------------------------------
# cache_key
# ---------------------------------------------------------------------------


def test_cache_key_is_stable_and_sensitive():
    base = runner.cache_key("prompt", "model", "case", "sha")
    assert base == runner.cache_key("prompt", "model", "case", "sha")
    # Any input change flips the key.
    assert base != runner.cache_key("PROMPT", "model", "case", "sha")
    assert base != runner.cache_key("prompt", "MODEL", "case", "sha")
    assert base != runner.cache_key("prompt", "model", "CASE", "sha")
    assert base != runner.cache_key("prompt", "model", "case", "SHA")


def test_cache_key_no_boundary_collisions():
    # The \0 separator prevents "a"+"b" colliding with "ab".
    assert runner.cache_key("a", "b", "c", "d") != runner.cache_key("ab", "", "c", "d")


# ---------------------------------------------------------------------------
# build_prompt
# ---------------------------------------------------------------------------


def test_build_prompt_fills_slots():
    prompt = runner.build_prompt(
        "db={playground_db} diff={diff_instructions}", "DIFF", "PLAYGROUND"
    )
    assert prompt == "db=PLAYGROUND diff=DIFF"


# ---------------------------------------------------------------------------
# extract_executed_commands
# ---------------------------------------------------------------------------


def _line(obj) -> str:
    return json.dumps(obj)


def test_extract_commands_from_message_content():
    # Real cortex v1.1.8 shape (smoke-validated): blocks under message.content.
    stdout = "\n".join(
        [
            _line({"type": "system", "subtype": "init"}),
            _line(
                {
                    "type": "assistant",
                    "message": {
                        "content": [
                            {"type": "text", "text": "let me check"},
                            {
                                "type": "tool_use",
                                "name": "bash",
                                "input": {
                                    "command": "snow connection list --format json",
                                    "description": "list connections",
                                },
                            },
                        ]
                    },
                }
            ),
            _line(
                {
                    "type": "assistant",
                    "message": {
                        "content": [
                            {
                                "type": "tool_use",
                                "name": "bash",
                                "input": {"command": "snow sql --help"},
                            }
                        ]
                    },
                }
            ),
            _line(
                {
                    "type": "user",
                    "message": {"content": [{"type": "tool_result", "content": "ok"}]},
                }
            ),
            _line({"type": "result", "result": "<!-- E2E_REPORT -->\n..."}),
        ]
    )
    cmds = runner.extract_executed_commands(stdout)
    assert cmds == ["snow connection list --format json", "snow sql --help"]


def test_extract_commands_flat_content_fallback_and_list_command():
    # Fallback: a flat obj["content"], nested tool_use, and a list-valued command.
    stdout = "\n".join(
        [
            _line(
                {
                    "type": "assistant",
                    "content": [
                        {
                            "type": "tool_use",
                            "tool_use": {
                                "name": "exec",
                                "input": {"cmd": ["snow", "object", "list", "table"]},
                            },
                        }
                    ],
                }
            ),
        ]
    )
    assert runner.extract_executed_commands(stdout) == ["snow object list table"]


def test_extract_commands_ignores_noise_and_bad_lines():
    stdout = "\n".join(
        [
            "not json at all",
            _line({"type": "assistant", "message": {"content": "not a list"}}),
            _line(
                {
                    "type": "assistant",
                    "message": {"content": [{"type": "text", "text": "hi"}]},
                }
            ),
            _line(
                {
                    "type": "assistant",
                    "message": {"content": [{"type": "tool_use", "input": {}}]},
                }
            ),
            "",
        ]
    )
    assert runner.extract_executed_commands(stdout) == []


# ---------------------------------------------------------------------------
# select_cases
# ---------------------------------------------------------------------------


def test_select_cases_none_returns_all():
    cases = runner.load_all_cases()
    assert runner.select_cases(cases, None) == cases
    assert runner.select_cases(cases, []) == cases


def test_select_cases_by_exact_and_glob():
    cases = runner.load_all_cases()
    exact = runner.select_cases(cases, ["sql-rename-query-flag"])
    assert [c.name for c in exact] == ["sql-rename-query-flag"]

    globbed = {c.name for c in runner.select_cases(cases, ["sql-*"])}
    assert globbed == {"sql-rename-query-flag", "sql-add-query-alias"}

    assert runner.select_cases(cases, ["does-not-exist"]) == []


# ---------------------------------------------------------------------------
# _connection_creds + build_reviewer_config (connection cloning + isolation)
# ---------------------------------------------------------------------------


def test_connection_creds_from_user_config(tmp_path, monkeypatch):
    home = tmp_path / "sfhome"
    home.mkdir()
    (home / "connections.toml").write_text(
        "[mysrc]\n"
        'account = "acct"\n'
        'user = "u"\n'
        'private_key_file = "/keys/k.p8"\n'
    )
    monkeypatch.setenv("SNOWFLAKE_HOME", str(home))
    monkeypatch.delenv("SNOWFLAKE_CONNECTIONS_MYSRC_ACCOUNT", raising=False)
    creds = runner._connection_creds("mysrc")  # noqa: SLF001
    assert creds == {"account": "acct", "user": "u", "private_key_file": "/keys/k.p8"}


def test_connection_creds_env_takes_precedence(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CONNECTIONS_E2EREVIEWER_ACCOUNT", "envacct")
    monkeypatch.setenv("SNOWFLAKE_CONNECTIONS_E2EREVIEWER_USER", "envuser")
    creds = runner._connection_creds("e2ereviewer")  # noqa: SLF001
    assert creds["account"] == "envacct"
    assert creds["user"] == "envuser"


def test_build_reviewer_config_overrides_db_and_sets_home(tmp_path):
    creds = {
        "account": "acct",
        "user": "u",
        "private_key_file": "/keys/k.p8",
        "database": "ORIG_DB",
    }
    config_file, env_overrides = runner.build_reviewer_config(
        creds, tmp_path, "PLAYGROUND_DB"
    )
    text = Path(config_file).read_text()
    assert "[e2ereviewer]" in text  # section the reviewer prompt/agent connect with
    assert 'account = "acct"' in text
    assert 'private_key_file = "/keys/k.p8"' in text
    assert 'database = "PLAYGROUND_DB"' in text  # default DB overridden to playground
    assert "ORIG_DB" not in text
    # SNOWFLAKE_HOME points the agent's `snow` at the temp config (verified live to be
    # the mechanism that actually resolves --connection e2ereviewer for the CLI).
    assert env_overrides == {"SNOWFLAKE_HOME": str(tmp_path)}
    # The caller's creds dict is not mutated.
    assert creds["database"] == "ORIG_DB"


# ---------------------------------------------------------------------------
# run_case orchestration (all live ops mocked)
# ---------------------------------------------------------------------------

_TEMPLATE = "playground={playground_db}\n{diff_instructions}"


def _fake_case(mutate) -> runner.Case:
    return runner.Case(
        name="fake-case",
        path=Path("/tmp/fake-case"),
        description="d",
        difficulty="hard",
        tags=["e2e-reviewer"],
        expected_verdict="FAIL",
        expected_breaking_changes=True,
        needs_live_snowflake=False,
        expected_commands=["snow connection list --format json"],
        rubric="r",
        targets=["src/x.py"],
        mutate=mutate,
    )


def _agent_stdout(cmd: str) -> str:
    # Matches the real cortex v1.1.8 shape: tool_use under message.content.
    return json.dumps(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "tool_use", "name": "bash", "input": {"command": cmd}}
                ]
            },
        }
    )


@contextlib.contextmanager
def _mock_live(agent_run):
    """Patch every live op in runner; yield the run_agent mock."""
    run_agent = mock.MagicMock(return_value=agent_run)
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            mock.patch.object(runner, "git_rev_parse", return_value="abc1234")
        )
        stack.enter_context(mock.patch.object(runner, "add_worktree"))
        stack.enter_context(mock.patch.object(runner, "remove_worktree"))
        stack.enter_context(mock.patch.object(runner, "ensure_base_venv"))
        stack.enter_context(mock.patch.object(runner, "relink_cli"))
        stack.enter_context(
            mock.patch.object(
                runner, "_connection_creds", return_value={"account": "a", "user": "u"}
            )
        )
        stack.enter_context(
            mock.patch.object(runner, "build_reviewer_config", return_value=(None, {}))
        )
        prov = stack.enter_context(mock.patch.object(runner, "provision_playground"))
        drop = stack.enter_context(mock.patch.object(runner, "drop_playground"))
        stack.enter_context(mock.patch.object(runner.rev, "run_agent", run_agent))
        yield run_agent, prov, drop


def test_run_case_happy_path(tmp_path):
    mutate = mock.MagicMock()
    case = _fake_case(mutate)
    agent = runner.rev.AgentRun(
        report="<!-- E2E_REPORT -->\n### Verdict\nFAIL — flag removed.",
        exit_code=0,
        duration=1.2,
        stdout=_agent_stdout("snow connection list --format json"),
    )
    results_dir = tmp_path / "results"

    with _mock_live(agent) as (run_agent, prov, drop):
        result = runner.run_case(
            case,
            template=_TEMPLATE,
            diff_instructions="LOCAL-DIFF",
            model="claude-opus-4-6",
            connection="mysource",
            venv_dir=tmp_path / "venv",
            results_dir=results_dir,
        )

    # Mutation applied to the checked-out worktree.
    assert mutate.call_count == 1
    # Result fields.
    assert result.verdict == "FAIL"
    assert result.expected_verdict == "FAIL"
    assert result.exit_code == 0
    assert result.base_sha == "abc1234"
    assert result.executed_commands == ["snow connection list --format json"]
    assert result.from_cache is False
    # Playground provisioned (via the connector, using cloned creds) + dropped.
    assert prov.call_count == 1
    assert prov.call_args.args[0].startswith("CORTEX_EVAL_FAKE_CASE")  # the case's DB
    assert drop.call_count == 1
    # The agent connects via the reviewer section (points at the isolated playground),
    # not the raw source connection; venv is on PATH.
    _, kwargs = run_agent.call_args
    assert kwargs["connection"] == runner.REVIEWER_CONNECTION
    assert "LOCAL-DIFF" in kwargs["prompt"]
    assert kwargs["env"]["PATH"].startswith(str(tmp_path / "venv" / "bin"))
    # Artifacts persisted.
    case_dir = results_dir / "fake-case"
    assert (case_dir / "report.md").read_text().startswith("<!-- E2E_REPORT -->")
    assert "LOCAL-DIFF" in (case_dir / "prompt.txt").read_text()
    assert (case_dir / "stdout.jsonl").exists()
    persisted = json.loads((case_dir / "result.json").read_text())
    assert persisted["verdict"] == "FAIL"
    assert persisted["cache_key"] == result.cache_key


def test_run_case_cache_hit_skips_agent(tmp_path):
    case = _fake_case(mock.MagicMock())
    agent = runner.rev.AgentRun("<!-- E2E_REPORT -->\n### Verdict\nFAIL", 0, 1.0, "{}")
    kw = dict(
        template=_TEMPLATE,
        diff_instructions="LOCAL",
        model="m",
        venv_dir=tmp_path / "venv",
        results_dir=tmp_path / "r",
    )
    with _mock_live(agent) as (run_agent, _prov, _drop):
        first = runner.run_case(case, **kw)
        second = runner.run_case(case, **kw)  # same prompt/model/base → cache hit
    assert run_agent.call_count == 1
    assert first.from_cache is False
    assert second.from_cache is True
    assert second.verdict == first.verdict


def test_run_case_no_cache_reruns(tmp_path):
    case = _fake_case(mock.MagicMock())
    agent = runner.rev.AgentRun("<!-- E2E_REPORT -->\n### Verdict\nFAIL", 0, 1.0, "{}")
    kw = dict(
        template=_TEMPLATE,
        diff_instructions="LOCAL",
        model="m",
        venv_dir=tmp_path / "venv",
        results_dir=tmp_path / "r",
        use_cache=False,
    )
    with _mock_live(agent) as (run_agent, _prov, _drop):
        runner.run_case(case, **kw)
        runner.run_case(case, **kw)
    assert run_agent.call_count == 2


# ---------------------------------------------------------------------------
# ensure_base_venv — .ready sentinel
# ---------------------------------------------------------------------------


def test_ensure_base_venv_uses_ready_sentinel(tmp_path):
    venv_dir = tmp_path / "venv"
    calls = []

    def fake_run(cmd, **_kwargs):
        calls.append(cmd)

    with mock.patch.object(runner, "_run", side_effect=fake_run):
        runner.ensure_base_venv(venv_dir)

    # venv creation + pip install both ran; sentinel written.
    assert any("venv" in " ".join(c) for c in calls)
    assert any("pip" in " ".join(c) for c in calls)
    assert (venv_dir / ".ready").exists()

    # Second call: sentinel present → nothing runs.
    calls.clear()
    with mock.patch.object(runner, "_run", side_effect=fake_run):
        runner.ensure_base_venv(venv_dir)
    assert calls == []


def test_ensure_base_venv_no_sentinel_on_failed_install(tmp_path):
    venv_dir = tmp_path / "venv"
    call_count = [0]

    def fake_run(cmd, **_kwargs):
        call_count[0] += 1
        if "pip" in " ".join(cmd):
            raise subprocess.CalledProcessError(1, cmd)

    with mock.patch.object(runner, "_run", side_effect=fake_run):
        try:
            runner.ensure_base_venv(venv_dir)
        except subprocess.CalledProcessError:
            pass

    # Sentinel must NOT exist after a failed install.
    assert not (venv_dir / ".ready").exists()
    # A retry attempt must re-run the install (not skip due to stale bin/python).
    first_calls = call_count[0]
    call_count[0] = 0
    with mock.patch.object(runner, "_run", side_effect=fake_run):
        try:
            runner.ensure_base_venv(venv_dir)
        except subprocess.CalledProcessError:
            pass
    assert call_count[0] == first_calls  # same number of calls → retried


# ---------------------------------------------------------------------------
# build_reviewer_config — raw private key spill
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="os.chmod has no effect on Windows")
def test_build_reviewer_config_raw_key_spill(tmp_path):
    raw_pem = "-----BEGIN RSA PRIVATE KEY-----\nFAKEKEY\n-----END RSA PRIVATE KEY-----"
    creds = {"account": "a", "user": "u", "private_key_raw": raw_pem}
    config_file, _ = runner.build_reviewer_config(creds, tmp_path, "DB")

    key_file = tmp_path / "rsa_key.p8"
    # Key was spilled to a file with restrictive permissions.
    assert key_file.exists()
    assert oct(key_file.stat().st_mode)[-3:] == "600"
    # Raw PEM must never appear inline in the TOML.
    toml_text = Path(config_file).read_text()
    assert "private_key_raw" not in toml_text
    assert "FAKEKEY" not in toml_text
    # private_key_file points at the spilled file.
    assert str(key_file) in toml_text
    # Caller's dict is not mutated.
    assert "private_key_raw" in creds
