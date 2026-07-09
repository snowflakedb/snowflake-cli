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

"""Local runner for the E2E PR-review eval.

Runs the real reviewer (the Cortex Code agent) over the mutation-based PR cases with a
chosen (prompt, model) in LOCAL-DIFF mode: each case is a fresh git worktree of the base
ref (origin/main) with the case's mutation applied, the mutated CLI installed into a
shared venv, and the agent pointed at it. The report and raw stream-json are persisted
per case for the scorer.

File safety: never writes to ``~/.snowflake``. Any generated connection config goes to a
per-run temp dir. Ephemeral playground databases are created and dropped via the chosen
connection (a Snowflake connection is required regardless, since Cortex serves the model).
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

import snowflake.connector

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

_EVAL_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _EVAL_DIR.parent
REPO_ROOT = Path(__file__).resolve().parents[3]

for _p in (str(_EVAL_DIR), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import cortex_e2e_review as rev  # noqa: E402
from cases import Case, load_all_cases  # noqa: E402
from snowflake.cli._app.snow_connector import (  # noqa: E402
    update_connection_details_with_private_key,
)

DEFAULT_MODEL = "claude-opus-4-6"
DEFAULT_BASE_REF = "origin/main"
# --connection selects the SOURCE connection whose creds are cloned (env creds under
# SNOWFLAKE_CONNECTIONS_E2EREVIEWER_* in CI, or a section in the user's config locally).
DEFAULT_CONNECTION = "e2ereviewer"
# The section name the reviewer prompt hardcodes and the agent connects with. The runner
# writes a temp config with this section pointing at the ephemeral playground.
REVIEWER_CONNECTION = "e2ereviewer"


@dataclass
class CaseResult:
    """Per-case artifact the scorer consumes (persisted as result.json)."""

    case: str
    model: str
    base_sha: str
    cache_key: str
    verdict: str | None
    expected_verdict: str
    expected_breaking_changes: bool
    needs_live_snowflake: bool
    exit_code: int
    duration_sec: float
    executed_commands: list[str] = field(default_factory=list)
    from_cache: bool = False


# ---------------------------------------------------------------------------
# Pure helpers (no I/O, no subprocess) — unit-tested
# ---------------------------------------------------------------------------


def cache_key(prompt: str, model: str, case_name: str, base_sha: str) -> str:
    """Stable key identifying a run of one case under a given prompt/model/base."""
    h = hashlib.sha256()
    for part in (prompt, model, case_name, base_sha):
        h.update(part.encode("utf-8"))
        h.update(b"\0")
    return h.hexdigest()[:16]


def build_prompt(template: str, diff_instructions: str, playground_db: str) -> str:
    """Fill the reviewer prompt template for a local-diff eval run."""
    return template.format(
        playground_db=playground_db, diff_instructions=diff_instructions
    )


def extract_executed_commands(stdout: str) -> list[str]:
    """Extract shell commands the agent ran from cortex ``stream-json`` stdout.

    Cortex emits Claude-Code / Anthropic-Messages style output: one JSON object per
    line, and ``type="assistant"`` records carry the content blocks under
    ``obj["message"]["content"]`` (a flat ``obj["content"]`` is accepted as a
    fallback). Any ``tool_use`` block whose input has a ``command``, ``cmd``, or
    ``script`` key is captured — the tool name is not filtered, so ``bash``, ``exec``,
    and other shell-like tools are all included. This is the near-deterministic "did it
    actually investigate" signal the scorer checks against ``expected_commands`` (only
    executed commands count — a command merely mentioned in the report text does not).

    Best-effort: unrecognized shapes are skipped and yield ``[]``.
    """
    commands: list[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("type") != "assistant":
            continue
        message = obj.get("message")
        content = (
            message.get("content") if isinstance(message, dict) else obj.get("content")
        )
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_use":
                continue
            tool_use = block.get("tool_use", block)
            inp = tool_use.get("input")
            if not isinstance(inp, dict):
                continue
            cmd = inp.get("command") or inp.get("cmd") or inp.get("script")
            if isinstance(cmd, list):
                cmd = " ".join(str(x) for x in cmd)
            if isinstance(cmd, str) and cmd.strip():
                commands.append(cmd.strip())
    return commands


def select_cases(cases: list[Case], patterns: list[str] | None) -> list[Case]:
    """Filter cases by fnmatch patterns against the case name (None → all)."""
    if not patterns:
        return cases
    return [c for c in cases if any(fnmatch.fnmatch(c.name, pat) for pat in patterns)]


# ---------------------------------------------------------------------------
# Live infrastructure (git worktree, venv, connection, playground) — mocked in tests
# ---------------------------------------------------------------------------


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a subprocess, raising CalledProcessError on failure with captured output."""
    return subprocess.run(cmd, check=True, capture_output=True, text=True, **kwargs)


def git_rev_parse(base_ref: str) -> str:
    return _run(["git", "-C", str(REPO_ROOT), "rev-parse", base_ref]).stdout.strip()


def add_worktree(dest: Path, base_ref: str) -> None:
    _run(
        [
            "git",
            "-C",
            str(REPO_ROOT),
            "worktree",
            "add",
            "--detach",
            str(dest),
            base_ref,
        ]
    )


def remove_worktree(dest: Path) -> None:
    # Best-effort cleanup — never fail the run over a leftover worktree.
    subprocess.run(
        ["git", "-C", str(REPO_ROOT), "worktree", "remove", "--force", str(dest)],
        capture_output=True,
        text=True,
    )


def ensure_base_venv(venv_dir: Path) -> None:
    """Create the shared venv once and install snowflake-cli + deps into it.

    Installs the ``development`` extra (pytest, syrupy, ...) so the agent has the
    project's test toolchain available and never needs to ``pip install`` mid-run
    (which would mutate the shared venv unpredictably). Subsequent cases only relink
    the editable package (see :func:`relink_cli`), so this heavy install happens once
    per runner invocation.

    Uses a ``.ready`` sentinel (written after a successful install) rather than checking
    ``bin/python`` existence: ``python -m venv`` creates ``bin/python`` before the pip
    install runs, so a failed install would leave a half-built venv that every subsequent
    case silently reuses.
    """
    if (venv_dir / ".ready").exists():
        return
    _run([sys.executable, "-m", "venv", str(venv_dir)])
    _run([str(venv_dir / "bin" / "pip"), "install", "-e", f"{REPO_ROOT}[development]"])
    venv_dir.mkdir(parents=True, exist_ok=True)
    (venv_dir / ".ready").touch()


def relink_cli(venv_dir: Path, worktree: Path) -> None:
    """Point the shared venv's ``snow`` at the mutated worktree (editable, no deps)."""
    _run([str(venv_dir / "bin" / "pip"), "install", "-e", str(worktree), "--no-deps"])


def agent_env(venv_dir: Path, extra_env: dict[str, str] | None = None) -> dict:
    """Environment for the cortex subprocess: the mutated venv's ``snow`` first on PATH,
    plus any *extra_env* (the runner passes ``SNOWFLAKE_HOME`` pointing at the temp
    reviewer config, which cortex's child ``snow`` processes inherit)."""
    env = os.environ.copy()
    env["PATH"] = f"{venv_dir / 'bin'}{os.pathsep}{env.get('PATH', '')}"
    if extra_env:
        env.update(extra_env)
    return env


def _read_user_connection(name: str) -> dict[str, str]:
    """Read a connection's fields from the user's Snowflake config (read-only).

    Looks in ``$SNOWFLAKE_HOME`` (default ``~/.snowflake``): a top-level ``[name]``
    section in ``connections.toml`` or a ``[connections.name]`` section in
    ``config.toml``. Returns the section's fields coerced to strings.
    """
    home = Path(os.environ.get("SNOWFLAKE_HOME") or "~/.snowflake").expanduser()
    for path, keys in (
        (home / "connections.toml", (name,)),
        (home / "config.toml", ("connections", name)),
    ):
        if not path.exists():
            continue
        node: object = tomllib.loads(path.read_text())
        for k in keys:
            node = node.get(k, {}) if isinstance(node, dict) else {}
        if isinstance(node, dict) and node:
            return {k: str(v) for k, v in node.items()}
    raise RuntimeError(
        f"connection {name!r} not found in {home}/connections.toml or config.toml"
    )


def _connection_creds(source_connection: str) -> dict[str, str]:
    """Reviewer connection creds: env creds (CI) or a clone of the user's config."""
    prefix = f"SNOWFLAKE_CONNECTIONS_{source_connection.upper()}"
    if os.environ.get(f"{prefix}_ACCOUNT"):
        env_map = {
            "account": "ACCOUNT",
            "user": "USER",
            "authenticator": "AUTHENTICATOR",
            "host": "HOST",
            "warehouse": "WAREHOUSE",
            "role": "ROLE",
            "password": "PASSWORD",
            "private_key_file": "PRIVATE_KEY_FILE",
            "private_key_raw": "PRIVATE_KEY_RAW",
        }
        return {
            field: os.environ[f"{prefix}_{envk}"]
            for field, envk in env_map.items()
            if os.environ.get(f"{prefix}_{envk}")
        }
    return _read_user_connection(source_connection)


def build_reviewer_config(
    creds: dict[str, str], tmp_dir: Path, database: str | None
) -> tuple[str, dict[str, str]]:
    """Materialize the ``e2ereviewer`` connection the agent uses, pointed at the playground.

    Given cloned *creds* (from :func:`_connection_creds`), overrides the default
    database to the ephemeral playground (when given),
    writes a temp ``connections.toml`` with an ``[e2ereviewer]`` section, and returns
    ``(config_file, env_overrides)`` where ``env_overrides`` sets ``SNOWFLAKE_HOME`` to
    *tmp_dir*.

    Both cortex (via ``--config-file``) and every ``snow`` the agent spawns (via the
    inherited ``SNOWFLAKE_HOME``) then resolve ``--connection e2ereviewer`` to this
    file, with the playground as the default database — so all the agent's work is
    confined to the ephemeral DB and the user's ``~/.snowflake`` is never modified.
    (Verified empirically: ``snow --connection e2ereviewer`` and a cortex run both work
    with ``SNOWFLAKE_HOME`` pointed here; the ``SNOWFLAKE_CONNECTIONS_*`` env-var
    convention does NOT define a named connection for the CLI, so it can't be used.)

    Never writes outside *tmp_dir*.
    """
    creds = dict(creds)
    # A raw PEM can't live inline in connections.toml — spill it to a temp key file.
    raw = creds.pop("private_key_raw", "")
    if raw:
        key_path = tmp_dir / "rsa_key.p8"
        key_path.write_text(raw)
        os.chmod(key_path, 0o600)
        creds["private_key_file"] = str(key_path)
    if database:
        creds["database"] = database
    config_file = tmp_dir / "connections.toml"
    rev.write_connections_toml(creds, str(config_file))
    return str(config_file), {"SNOWFLAKE_HOME": str(tmp_dir)}


def playground_name(case: Case, key: str) -> str:
    """Deterministic, valid-identifier playground DB name for a case run."""
    safe = "".join(ch if ch.isalnum() else "_" for ch in case.name).upper()
    return f"CORTEX_EVAL_{safe}_{key[:8].upper()}"


def _admin_connect(creds: dict[str, str]):
    """Connect via the Python connector for DB admin — independent of the (mutated) CLI.

    Provisioning must not go through the worktree's ``snow`` binary: a case may mutate
    (or break) ``snow sql`` itself, which would fail provisioning. The connector uses the
    same cloned creds directly. Key-pair material is resolved by the reviewer's helper.

    The source connection **must** use key-pair authentication — username/password creds
    cannot be written into a connections.toml for the agent's child processes.
    """
    cfg = {k: v for k, v in creds.items() if v and k != "database"}
    cfg.setdefault("application", "CORTEX_EVAL_RUNNER")
    cfg.setdefault("authenticator", "SNOWFLAKE_JWT")
    update_connection_details_with_private_key(cfg)
    if not cfg.get("private_key"):
        raise RuntimeError(
            "The source connection must use key-pair authentication (private_key_file or "
            "private_key_raw). Username/password connections are not supported because the "
            "runner writes a connections.toml for the agent's child processes."
        )
    return snowflake.connector.connect(**cfg)


def provision_playground(db: str, creds: dict[str, str]) -> None:
    conn = _admin_connect(creds)
    try:
        conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    finally:
        conn.close()


def drop_playground(db: str, creds: dict[str, str]) -> None:
    # Best-effort teardown.
    try:
        conn = _admin_connect(creds)
        try:
            conn.cursor().execute(f"DROP DATABASE IF EXISTS {db}")
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        print(f"  Warning: could not drop playground {db}: {exc}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _cached_result(result_path: Path, key: str) -> CaseResult | None:
    if not result_path.exists():
        return None
    try:
        prior = json.loads(result_path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    if prior.get("cache_key") != key:
        return None
    known = set(CaseResult.__dataclass_fields__)
    result = CaseResult(**{k: v for k, v in prior.items() if k in known})
    result.from_cache = True
    return result


def run_case(
    case: Case,
    *,
    template: str,
    diff_instructions: str,
    model: str,
    connection: str = DEFAULT_CONNECTION,
    base_ref: str = DEFAULT_BASE_REF,
    venv_dir: Path,
    results_dir: Path,
    timeout: int = rev.AGENT_TIMEOUT_SEC,
    use_cache: bool = True,
) -> CaseResult:
    """Run one case end-to-end and persist its artifacts; return the CaseResult.

    The cache key is derived from the *prompt spec* (template + diff instructions),
    model, case, and base SHA — not the ephemeral playground name — so re-runs of an
    unchanged (prompt, model) hit the cache.
    """
    base = git_rev_parse(base_ref)
    key = cache_key(template + "\x1f" + diff_instructions, model, case.name, base)
    case_dir = results_dir / case.name
    result_path = case_dir / "result.json"

    cached = _cached_result(result_path, key) if use_cache else None
    if cached is not None:
        print(f"[{case.name}] cache hit ({key}) — skipping agent run")
        return cached

    case_dir.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix=f"eval_{case.name}_"))
    worktree = tmp / "worktree"
    db = playground_name(case, key)
    provisioned = False
    source_creds: dict[str, str] = {}
    try:
        add_worktree(worktree, base_ref)
        case.mutate(worktree)
        ensure_base_venv(venv_dir)
        relink_cli(venv_dir, worktree)

        # Clone the source connection's creds (user config / env) once, then provision
        # the ephemeral playground via the Python connector — NOT the worktree's `snow`,
        # which a mutation may have broken.
        source_creds = _connection_creds(connection)
        provision_playground(db, source_creds)
        provisioned = True

        # The agent connects via [e2ereviewer], whose default database IS the playground:
        # cortex reads it from --config-file, and the agent's `snow` children resolve it
        # from the inherited SNOWFLAKE_HOME. All the agent's work is confined to the
        # ephemeral DB; the user's ~/.snowflake is never touched.
        config_file, conn_env = build_reviewer_config(source_creds, tmp, db)
        env = agent_env(venv_dir, conn_env)

        prompt = build_prompt(template, diff_instructions, db)
        agent = rev.run_agent(
            prompt=prompt,
            model=model,
            workdir=str(worktree),
            connection=REVIEWER_CONNECTION,
            config_file=config_file,
            timeout=timeout,
            env=env,
        )
        result = CaseResult(
            case=case.name,
            model=model,
            base_sha=base,
            cache_key=key,
            verdict=rev.parse_verdict(agent.report),
            expected_verdict=case.expected_verdict,
            expected_breaking_changes=case.expected_breaking_changes,
            needs_live_snowflake=case.needs_live_snowflake,
            exit_code=agent.exit_code,
            duration_sec=round(agent.duration, 1),
            executed_commands=extract_executed_commands(agent.stdout),
        )
        (case_dir / "prompt.txt").write_text(prompt)
        (case_dir / "report.md").write_text(agent.report)
        (case_dir / "stdout.jsonl").write_text(agent.stdout)
        result_path.write_text(json.dumps(asdict(result), indent=2))
        print(
            f"[{case.name}] verdict={result.verdict} expected={case.expected_verdict} "
            f"commands={len(result.executed_commands)} ({agent.duration:.0f}s)"
        )
        return result
    finally:
        if provisioned:
            drop_playground(db, source_creds)
        remove_worktree(worktree)
        shutil.rmtree(tmp, ignore_errors=True)


def run_eval(
    *,
    cases: list[Case],
    template: str,
    diff_instructions: str,
    model: str,
    connection: str,
    base_ref: str,
    venv_dir: Path,
    results_dir: Path,
    timeout: int,
    use_cache: bool,
) -> list[CaseResult]:
    results: list[CaseResult] = []
    for case in cases:
        try:
            results.append(
                run_case(
                    case,
                    template=template,
                    diff_instructions=diff_instructions,
                    model=model,
                    connection=connection,
                    base_ref=base_ref,
                    venv_dir=venv_dir,
                    results_dir=results_dir,
                    timeout=timeout,
                    use_cache=use_cache,
                )
            )
        except Exception as exc:  # noqa: BLE001 — one bad case must not abort the sweep
            print(f"[{case.name}] ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
    return results


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Run the E2E PR-review eval locally.")
    p.add_argument(
        "-i",
        "--case",
        action="append",
        dest="patterns",
        help="case name or glob to run (repeatable; default: all)",
    )
    p.add_argument("--model", default=os.environ.get("CORTEX_MODEL", DEFAULT_MODEL))
    p.add_argument("--connection", default=DEFAULT_CONNECTION)
    p.add_argument("--base-ref", default=DEFAULT_BASE_REF)
    p.add_argument(
        "--venv-dir", default=None, help="shared venv dir (default: <results>/venv)"
    )
    p.add_argument("--results-dir", default="eval_results")
    p.add_argument("--timeout", type=int, default=rev.AGENT_TIMEOUT_SEC)
    p.add_argument("--no-cache", action="store_true")
    args = p.parse_args(argv)

    if not shutil.which("cortex"):
        print(
            "error: 'cortex' not found on PATH. Install it or add ~/.local/bin to PATH.",
            file=sys.stderr,
        )
        return 1

    results_dir = Path(args.results_dir).resolve()
    venv_dir = Path(args.venv_dir).resolve() if args.venv_dir else results_dir / "venv"
    cases = select_cases(load_all_cases(), args.patterns)
    if not cases:
        print("No cases matched.", file=sys.stderr)
        return 1
    print(f"Running {len(cases)} case(s) with model={args.model} base={args.base_ref}")
    results = run_eval(
        cases=cases,
        template=rev.AGENT_PROMPT_TEMPLATE,
        diff_instructions=rev.LOCAL_DIFF_INSTRUCTIONS,
        model=args.model,
        connection=args.connection,
        base_ref=args.base_ref,
        venv_dir=venv_dir,
        results_dir=results_dir,
        timeout=args.timeout,
        use_cache=not args.no_cache,
    )
    n_ok = len(results)
    n_err = len(cases) - n_ok
    if n_err:
        print(f"{n_ok} ok / {n_err} errored", file=sys.stderr)
    else:
        print(f"{n_ok} case(s) completed")
    print(f"Results written to {results_dir}")
    return 0 if results else 1


if __name__ == "__main__":
    sys.exit(main())
