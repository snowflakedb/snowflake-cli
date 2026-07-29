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

"""Scorer for the E2E PR-review eval.

Consumes the per-case artifacts the runner persists under ``<results>/<case>/``
(``report.md``, ``stdout.jsonl``, ``result.json``) plus the case's ground truth
(``case.toml`` via :mod:`cases`) and produces, per case:

**Deterministic checks** (no LLM, no network):
- ``marker_present`` — the agent emitted the required ``<!-- E2E_REPORT -->``
  marker. Checked against the *raw* ``stdout.jsonl``, because the runner's
  ``_parse_stream_json`` strips the marker out of ``report.md``.
- ``all_sections_present`` — all six required report sections are present.
- ``verdict_correct`` — the parsed verdict equals ``expected_verdict``.
- ``breaking_changes_correct`` — whether the report's *Breaking Changes* section
  reports a breaking change matches ``expected_breaking_changes``.
- ``command_coverage`` — did the agent actually run the ``expected_commands``,
  checked against the tool trajectory (``executed_commands`` extracted from
  ``stdout.jsonl``). Only executed commands count — a command merely mentioned in
  the report text does not.

**LLM judge** (optional, ``--judge``) via Snowflake AI Observability's
``trulens-providers-cortex`` — ``CORTEX.COMPLETE`` under the hood, no separate
Corvo access. Two custom criteria (both higher-is-better): ``coverage`` (does the
report address the case rubric?) and ``no_overclaim`` (did it fabricate
validation it couldn't have performed?).

**Aggregates**: verdict accuracy, false-positive / false-negative rate, SKIP
precision / recall, section completeness, breaking-change accuracy, mean command
coverage, and mean judge scores — reported overall and per slice (by expected
verdict, by difficulty, by tag).

Offline by default: without ``--judge`` the scorer touches neither Snowflake nor
the network, so it can score persisted artifacts anywhere. Tests mock the judge
provider entirely.
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Iterable

_EVAL_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _EVAL_DIR.parent

for _p in (str(_EVAL_DIR), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import cortex_e2e_review as rev  # noqa: E402
from cases import Case, load_all_cases  # noqa: E402

# The marker the prompt requires the agent to open its report with. The runner's
# _parse_stream_json strips it from report.md, so it survives only in the raw
# stream — the scorer checks the raw stdout for it.
REPORT_MARKER = "<!-- E2E_REPORT -->"

# The six sections AGENT_PROMPT_TEMPLATE mandates, in order.
REQUIRED_SECTIONS = (
    "Summary",
    "E2E Test Results",
    "Breaking Changes",
    "Side-Effect Verification",
    "Potential Risks",
    "Verdict",
)

# The prompt's canonical "nothing to report" phrasing for Breaking Changes is
# "No breaking changes detected." We treat a Breaking Changes section that only
# says some variant of that (or "none"/"n/a") as *not* reporting a breaking change.
_NO_BREAKING_RE = re.compile(
    r"\s*(no\b.*?breaking|none\b|n/?a\b|not\s+applicable\b)", re.IGNORECASE
)

DEFAULT_JUDGE_MODEL = "claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# Deterministic report parsing (pure — unit-tested)
# ---------------------------------------------------------------------------


def _normalize_heading(text: str) -> str:
    """Collapse a heading to alphanumerics so "Side-Effect Verification" and
    "Side Effect Verification" compare equal."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def split_sections(report: str) -> dict[str, str]:
    """Map each ``#``-``####`` heading in *report* to its body text.

    Later headings win on duplicate names (the agent occasionally restates a
    section); the body runs until the next heading of any level.
    """
    headings = list(re.finditer(r"(?m)^#{1,4}\s+(.+?)\s*$", report))
    sections: dict[str, str] = {}
    for i, m in enumerate(headings):
        end = headings[i + 1].start() if i + 1 < len(headings) else len(report)
        sections[m.group(1).strip()] = report[m.end() : end].strip()
    return sections


def _sections_by_norm(report: str) -> dict[str, str]:
    return {
        _normalize_heading(name): body for name, body in split_sections(report).items()
    }


def check_sections(report: str) -> dict[str, bool]:
    """Presence of each required section, keyed by its canonical name."""
    norm = _sections_by_norm(report)
    return {name: _normalize_heading(name) in norm for name in REQUIRED_SECTIONS}


def reports_breaking_change(report: str) -> bool:
    """Whether the report's *Breaking Changes* section asserts a breaking change.

    Absent section → False. A section whose body opens with a "no breaking
    changes" / "none" / "n/a" variant → False. Anything else → True.
    """
    body = _sections_by_norm(report).get(_normalize_heading("Breaking Changes"), "")
    body = body.strip()
    if not body:
        return False
    return _NO_BREAKING_RE.match(body) is None


def _normalize_cmd(cmd: str) -> str:
    return re.sub(r"\s+", " ", cmd.strip())


@dataclass
class CoverageResult:
    coverage: float
    covered: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)


def command_coverage(expected: list[str], executed: list[str]) -> CoverageResult:
    """Fraction of *expected* command fragments the agent actually ran.

    A fragment is matched (normalized-whitespace substring) against the *executed*
    trajectory — the ``executed_commands`` extracted from ``stdout.jsonl``, the
    near-deterministic "did it actually run this" signal. The report text is
    deliberately not consulted: a command the agent merely *mentions* in its report
    but never executed does not count as covered.
    """
    if not expected:
        return CoverageResult(coverage=1.0)
    exec_norm = [_normalize_cmd(c) for c in executed]
    covered: list[str] = []
    missing: list[str] = []
    for frag in expected:
        needle = _normalize_cmd(frag)
        if any(needle in c for c in exec_norm):
            covered.append(frag)
        else:
            missing.append(frag)
    return CoverageResult(
        coverage=len(covered) / len(expected),
        covered=covered,
        missing=missing,
    )


# ---------------------------------------------------------------------------
# LLM judge (optional) — trulens-providers-cortex
# ---------------------------------------------------------------------------


@dataclass
class JudgeCriterion:
    """A single LLM-judge dimension. ``system_prompt`` may reference ``{rubric}``."""

    name: str
    system_prompt: str


# Both criteria are higher-is-better so they aggregate (mean) consistently. Each is
# shown the report AND the executed-command trajectory (see _format_judge_input) — the
# same trajectory the deterministic coverage check uses — so judgments are grounded in
# what the agent actually ran, not just what the summary report happens to paste.
COVERAGE_CRITERION = JudgeCriterion(
    name="coverage",
    system_prompt=(
        "You are grading an automated end-to-end CLI PR-review report against a "
        "rubric describing what a correct review must establish.\n\n"
        "RUBRIC:\n{rubric}\n\n"
        "You are given the reviewer's report AND the actual EXECUTED-COMMAND "
        "TRAJECTORY (the exact shell commands the agent ran), which is evidence of "
        "what was truly investigated.\n\n"
        "Rate on a 0-10 scale how completely the report addresses the rubric: "
        "10 = every point is investigated (as corroborated by the trajectory) and "
        "reported, 0 = the rubric is essentially ignored. Respond with the integer "
        "score."
    ),
)

NO_OVERCLAIM_CRITERION = JudgeCriterion(
    name="no_overclaim",
    system_prompt=(
        "You are auditing an automated end-to-end CLI PR-review report for "
        "OVERCLAIMING.\n\n"
        "You are given the reviewer's report AND the actual EXECUTED-COMMAND "
        "TRAJECTORY (the exact shell commands the agent ran). The report is a written "
        "summary, so it legitimately does NOT paste raw stdout/transcripts — do NOT "
        "penalize the mere absence of shown output.\n\n"
        "Overclaiming is asserting commands, exit codes, or Snowflake side effects "
        "that are NOT supported by the trajectory: e.g. claiming a command was run "
        "that never appears in the trajectory, or claiming a verification the "
        "trajectory shows no basis for. A claimed result IS substantiated when a "
        "corresponding command appears in the trajectory.\n\n"
        "Rate on a 0-10 scale: 10 = every empirical claim in the report is backed by "
        "a corresponding command in the trajectory, 0 = the report fabricates commands "
        "or results absent from the trajectory. Respond with the integer score."
    ),
)

DEFAULT_CRITERIA: tuple[JudgeCriterion, ...] = (
    COVERAGE_CRITERION,
    NO_OVERCLAIM_CRITERION,
)


@dataclass
class JudgeScore:
    """One criterion's judge result. ``score`` is normalized 0-1 (higher better)."""

    score: float | None
    reason: str = ""
    error: str = ""


def _invoke_provider(
    provider, system_prompt: str, user_prompt: str
) -> tuple[float, str]:
    """Call the TruLens Cortex provider, tolerating both the reasons and the
    bare-score APIs, and normalize the return to ``(score, reason)``."""
    fn = getattr(provider, "generate_score_and_reasons", None)
    if callable(fn):
        out = fn(system_prompt=system_prompt, user_prompt=user_prompt)
        if isinstance(out, tuple):
            score, meta = out[0], (out[1] if len(out) > 1 else {})
        else:
            score, meta = out, {}
        reason = meta.get("reason", "") if isinstance(meta, dict) else str(meta)
        return float(score), str(reason)
    score = provider.generate_score(
        system_prompt=system_prompt, user_prompt=user_prompt
    )
    return float(score), ""


def _format_judge_input(report: str, executed_commands: list[str]) -> str:
    """Combine the report with the executed-command trajectory into one user prompt.

    Giving the judge the trajectory (the same signal the deterministic coverage check
    uses) lets it distinguish a truthful *summary* from a *fabrication*: a claimed
    command is substantiated iff it appears here, regardless of whether the report
    pasted its raw stdout.
    """
    if executed_commands:
        trajectory = "\n".join(f"{i}. {c}" for i, c in enumerate(executed_commands, 1))
    else:
        trajectory = "(no commands captured)"
    return (
        "## Reviewer report\n"
        f"{report}\n\n"
        "## Executed-command trajectory (the actual shell commands the agent ran)\n"
        f"{trajectory}\n"
    )


def judge_report(
    provider,
    report: str,
    case: Case,
    executed_commands: list[str] | None = None,
    criteria: Iterable[JudgeCriterion] = DEFAULT_CRITERIA,
) -> dict[str, JudgeScore]:
    """Score *report* against each criterion using the TruLens Cortex *provider*.

    The judge sees the report *and* the executed-command *trajectory* (see
    :func:`_format_judge_input`) so it can ground claims in what actually ran. A
    criterion that raises is captured as a :class:`JudgeScore` with ``error`` set
    (and ``score=None``) so one bad judge call never sinks the whole case.
    """
    rubric = case.rubric or "(no rubric provided)"
    user_prompt = _format_judge_input(report, executed_commands or [])
    scores: dict[str, JudgeScore] = {}
    for crit in criteria:
        system_prompt = crit.system_prompt.format(rubric=rubric)
        try:
            score, reason = _invoke_provider(provider, system_prompt, user_prompt)
            scores[crit.name] = JudgeScore(score=score, reason=reason)
        except Exception as exc:  # noqa: BLE001 — surface, don't abort
            scores[crit.name] = JudgeScore(
                score=None, error=f"{type(exc).__name__}: {exc}"
            )
    return scores


def build_cortex_provider(session, model_engine: str = DEFAULT_JUDGE_MODEL):
    """Construct the TruLens Cortex judge provider from a Snowpark *session*.

    Imported lazily so the deterministic path (and the offline tests) never need
    ``trulens`` installed. The provider issues ``CORTEX.COMPLETE`` calls over the
    same connection the reviewer uses — no separate model access required.
    """
    from trulens.providers.cortex import Cortex

    return Cortex(snowpark_session=session, model_engine=model_engine)


def build_snowpark_session(connection: str):
    """Open a Snowpark session for the judge, cloning the runner's connection creds.

    Live-only (guarded behind ``--judge``); imported lazily.
    """
    import runner
    from snowflake.cli._app.snow_connector import (
        update_connection_details_with_private_key,
    )
    from snowflake.snowpark import Session

    creds = {
        k: v
        for k, v in runner._connection_creds(connection).items()  # noqa: SLF001
        if v
    }
    creds.setdefault("application", "CORTEX_EVAL_SCORER")
    creds.setdefault("authenticator", "SNOWFLAKE_JWT")
    update_connection_details_with_private_key(creds)
    return Session.builder.configs(creds).create()


# ---------------------------------------------------------------------------
# Per-case scoring
# ---------------------------------------------------------------------------


@dataclass
class CaseScore:
    case: str
    expected_verdict: str
    verdict: str | None
    verdict_correct: bool
    difficulty: str
    tags: list[str]
    marker_present: bool
    sections_present: dict[str, bool]
    all_sections_present: bool
    reported_breaking_changes: bool
    expected_breaking_changes: bool
    breaking_changes_correct: bool
    command_coverage: float
    commands_covered: list[str]
    commands_missing: list[str]
    exit_code: int | None = None
    from_cache: bool = False
    judge: dict[str, JudgeScore] | None = None


def score_case(
    case: Case,
    report: str,
    executed_commands: list[str],
    *,
    raw_stdout: str = "",
    exit_code: int | None = None,
    from_cache: bool = False,
    provider=None,
    criteria: Iterable[JudgeCriterion] = DEFAULT_CRITERIA,
) -> CaseScore:
    """Score a single case's artifacts against its ground truth.

    Ground truth (``expected_*``, ``rubric``, ``difficulty``, ``tags``) comes from
    *case*; the observations (*report*, *executed_commands*, *raw_stdout*) come
    from the run. The verdict is parsed from *report* here — that parse *is* the
    deterministic verdict check.
    """
    verdict = rev.parse_verdict(report)
    sections = check_sections(report)
    cov = command_coverage(case.expected_commands, executed_commands)
    reported_breaking = reports_breaking_change(report)
    judge = (
        judge_report(provider, report, case, executed_commands, criteria)
        if provider is not None
        else None
    )
    return CaseScore(
        case=case.name,
        expected_verdict=case.expected_verdict,
        verdict=verdict,
        verdict_correct=verdict == case.expected_verdict,
        difficulty=case.difficulty or "unknown",
        tags=list(case.tags),
        # The marker is stripped from report.md, so look in the raw stream first.
        marker_present=REPORT_MARKER in raw_stdout or REPORT_MARKER in report,
        sections_present=sections,
        all_sections_present=all(sections.values()),
        reported_breaking_changes=reported_breaking,
        expected_breaking_changes=case.expected_breaking_changes,
        breaking_changes_correct=reported_breaking == case.expected_breaking_changes,
        command_coverage=cov.coverage,
        commands_covered=cov.covered,
        commands_missing=cov.missing,
        exit_code=exit_code,
        from_cache=from_cache,
        judge=judge,
    )


# ---------------------------------------------------------------------------
# Aggregates + slices
# ---------------------------------------------------------------------------


def _rate(numerator: int, denominator: int) -> float | None:
    """Fraction, or ``None`` when the slice is empty (avoids misleading zeros)."""
    return numerator / denominator if denominator else None


@dataclass
class Aggregate:
    n: int
    verdict_accuracy: float | None
    false_positive_rate: float | None  # expected != FAIL but verdict == FAIL
    false_negative_rate: float | None  # expected == FAIL but verdict != FAIL
    skip_precision: float | None
    skip_recall: float | None
    marker_rate: float | None
    section_completeness: float | None
    breaking_changes_accuracy: float | None
    mean_command_coverage: float | None
    mean_judge: dict[str, float]


def aggregate(scores: list[CaseScore]) -> Aggregate:
    n = len(scores)

    non_fail = [s for s in scores if s.expected_verdict != "FAIL"]
    exp_fail = [s for s in scores if s.expected_verdict == "FAIL"]
    said_skip = [s for s in scores if s.verdict == "SKIP"]
    exp_skip = [s for s in scores if s.expected_verdict == "SKIP"]

    mean_judge: dict[str, float] = {}
    judged = [s for s in scores if s.judge]
    if judged:
        by_crit: dict[str, list[float]] = defaultdict(list)
        for s in judged:
            for name, js in s.judge.items():  # type: ignore[union-attr]
                if js.score is not None:
                    by_crit[name].append(js.score)
        mean_judge = {k: sum(v) / len(v) for k, v in by_crit.items() if v}

    return Aggregate(
        n=n,
        verdict_accuracy=_rate(sum(s.verdict_correct for s in scores), n),
        false_positive_rate=_rate(
            sum(s.verdict == "FAIL" for s in non_fail), len(non_fail)
        ),
        false_negative_rate=_rate(
            sum(s.verdict != "FAIL" for s in exp_fail), len(exp_fail)
        ),
        skip_precision=_rate(
            sum(s.expected_verdict == "SKIP" for s in said_skip), len(said_skip)
        ),
        skip_recall=_rate(sum(s.verdict == "SKIP" for s in exp_skip), len(exp_skip)),
        marker_rate=_rate(sum(s.marker_present for s in scores), n),
        section_completeness=_rate(sum(s.all_sections_present for s in scores), n),
        breaking_changes_accuracy=_rate(
            sum(s.breaking_changes_correct for s in scores), n
        ),
        mean_command_coverage=(
            sum(s.command_coverage for s in scores) / n if n else None
        ),
        mean_judge=mean_judge,
    )


def _slice(
    scores: list[CaseScore], key: Callable[[CaseScore], list[str]]
) -> dict[str, Aggregate]:
    groups: dict[str, list[CaseScore]] = defaultdict(list)
    for s in scores:
        for k in key(s):
            groups[k].append(s)
    return {k: aggregate(groups[k]) for k in sorted(groups)}


@dataclass
class ScoreReport:
    n: int
    overall: Aggregate
    slices: dict[str, dict[str, Aggregate]]
    cases: list[CaseScore]


def build_report(scores: list[CaseScore]) -> ScoreReport:
    return ScoreReport(
        n=len(scores),
        overall=aggregate(scores),
        slices={
            "expected_verdict": _slice(scores, lambda s: [s.expected_verdict]),
        },
        cases=scores,
    )


# ---------------------------------------------------------------------------
# Loading persisted artifacts
# ---------------------------------------------------------------------------


def score_results_dir(
    results_dir: Path,
    cases: dict[str, Case] | None = None,
    *,
    provider=None,
    criteria: Iterable[JudgeCriterion] = DEFAULT_CRITERIA,
) -> ScoreReport:
    """Score every case under *results_dir* that has a persisted ``result.json``."""
    if cases is None:
        cases = {c.name: c for c in load_all_cases()}
    scores: list[CaseScore] = []
    for case_dir in sorted(p for p in results_dir.iterdir() if p.is_dir()):
        result_path = case_dir / "result.json"
        if not result_path.exists():
            continue
        result = json.loads(result_path.read_text())
        case = cases.get(result.get("case", case_dir.name))
        if case is None:
            print(
                f"[{case_dir.name}] no matching case definition — skipping",
                file=sys.stderr,
            )
            continue
        report = (
            (case_dir / "report.md").read_text()
            if (case_dir / "report.md").exists()
            else ""
        )
        stdout_path = case_dir / "stdout.jsonl"
        raw_stdout = stdout_path.read_text() if stdout_path.exists() else ""
        scores.append(
            score_case(
                case,
                report,
                list(result.get("executed_commands", [])),
                raw_stdout=raw_stdout,
                exit_code=result.get("exit_code"),
                from_cache=bool(result.get("from_cache", False)),
                provider=provider,
                criteria=criteria,
            )
        )
    return build_report(scores)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _pct(x: float | None) -> str:
    return "—" if x is None else f"{x:.0%}"


def _f2(x: float | None) -> str:
    return "—" if x is None else f"{x:.2f}"


def _agg_row(label: str, a: Aggregate) -> str:
    return (
        f"| {label} | {a.n} | {_pct(a.verdict_accuracy)} | "
        f"{_pct(a.false_positive_rate)} | {_pct(a.false_negative_rate)} | "
        f"{_pct(a.skip_precision)} | {_pct(a.skip_recall)} | "
        f"{_pct(a.marker_rate)} | {_pct(a.section_completeness)} | "
        f"{_pct(a.breaking_changes_accuracy)} | {_pct(a.mean_command_coverage)} |"
    )


_AGG_HEADER = (
    "| slice | n | verdict acc | FP rate | FN rate | SKIP prec | SKIP rec | "
    "marker | sections | breaking acc | cmd cov |\n"
    "|---|---|---|---|---|---|---|---|---|---|---|"
)


def render_markdown(report: ScoreReport) -> str:
    out: list[str] = ["# E2E reviewer eval — scores", ""]
    out.append(f"Scored **{report.n}** case(s).")
    out.append("")

    out.append("## Overall")
    out.append("")
    out.append(_AGG_HEADER)
    out.append(_agg_row("overall", report.overall))
    if report.overall.mean_judge:
        judged = ", ".join(
            f"{k}={_f2(v)}" for k, v in sorted(report.overall.mean_judge.items())
        )
        out.append("")
        out.append(f"Mean judge scores: {judged}")
    out.append("")

    for slice_name, groups in report.slices.items():
        out.append(f"## By {slice_name}")
        out.append("")
        out.append(_AGG_HEADER)
        for key, agg in groups.items():
            out.append(_agg_row(key, agg))
        judge_lines = [
            f"{key}: "
            + ", ".join(f"{k}={_f2(v)}" for k, v in sorted(agg.mean_judge.items()))
            for key, agg in groups.items()
            if agg.mean_judge
        ]
        if judge_lines:
            out.append("")
            out.append("Mean judge scores — " + "; ".join(judge_lines))
        out.append("")

    out.append("## Per case")
    out.append("")
    out.append(
        "| case | expected | verdict | ✓ | marker | sections | breaking ✓ | cmd cov | judge |\n"
        "|---|---|---|---|---|---|---|---|---|"
    )
    for s in report.cases:
        judge_str = "—"
        if s.judge:
            judge_str = ", ".join(
                f"{k}={_f2(v.score)}" for k, v in sorted(s.judge.items())
            )
        out.append(
            f"| {s.case} | {s.expected_verdict} | {s.verdict or '—'} | "
            f"{'✅' if s.verdict_correct else '❌'} | "
            f"{'✅' if s.marker_present else '❌'} | "
            f"{'✅' if s.all_sections_present else '❌'} | "
            f"{'✅' if s.breaking_changes_correct else '❌'} | "
            f"{_pct(s.command_coverage)} | {judge_str} |"
        )
    out.append("")
    return "\n".join(out)


def _report_to_dict(report: ScoreReport) -> dict:
    return asdict(report)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="Score persisted E2E PR-review eval runs.")
    p.add_argument("--results-dir", default="eval_results")
    p.add_argument(
        "--json-out",
        default=None,
        help="write the full score report as JSON (default: <results>/scores.json)",
    )
    p.add_argument(
        "--markdown-out",
        default=None,
        help="write the markdown summary (default: <results>/scores.md)",
    )
    p.add_argument(
        "--judge",
        action="store_true",
        help="run the LLM judge (requires a live Snowflake connection)",
    )
    p.add_argument("--judge-model", default=DEFAULT_JUDGE_MODEL)
    p.add_argument(
        "--connection",
        default="e2ereviewer",
        help="source connection whose creds the judge session clones",
    )
    args = p.parse_args(argv)

    results_dir = Path(args.results_dir).resolve()
    if not results_dir.exists():
        print(f"Results dir not found: {results_dir}", file=sys.stderr)
        return 1

    session = None
    provider = None
    if args.judge:
        session = build_snowpark_session(args.connection)
        provider = build_cortex_provider(session, args.judge_model)

    try:
        report = score_results_dir(results_dir, provider=provider)
        if report.n == 0:
            print(f"No scored cases under {results_dir}", file=sys.stderr)
            return 1

        json_out = Path(args.json_out) if args.json_out else results_dir / "scores.json"
        md_out = (
            Path(args.markdown_out) if args.markdown_out else results_dir / "scores.md"
        )
        json_out.write_text(
            json.dumps(_report_to_dict(report), indent=2), encoding="utf-8"
        )
        markdown = render_markdown(report)
        md_out.write_text(markdown, encoding="utf-8")

        print(markdown)
        print(f"\nWrote {json_out} and {md_out}")
        return 0
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    sys.exit(main())
