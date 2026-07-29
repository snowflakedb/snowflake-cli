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

"""A/B compare for the E2E PR-review eval.

Runs the reviewer over the mutation dataset **twice** — once with the *baseline*
prompt+model (auto-extracted from a git ref, ``origin/main`` by default) and once
with the *candidate* prompt+model (the working-tree reviewer script) — then diffs
the two :class:`scorer.ScoreReport` s and emits a markdown + JSON report of
per-metric and **per-slice deltas**, explicitly flagging any slice that regressed
(e.g. SKIP precision or the false-positive rate worsening even when the overall
average improves).

The two sides differ only in the (prompt, model) they run under: both are
evaluated against the *same* mutation-on-``base_ref`` dataset, so a delta is
attributable to the prompt/model change, not to Snowflake or CLI drift.

**Baseline extraction is static** (``ast`` — no import, no execution): the
baseline ``AGENT_PROMPT_TEMPLATE`` / ``LOCAL_DIFF_INSTRUCTIONS`` and the default
model are parsed out of ``git show <ref>:.github/scripts/cortex_e2e_review.py``.
The candidate spec is parsed the same way from the working-tree file, so both
sides go through one code path.

Offline by default for the *diff* (``--skip-run`` scores already-persisted
``<results>/baseline`` and ``<results>/candidate`` dirs and diffs them); running
the agents needs the same live prerequisites as :mod:`runner`. Tests mock the
runner, the judge, and ``git show`` entirely.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

_EVAL_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _EVAL_DIR.parent

for _p in (str(_EVAL_DIR), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import runner  # noqa: E402
import scorer  # noqa: E402
from prompt_spec import (  # noqa: E402
    REVIEWER_REL_PATH,
    WORKFLOW_REL_PATH,
    PromptSpec,
    extract_prompt_spec,
)
from scorer import Aggregate, ScoreReport  # noqa: E402

# Deterministic aggregate metrics to diff, mapped to whether higher is better. The
# two error rates are the only lower-is-better metrics — a candidate that raises
# its false-positive rate has regressed even though the number went up.
METRIC_HIGHER_IS_BETTER: dict[str, bool] = {
    "verdict_accuracy": True,
    "false_positive_rate": False,
    "false_negative_rate": False,
    "skip_precision": True,
    "skip_recall": True,
    "marker_rate": True,
    "section_completeness": True,
    "breaking_changes_accuracy": True,
    "mean_command_coverage": True,
}


def _git_show(repo_relative_ref: str) -> str:
    """``git show <ref>:<path>`` from the repo root, returning file contents."""
    return runner._run(  # noqa: SLF001 — reuse the runner's checked subprocess helper
        ["git", "-C", str(runner.REPO_ROOT), "show", repo_relative_ref]
    ).stdout


def baseline_spec(base_ref: str, model_override: str | None = None) -> PromptSpec:
    """The baseline prompt+model, extracted from ``<base_ref>``'s reviewer script."""
    source = _git_show(f"{base_ref}:{REVIEWER_REL_PATH}")
    workflow_source = _git_show(f"{base_ref}:{WORKFLOW_REL_PATH}")
    return extract_prompt_spec(
        source, workflow_source=workflow_source, model_override=model_override
    )


def candidate_spec(model_override: str | None = None) -> PromptSpec:
    """The candidate prompt+model, extracted from the working-tree reviewer script.

    Parsed from the same file :mod:`cortex_e2e_review` was imported from, via the
    identical ``ast`` path used for the baseline — so the two specs are strictly
    comparable and the "was there even a prompt change?" question is answered by
    comparing their :meth:`PromptSpec.prompt_hash`.
    """
    source = Path(runner.rev.__file__).read_text()
    workflow_path = runner.REPO_ROOT / WORKFLOW_REL_PATH
    workflow_source = workflow_path.read_text() if workflow_path.exists() else None
    return extract_prompt_spec(
        source, workflow_source=workflow_source, model_override=model_override
    )


# ---------------------------------------------------------------------------
# Diff (pure — unit-tested)
# ---------------------------------------------------------------------------


@dataclass
class MetricDelta:
    """One metric compared across the two runs. ``delta = candidate - baseline``."""

    metric: str
    baseline: float | None
    candidate: float | None
    delta: float | None
    higher_is_better: bool
    regressed: bool
    improved: bool


def _metric_delta(
    metric: str, base: float | None, cand: float | None, higher_is_better: bool
) -> MetricDelta:
    if base is None or cand is None:
        # A metric undefined on either side (empty slice) has no comparable delta.
        return MetricDelta(metric, base, cand, None, higher_is_better, False, False)
    delta = cand - base
    if higher_is_better:
        regressed, improved = delta < 0, delta > 0
    else:
        regressed, improved = delta > 0, delta < 0
    return MetricDelta(metric, base, cand, delta, higher_is_better, regressed, improved)


def diff_aggregate(base: Aggregate, cand: Aggregate) -> list[MetricDelta]:
    """Per-metric deltas between two aggregates, deterministic metrics then judge."""
    deltas = [
        _metric_delta(name, getattr(base, name), getattr(cand, name), hib)
        for name, hib in METRIC_HIGHER_IS_BETTER.items()
    ]
    for name in sorted(set(base.mean_judge) | set(cand.mean_judge)):
        deltas.append(
            _metric_delta(
                f"judge:{name}",
                base.mean_judge.get(name),
                cand.mean_judge.get(name),
                higher_is_better=True,
            )
        )
    return deltas


def diff_slices(
    base: ScoreReport, cand: ScoreReport
) -> dict[str, dict[str, list[MetricDelta]]]:
    """Per-slice-key metric deltas, for every slice key present on *both* sides."""
    out: dict[str, dict[str, list[MetricDelta]]] = {}
    for slice_type in sorted(set(base.slices) | set(cand.slices)):
        base_groups = base.slices.get(slice_type, {})
        cand_groups = cand.slices.get(slice_type, {})
        group_deltas: dict[str, list[MetricDelta]] = {}
        for key in sorted(set(base_groups) | set(cand_groups)):
            b_agg, c_agg = base_groups.get(key), cand_groups.get(key)
            if b_agg is None or c_agg is None:
                continue  # slice key only on one side → nothing to diff
            group_deltas[key] = diff_aggregate(b_agg, c_agg)
        out[slice_type] = group_deltas
    return out


@dataclass
class Regression:
    """A single metric that worsened, tagged with where (overall or which slice)."""

    location: str  # "overall" or "<slice_type>=<key>"
    metric: str
    baseline: float | None
    candidate: float | None
    delta: float | None


def collect_regressions(
    overall: list[MetricDelta], slices: dict[str, dict[str, list[MetricDelta]]]
) -> list[Regression]:
    """Every regressed metric across overall + slices, overall listed first."""
    regs: list[Regression] = []
    for d in overall:
        if d.regressed:
            regs.append(
                Regression("overall", d.metric, d.baseline, d.candidate, d.delta)
            )
    for slice_type, groups in slices.items():
        for key, deltas in groups.items():
            for d in deltas:
                if d.regressed:
                    regs.append(
                        Regression(
                            f"{slice_type}={key}",
                            d.metric,
                            d.baseline,
                            d.candidate,
                            d.delta,
                        )
                    )
    return regs


@dataclass
class ComparisonReport:
    baseline: dict
    candidate: dict
    n: int
    overall: list[MetricDelta]
    slices: dict[str, dict[str, list[MetricDelta]]]
    regressions: list[Regression]
    baseline_report: ScoreReport
    candidate_report: ScoreReport


def _spec_summary(spec: PromptSpec) -> dict:
    return {"model": spec.model, "prompt_hash": spec.prompt_hash()}


def build_comparison(
    base_spec: PromptSpec,
    cand_spec: PromptSpec,
    base_report: ScoreReport,
    cand_report: ScoreReport,
) -> ComparisonReport:
    """Assemble the full A/B comparison from two scored runs."""
    overall = diff_aggregate(base_report.overall, cand_report.overall)
    slices = diff_slices(base_report, cand_report)
    return ComparisonReport(
        baseline=_spec_summary(base_spec),
        candidate=_spec_summary(cand_spec),
        n=cand_report.n,
        overall=overall,
        slices=slices,
        regressions=collect_regressions(overall, slices),
        baseline_report=base_report,
        candidate_report=cand_report,
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _fmt_value(metric: str, x: float | None) -> str:
    if x is None:
        return "—"
    if metric.startswith("judge:"):
        return f"{x:.2f}"
    return f"{x:.0%}"


def _fmt_signed(metric: str, delta: float | None) -> str:
    """Signed magnitude of a delta — percentage points for rates, raw for judge."""
    if delta is None:
        return "—"
    if delta == 0:
        return "±0"
    sign = "+" if delta > 0 else "−"
    mag = abs(delta)
    body = f"{mag:.2f}" if metric.startswith("judge:") else f"{mag * 100:.0f} pts"
    return f"{sign}{body}"


def _fmt_delta(d: MetricDelta) -> str:
    """A delta cell with a trailing ⚠️ (regression) / ✅ (improvement) marker."""
    body = _fmt_signed(d.metric, d.delta)
    arrow = " ⚠️" if d.regressed else (" ✅" if d.improved else "")
    return f"{body}{arrow}"


_DELTA_HEADER = "| metric | baseline | candidate | Δ (cand − base) |\n|---|---|---|---|"


def _delta_table(deltas: list[MetricDelta]) -> list[str]:
    rows = [_DELTA_HEADER]
    for d in deltas:
        rows.append(
            f"| {d.metric} | {_fmt_value(d.metric, d.baseline)} | "
            f"{_fmt_value(d.metric, d.candidate)} | {_fmt_delta(d)} |"
        )
    return rows


def render_markdown(report: ComparisonReport) -> str:
    out: list[str] = ["# E2E reviewer eval — A/B comparison", ""]
    out.append(
        f"- **baseline**: model `{report.baseline['model']}`, "
        f"prompt `{report.baseline['prompt_hash']}`"
    )
    out.append(
        f"- **candidate**: model `{report.candidate['model']}`, "
        f"prompt `{report.candidate['prompt_hash']}`"
    )
    same_prompt = report.baseline["prompt_hash"] == report.candidate["prompt_hash"]
    same_model = report.baseline["model"] == report.candidate["model"]
    if same_prompt and same_model:
        out.append("")
        out.append("> ⚠️ baseline and candidate are identical (same prompt and model).")
    elif same_prompt:
        out.append("")
        out.append("> Note: prompts are identical; only the model differs.")
    out.append("")
    out.append(f"Compared **{report.n}** case(s).")
    out.append("")

    out.append("## Regressions")
    out.append("")
    if not report.regressions:
        out.append("None. 🎉")
    else:
        out.append("| where | metric | baseline | candidate | Δ |")
        out.append("|---|---|---|---|---|")
        for r in report.regressions:
            out.append(
                f"| {r.location} | {r.metric} | {_fmt_value(r.metric, r.baseline)} | "
                f"{_fmt_value(r.metric, r.candidate)} | "
                f"{_fmt_signed(r.metric, r.delta)} ⚠️ |"
            )
    out.append("")

    out.append("## Overall")
    out.append("")
    out.extend(_delta_table(report.overall))
    out.append("")

    for slice_type, groups in report.slices.items():
        out.append(f"## By {slice_type}")
        out.append("")
        for key, deltas in groups.items():
            out.append(f"### {key}")
            out.append("")
            out.extend(_delta_table(deltas))
            out.append("")
    return "\n".join(out)


def _report_to_dict(report: ComparisonReport) -> dict:
    return asdict(report)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _run_side(
    label: str,
    spec: PromptSpec,
    *,
    cases: list,
    connection: str,
    base_ref: str,
    venv_dir: Path,
    results_dir: Path,
    timeout: int,
    use_cache: bool,
) -> None:
    """Run the reviewer over *cases* under one prompt spec into ``<results>/<label>``."""
    print(f"\n=== Running {label}: model={spec.model} prompt={spec.prompt_hash()} ===")
    runner.run_eval(
        cases=cases,
        template=spec.template,
        diff_instructions=spec.diff_instructions,
        model=spec.model,
        connection=connection,
        base_ref=base_ref,
        venv_dir=venv_dir,
        results_dir=results_dir / label,
        timeout=timeout,
        use_cache=use_cache,
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser(
        description="A/B compare the E2E PR-review eval: baseline (a git ref) vs "
        "candidate (working tree)."
    )
    p.add_argument(
        "-i",
        "--case",
        action="append",
        dest="patterns",
        help="case name or glob to run (repeatable; default: all)",
    )
    p.add_argument(
        "--base-ref",
        default=runner.DEFAULT_BASE_REF,
        help="git ref that is BOTH the baseline reviewer source and the dataset base "
        "(default: origin/main)",
    )
    p.add_argument(
        "--baseline-model",
        default=None,
        help="override the baseline model (default: the base ref's CORTEX_MODEL default)",
    )
    p.add_argument(
        "--candidate-model",
        default=None,
        help="override the candidate model (default: the working tree's default)",
    )
    p.add_argument("--connection", default=runner.DEFAULT_CONNECTION)
    p.add_argument(
        "--venv-dir", default=None, help="shared venv dir (default: <results>/venv)"
    )
    p.add_argument("--results-dir", default="eval_results")
    p.add_argument("--timeout", type=int, default=runner.rev.AGENT_TIMEOUT_SEC)
    p.add_argument("--no-cache", action="store_true")
    p.add_argument(
        "--skip-run",
        action="store_true",
        help="do not run the agents; just score existing <results>/baseline and "
        "<results>/candidate and diff them",
    )
    p.add_argument(
        "--judge",
        action="store_true",
        help="run the LLM judge on both sides (requires a live Snowflake connection)",
    )
    p.add_argument("--judge-model", default=scorer.DEFAULT_JUDGE_MODEL)
    p.add_argument(
        "--json-out", default=None, help="default: <results>/comparison.json"
    )
    p.add_argument(
        "--markdown-out", default=None, help="default: <results>/comparison.md"
    )
    args = p.parse_args(argv)

    results_dir = Path(args.results_dir).resolve()
    venv_dir = Path(args.venv_dir).resolve() if args.venv_dir else results_dir / "venv"

    base_spec = baseline_spec(args.base_ref, args.baseline_model)
    cand_spec = candidate_spec(args.candidate_model)
    print(
        f"baseline: model={base_spec.model} prompt={base_spec.prompt_hash()}\n"
        f"candidate: model={cand_spec.model} prompt={cand_spec.prompt_hash()}"
    )
    if (base_spec.prompt_hash(), base_spec.model) == (
        cand_spec.prompt_hash(),
        cand_spec.model,
    ):
        print(
            "Warning: baseline and candidate are identical (same prompt and model) — "
            "the diff will be all zeros.",
            file=sys.stderr,
        )

    cases = runner.select_cases(runner.load_all_cases(), args.patterns)
    if not cases:
        print("No cases matched.", file=sys.stderr)
        return 1

    if not args.skip_run:
        for label, spec in (("baseline", base_spec), ("candidate", cand_spec)):
            _run_side(
                label,
                spec,
                cases=cases,
                connection=args.connection,
                base_ref=args.base_ref,
                venv_dir=venv_dir,
                results_dir=results_dir,
                timeout=args.timeout,
                use_cache=not args.no_cache,
            )

    session = None
    provider = None
    if args.judge:
        session = scorer.build_snowpark_session(args.connection)
        provider = scorer.build_cortex_provider(session, args.judge_model)

    try:
        for label in ("baseline", "candidate"):
            if not (results_dir / label).exists():
                hint = " (run without --skip-run first)" if args.skip_run else ""
                print(
                    f"Results dir not found: {results_dir / label}{hint}",
                    file=sys.stderr,
                )
                return 1

        case_map = {c.name: c for c in cases}
        base_report = scorer.score_results_dir(
            results_dir / "baseline", cases=case_map, provider=provider
        )
        cand_report = scorer.score_results_dir(
            results_dir / "candidate", cases=case_map, provider=provider
        )
        if base_report.n == 0 or cand_report.n == 0:
            print(
                f"Nothing to compare (baseline n={base_report.n}, candidate "
                f"n={cand_report.n}) under {results_dir}.",
                file=sys.stderr,
            )
            return 1
        if base_report.n != cand_report.n:
            print(
                f"Warning: baseline scored {base_report.n} case(s) but candidate scored "
                f"{cand_report.n} — the diff compares different case sets, so overall "
                "deltas may be misleading (per-slice deltas remain per-key).",
                file=sys.stderr,
            )

        comparison = build_comparison(base_spec, cand_spec, base_report, cand_report)

        json_out = (
            Path(args.json_out) if args.json_out else results_dir / "comparison.json"
        )
        md_out = (
            Path(args.markdown_out)
            if args.markdown_out
            else results_dir / "comparison.md"
        )
        json_out.write_text(
            json.dumps(_report_to_dict(comparison), indent=2), encoding="utf-8"
        )
        markdown = render_markdown(comparison)
        md_out.write_text(markdown, encoding="utf-8")

        print(markdown)
        print(f"\nWrote {json_out} and {md_out}")
        # Non-zero exit when the candidate regressed on any metric — useful in CI gating.
        return 2 if comparison.regressions else 0
    finally:
        if session is not None:
            session.close()


if __name__ == "__main__":
    sys.exit(main())
