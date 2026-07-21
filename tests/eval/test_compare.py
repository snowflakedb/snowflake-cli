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

"""Tests for the eval A/B compare. Offline — git/runner/judge are mocked."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock

import pytest

_EVAL_DIR = Path(__file__).parents[2] / ".github" / "scripts" / "eval"
sys.path.insert(0, str(_EVAL_DIR))

import compare  # noqa: E402
import scorer  # noqa: E402
from cases import Case  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# A minimal reviewer-script source carrying the constants compare extracts. The
# diff-instructions use adjacent-string concatenation to mirror the real file (the
# parser folds it into one Constant, so literal_eval must still resolve it).
_WORKFLOW_SOURCE = """\
jobs:
  cortex-review:
    steps:
      - name: Run Cortex E2E review
        env:
          CORTEX_MODEL: claude-opus-4-8
        run: python .github/scripts/cortex_e2e_review.py
"""

_REVIEWER_SOURCE = '''\
"""doc"""
import os

PR_DIFF_INSTRUCTIONS = "review PR #{pr_number}"

LOCAL_DIFF_INSTRUCTIONS = (
    "line one\\n"
    "line two\\n"
)

AGENT_PROMPT_TEMPLATE = """\\
You are a reviewer. Playground: {playground_db}
{diff_instructions}
"""


def main():
    model = os.environ.get("CORTEX_MODEL", "claude-opus-4-6")
    print(model)
'''


def _agg(**overrides) -> scorer.Aggregate:
    """An Aggregate with sane defaults; override individual metrics per test."""
    defaults = dict(
        n=4,
        verdict_accuracy=1.0,
        false_positive_rate=0.0,
        false_negative_rate=0.0,
        skip_precision=1.0,
        skip_recall=1.0,
        marker_rate=1.0,
        section_completeness=1.0,
        breaking_changes_accuracy=1.0,
        mean_command_coverage=1.0,
        mean_judge={},
    )
    defaults.update(overrides)
    return scorer.Aggregate(**defaults)


def _report(overall: scorer.Aggregate, slices=None, n=4) -> scorer.ScoreReport:
    return scorer.ScoreReport(n=n, overall=overall, slices=slices or {}, cases=[])


# ---------------------------------------------------------------------------
# Prompt spec extraction
# ---------------------------------------------------------------------------


def test_extract_prompt_spec_reads_constants_and_model_default():
    spec = compare.extract_prompt_spec(_REVIEWER_SOURCE)
    assert "You are a reviewer" in spec.template
    assert spec.diff_instructions == "line one\nline two\n"  # concat folded + resolved
    assert spec.model == "claude-opus-4-6"  # from CORTEX_MODEL default


def test_extract_prompt_spec_model_override_wins():
    spec = compare.extract_prompt_spec(
        _REVIEWER_SOURCE, model_override="claude-opus-4-8"
    )
    assert spec.model == "claude-opus-4-8"


def test_extract_prompt_spec_falls_back_when_no_model_default():
    source = _REVIEWER_SOURCE.replace(
        'os.environ.get("CORTEX_MODEL", "claude-opus-4-6")', "None"
    )
    spec = compare.extract_prompt_spec(source)
    assert spec.model == compare.runner.DEFAULT_MODEL


def test_extract_prompt_spec_uses_workflow_model_over_script_default():
    # The workflow YAML's CORTEX_MODEL is the authoritative production value and
    # must win over the script's os.environ.get fallback.
    spec = compare.extract_prompt_spec(
        _REVIEWER_SOURCE, workflow_source=_WORKFLOW_SOURCE
    )
    assert (
        spec.model == "claude-opus-4-8"
    )  # workflow, not "claude-opus-4-6" from script


def test_extract_prompt_spec_missing_constant_raises():
    source = _REVIEWER_SOURCE.replace("AGENT_PROMPT_TEMPLATE", "SOMETHING_ELSE")
    with pytest.raises(ValueError, match="AGENT_PROMPT_TEMPLATE"):
        compare.extract_prompt_spec(source)


def test_prompt_hash_ignores_model_and_tracks_prompt():
    a = compare.PromptSpec("tmpl", "diff", "model-1")
    b = compare.PromptSpec("tmpl", "diff", "model-2")  # same prompt, diff model
    c = compare.PromptSpec("tmpl", "DIFFERENT", "model-1")
    assert a.prompt_hash() == b.prompt_hash()
    assert a.prompt_hash() != c.prompt_hash()


# ---------------------------------------------------------------------------
# baseline_spec via git show (mocked) + candidate_spec from the real file
# ---------------------------------------------------------------------------


def test_baseline_spec_extracts_from_git_show():
    def _fake_git_show(ref_arg):
        return (
            _WORKFLOW_SOURCE
            if compare.WORKFLOW_REL_PATH in ref_arg
            else _REVIEWER_SOURCE
        )

    with mock.patch.object(compare, "_git_show", side_effect=_fake_git_show) as g:
        spec = compare.baseline_spec("origin/main")

    called_refs = [args[0] for (args, _) in g.call_args_list]
    assert f"origin/main:{compare.REVIEWER_REL_PATH}" in called_refs
    assert f"origin/main:{compare.WORKFLOW_REL_PATH}" in called_refs
    # Model comes from the workflow YAML, not the script default.
    assert spec.model == "claude-opus-4-8"
    assert "You are a reviewer" in spec.template


def test_candidate_spec_reads_working_tree_file():
    # The real working-tree reviewer script parses cleanly through the same path.
    spec = compare.candidate_spec()
    assert spec.template and spec.diff_instructions
    assert spec.model  # resolved (default or fallback)


# ---------------------------------------------------------------------------
# diff_aggregate — deltas + regression/improvement direction
# ---------------------------------------------------------------------------


def _by_metric(deltas):
    return {d.metric: d for d in deltas}


def test_diff_aggregate_higher_is_better_regression_and_improvement():
    base = _agg(verdict_accuracy=0.8, skip_recall=0.5)
    cand = _agg(verdict_accuracy=0.6, skip_recall=1.0)
    d = _by_metric(compare.diff_aggregate(base, cand))
    # verdict_accuracy dropped → regression
    assert round(d["verdict_accuracy"].delta, 2) == -0.2
    assert d["verdict_accuracy"].regressed is True
    assert d["verdict_accuracy"].improved is False
    # skip_recall rose → improvement
    assert d["skip_recall"].improved is True
    assert d["skip_recall"].regressed is False


def test_diff_aggregate_lower_is_better_error_rates():
    base = _agg(false_positive_rate=0.0, false_negative_rate=0.5)
    cand = _agg(false_positive_rate=0.25, false_negative_rate=0.0)
    d = _by_metric(compare.diff_aggregate(base, cand))
    # FP rate went UP → that is a regression for a lower-is-better metric.
    assert d["false_positive_rate"].regressed is True
    assert d["false_positive_rate"].improved is False
    # FN rate went DOWN → improvement.
    assert d["false_negative_rate"].improved is True
    assert d["false_negative_rate"].regressed is False


def test_diff_aggregate_none_metric_has_no_delta():
    base = _agg(skip_precision=None)
    cand = _agg(skip_precision=1.0)
    d = _by_metric(compare.diff_aggregate(base, cand))
    assert d["skip_precision"].delta is None
    assert d["skip_precision"].regressed is False
    assert d["skip_precision"].improved is False


def test_diff_aggregate_no_change_is_neither():
    d = _by_metric(compare.diff_aggregate(_agg(), _agg()))
    assert d["verdict_accuracy"].delta == 0.0
    assert d["verdict_accuracy"].regressed is False
    assert d["verdict_accuracy"].improved is False


def test_diff_aggregate_includes_judge_metrics():
    base = _agg(mean_judge={"coverage": 0.9, "no_overclaim": 0.5})
    cand = _agg(mean_judge={"coverage": 0.7})  # no_overclaim missing on candidate
    d = _by_metric(compare.diff_aggregate(base, cand))
    assert d["judge:coverage"].regressed is True
    assert round(d["judge:coverage"].delta, 2) == -0.2
    # A judge metric present on only one side is undefined → no delta.
    assert d["judge:no_overclaim"].delta is None


# ---------------------------------------------------------------------------
# diff_slices + collect_regressions
# ---------------------------------------------------------------------------


def test_diff_slices_only_diffs_shared_keys():
    base = _report(
        _agg(),
        slices={
            "expected_verdict": {"FAIL": _agg(verdict_accuracy=1.0), "PASS": _agg()}
        },
    )
    cand = _report(
        _agg(),
        slices={
            "expected_verdict": {"FAIL": _agg(verdict_accuracy=0.5), "SKIP": _agg()}
        },
    )
    slices = compare.diff_slices(base, cand)
    # FAIL is on both sides → diffed; PASS/SKIP each on one side → dropped.
    assert set(slices["expected_verdict"]) == {"FAIL"}
    fail = _by_metric(slices["expected_verdict"]["FAIL"])
    assert fail["verdict_accuracy"].regressed is True


def test_collect_regressions_spans_overall_and_slices():
    overall = compare.diff_aggregate(_agg(), _agg(verdict_accuracy=0.5))
    slices = {
        "difficulty": {
            "hard": compare.diff_aggregate(_agg(), _agg(false_positive_rate=0.5))
        }
    }
    regs = compare.collect_regressions(overall, slices)
    locations = {(r.location, r.metric) for r in regs}
    assert ("overall", "verdict_accuracy") in locations
    assert ("difficulty=hard", "false_positive_rate") in locations
    # Overall regressions are listed before slice ones.
    assert regs[0].location == "overall"


# ---------------------------------------------------------------------------
# build_comparison + render
# ---------------------------------------------------------------------------


def _spec(model="m", prompt=("tmpl", "diff")) -> compare.PromptSpec:
    return compare.PromptSpec(prompt[0], prompt[1], model)


def test_build_comparison_assembles_everything():
    base_spec = _spec("claude-opus-4-6")
    cand_spec = _spec("claude-opus-4-8", prompt=("tmpl2", "diff"))
    base = _report(_agg(verdict_accuracy=1.0), n=4)
    cand = _report(_agg(verdict_accuracy=0.75), n=4)
    comp = compare.build_comparison(base_spec, cand_spec, base, cand)
    assert comp.baseline["model"] == "claude-opus-4-6"
    assert comp.candidate["model"] == "claude-opus-4-8"
    assert comp.n == 4
    assert any(r.metric == "verdict_accuracy" for r in comp.regressions)


def test_render_markdown_flags_regression():
    base_spec = _spec("claude-opus-4-6")
    cand_spec = _spec("claude-opus-4-8", prompt=("tmpl2", "diff"))
    base = _report(_agg(verdict_accuracy=1.0))
    cand = _report(_agg(verdict_accuracy=0.5))
    md = compare.render_markdown(
        compare.build_comparison(base_spec, cand_spec, base, cand)
    )
    assert "# E2E reviewer eval — A/B comparison" in md
    assert "## Regressions" in md
    assert "verdict_accuracy" in md
    assert "⚠️" in md  # the regression is flagged
    assert "claude-opus-4-6" in md and "claude-opus-4-8" in md


def test_render_markdown_no_regressions_and_identical_warning():
    spec = _spec("m", prompt=("same", "diff"))
    rep = _report(_agg())
    md = compare.render_markdown(compare.build_comparison(spec, spec, rep, rep))
    assert "None. 🎉" in md
    assert "identical" in md  # same prompt + model warning


def test_comparison_is_json_serializable():
    comp = compare.build_comparison(
        _spec(), _spec("m2"), _report(_agg()), _report(_agg())
    )
    text = json.dumps(compare._report_to_dict(comp))  # noqa: SLF001
    assert '"verdict_accuracy"' in text


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _case(name="c1", expected_verdict="FAIL") -> Case:
    return Case(
        name=name,
        path=Path("/tmp") / name,
        description="d",
        difficulty="hard",
        tags=["e2e-reviewer"],
        expected_verdict=expected_verdict,
        expected_breaking_changes=True,
        needs_live_snowflake=False,
        expected_commands=[],
        rubric="r",
        targets=["src/x.py"],
        mutate=lambda _repo: None,
    )


def _full_report(verdict: str) -> str:
    sections = {
        "Summary": "s",
        "E2E Test Results": "ran",
        "Breaking Changes": "`--query` removed — breaking.",
        "Side-Effect Verification": "none",
        "Potential Risks": "none",
        "Verdict": f"{verdict} — because.",
    }
    parts = ["<!-- E2E_REPORT -->"]
    parts += [f"### {k}\n{v}" for k, v in sections.items()]
    return "\n\n".join(parts)


def _write_side(parent: Path, case_name: str, verdict: str):
    case_dir = parent / case_name
    case_dir.mkdir(parents=True)
    (case_dir / "report.md").write_text(_full_report(verdict))
    (case_dir / "stdout.jsonl").write_text('{"type":"result"}')
    (case_dir / "result.json").write_text(
        json.dumps({"case": case_name, "verdict": verdict, "executed_commands": []})
    )


def test_main_skip_run_diffs_persisted_dirs_and_flags_regression(tmp_path):
    case = _case(name="sql-rename-query-flag", expected_verdict="FAIL")
    # Baseline gets it right (FAIL); candidate gets it wrong (PASS) → verdict acc drops.
    _write_side(tmp_path / "baseline", case.name, "FAIL")
    _write_side(tmp_path / "candidate", case.name, "PASS")

    with mock.patch.object(
        compare, "baseline_spec", return_value=_spec("base-model")
    ), mock.patch.object(
        compare, "candidate_spec", return_value=_spec("cand-model", ("t2", "d"))
    ), mock.patch.object(
        compare.runner, "load_all_cases", return_value=[case]
    ):
        rc = compare.main(["--results-dir", str(tmp_path), "--skip-run"])

    assert rc == 2  # regression → non-zero
    assert (tmp_path / "comparison.json").exists()
    md = (tmp_path / "comparison.md").read_text(encoding="utf-8")
    assert "verdict_accuracy" in md and "⚠️" in md


def test_main_skip_run_no_regression_returns_zero(tmp_path):
    case = _case(name="sql-rename-query-flag", expected_verdict="FAIL")
    _write_side(tmp_path / "baseline", case.name, "FAIL")
    _write_side(tmp_path / "candidate", case.name, "FAIL")  # both correct

    with mock.patch.object(
        compare, "baseline_spec", return_value=_spec("base-model")
    ), mock.patch.object(
        compare, "candidate_spec", return_value=_spec("cand-model", ("t2", "d"))
    ), mock.patch.object(
        compare.runner, "load_all_cases", return_value=[case]
    ):
        rc = compare.main(["--results-dir", str(tmp_path), "--skip-run"])

    assert rc == 0


def test_main_runs_both_sides_when_not_skipping(tmp_path):
    case = _case(name="sql-rename-query-flag", expected_verdict="FAIL")

    def _fake_run_eval(**kwargs):
        # Emulate the runner writing artifacts into the label's results dir.
        _write_side(kwargs["results_dir"], case.name, "FAIL")
        return []

    with mock.patch.object(
        compare, "baseline_spec", return_value=_spec("base-model")
    ), mock.patch.object(
        compare, "candidate_spec", return_value=_spec("cand-model", ("t2", "d"))
    ), mock.patch.object(
        compare.runner, "load_all_cases", return_value=[case]
    ), mock.patch.object(
        compare.runner, "run_eval", side_effect=_fake_run_eval
    ) as run_eval:
        rc = compare.main(["--results-dir", str(tmp_path)])

    assert rc == 0
    assert run_eval.call_count == 2  # baseline + candidate
    labels = {Path(c.kwargs["results_dir"]).name for c in run_eval.call_args_list}
    assert labels == {"baseline", "candidate"}


def test_main_no_cases_matched_errors(tmp_path):
    with mock.patch.object(
        compare, "baseline_spec", return_value=_spec()
    ), mock.patch.object(
        compare, "candidate_spec", return_value=_spec("m2")
    ), mock.patch.object(
        compare.runner, "load_all_cases", return_value=[]
    ):
        rc = compare.main(["--results-dir", str(tmp_path), "--skip-run"])
    assert rc == 1


def test_main_skip_run_missing_side_dir_errors(tmp_path, capsys):
    # Only the baseline dir exists; --skip-run must fail cleanly, not traceback.
    case = _case(name="sql-rename-query-flag")
    _write_side(tmp_path / "baseline", case.name, "FAIL")
    with mock.patch.object(
        compare, "baseline_spec", return_value=_spec("b")
    ), mock.patch.object(
        compare, "candidate_spec", return_value=_spec("c", ("t2", "d"))
    ), mock.patch.object(
        compare.runner, "load_all_cases", return_value=[case]
    ):
        rc = compare.main(["--results-dir", str(tmp_path), "--skip-run"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "candidate" in err and "not found" in err


def test_main_closes_judge_session(tmp_path):
    case = _case(name="sql-rename-query-flag", expected_verdict="FAIL")
    _write_side(tmp_path / "baseline", case.name, "FAIL")
    _write_side(tmp_path / "candidate", case.name, "FAIL")
    mock_session = mock.MagicMock()
    with (
        mock.patch.object(compare, "baseline_spec", return_value=_spec("b")),
        mock.patch.object(
            compare, "candidate_spec", return_value=_spec("c", ("t2", "d"))
        ),
        mock.patch.object(compare.runner, "load_all_cases", return_value=[case]),
        mock.patch.object(scorer, "build_snowpark_session", return_value=mock_session),
        mock.patch.object(scorer, "build_cortex_provider", return_value=None),
    ):
        rc = compare.main(["--results-dir", str(tmp_path), "--skip-run", "--judge"])
    assert rc == 0
    mock_session.close.assert_called_once()


def test_main_warns_on_mismatched_case_counts(tmp_path, capsys):
    # Baseline scored 1 case, candidate scored 2 → deltas span different case sets.
    c1 = _case(name="case-1")
    c2 = _case(name="case-2")
    _write_side(tmp_path / "baseline", c1.name, "FAIL")
    _write_side(tmp_path / "candidate", c1.name, "FAIL")
    _write_side(tmp_path / "candidate", c2.name, "FAIL")
    with mock.patch.object(
        compare, "baseline_spec", return_value=_spec("b")
    ), mock.patch.object(
        compare, "candidate_spec", return_value=_spec("c", ("t2", "d"))
    ), mock.patch.object(
        compare.runner, "load_all_cases", return_value=[c1, c2]
    ):
        rc = compare.main(["--results-dir", str(tmp_path), "--skip-run"])
    assert rc == 0  # both sides all-correct → no regression
    err = capsys.readouterr().err
    assert "baseline scored 1" in err and "candidate scored 2" in err
