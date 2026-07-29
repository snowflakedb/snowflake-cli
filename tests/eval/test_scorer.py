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

"""Tests for the eval scorer. Fully offline — the LLM judge provider is mocked."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock

_EVAL_DIR = Path(__file__).parents[2] / ".github" / "scripts" / "eval"
sys.path.insert(0, str(_EVAL_DIR))

import scorer  # noqa: E402
from cases import Case  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------------


def _report(
    *,
    marker: bool = True,
    summary: str = "The PR renames a flag.",
    e2e: str = "Ran `snow sql --help`; exit 0.",
    breaking: str = "No breaking changes detected.",
    side_effects: str = "None needed.",
    risks: str = "None.",
    verdict: str = "PASS — verified.",
    drop_sections: tuple[str, ...] = (),
) -> str:
    blocks = {
        "Summary": summary,
        "E2E Test Results": e2e,
        "Breaking Changes": breaking,
        "Side-Effect Verification": side_effects,
        "Potential Risks": risks,
        "Verdict": verdict,
    }
    parts = ["<!-- E2E_REPORT -->"] if marker else []
    for name, body in blocks.items():
        if name in drop_sections:
            continue
        parts.append(f"### {name}\n{body}")
    return "\n\n".join(parts)


def _case(
    name: str = "fake-case",
    *,
    expected_verdict: str = "PASS",
    expected_breaking_changes: bool = False,
    expected_commands: list[str] | None = None,
    difficulty: str = "hard",
    tags: list[str] | None = None,
    rubric: str = "Confirm the flag works.",
) -> Case:
    return Case(
        name=name,
        path=Path("/tmp") / name,
        description="d",
        difficulty=difficulty,
        tags=tags if tags is not None else ["e2e-reviewer", "sql"],
        expected_verdict=expected_verdict,
        expected_breaking_changes=expected_breaking_changes,
        needs_live_snowflake=False,
        expected_commands=expected_commands if expected_commands is not None else [],
        rubric=rubric,
        targets=["src/x.py"],
        mutate=lambda _repo: None,
    )


# ---------------------------------------------------------------------------
# Section parsing
# ---------------------------------------------------------------------------


def test_split_sections_and_normalization():
    sections = scorer.split_sections(_report())
    # Every required section appears (compared under heading normalization).
    norm = {scorer._normalize_heading(k) for k in sections}  # noqa: SLF001
    for required in scorer.REQUIRED_SECTIONS:
        assert scorer._normalize_heading(required) in norm  # noqa: SLF001
    # Bodies are captured, not just headings.
    assert "renames a flag" in sections["Summary"]


def test_check_sections_all_present():
    checks = scorer.check_sections(_report())
    assert all(checks.values())
    assert set(checks) == set(scorer.REQUIRED_SECTIONS)


def test_check_sections_missing_one():
    checks = scorer.check_sections(_report(drop_sections=("Potential Risks",)))
    assert checks["Potential Risks"] is False
    assert checks["Summary"] is True


def test_check_sections_tolerates_heading_punctuation_variants():
    # "Side Effect Verification" (no hyphen) still matches the required section.
    report = _report().replace("Side-Effect Verification", "Side Effect Verification")
    assert scorer.check_sections(report)["Side-Effect Verification"] is True


# ---------------------------------------------------------------------------
# Breaking-changes detection
# ---------------------------------------------------------------------------


def test_reports_breaking_change_negative_phrasings():
    for phrasing in (
        "No breaking changes detected.",
        "None.",
        "N/A",
        "no breaking changes were found",
    ):
        assert scorer.reports_breaking_change(_report(breaking=phrasing)) is False


def test_reports_breaking_change_positive():
    report = _report(breaking="The `--query` option was removed, breaking scripts.")
    assert scorer.reports_breaking_change(report) is True


def test_reports_breaking_change_missing_section_is_false():
    report = _report(drop_sections=("Breaking Changes",))
    assert scorer.reports_breaking_change(report) is False


# ---------------------------------------------------------------------------
# Command coverage
# ---------------------------------------------------------------------------


def test_command_coverage_empty_expected_is_full():
    cov = scorer.command_coverage([], [])
    assert cov.coverage == 1.0
    assert cov.covered == [] and cov.missing == []


def test_command_coverage_trajectory_hit():
    cov = scorer.command_coverage(
        ["snow sql --help"],
        ["snow sql --help --format json", "cat file"],
    )
    assert cov.coverage == 1.0
    assert cov.covered == ["snow sql --help"]


def test_command_coverage_ignores_report_text():
    # A command only *mentioned* in the report but never executed is NOT covered:
    # coverage is grounded purely in the executed-command trajectory.
    cov = scorer.command_coverage(
        ["snow sql --help"],
        executed=["ls -la"],
    )
    assert cov.coverage == 0.0
    assert cov.missing == ["snow sql --help"]


def test_command_coverage_partial_and_missing():
    cov = scorer.command_coverage(
        ["snow sql --help", "snow connection list"],
        executed=["snow sql --help"],
    )
    assert cov.coverage == 0.5
    assert cov.missing == ["snow connection list"]


# ---------------------------------------------------------------------------
# score_case — deterministic path
# ---------------------------------------------------------------------------


def test_score_case_correct_fail():
    case = _case(
        expected_verdict="FAIL",
        expected_breaking_changes=True,
        expected_commands=["snow sql --help"],
    )
    report = _report(
        breaking="`--query` removed — breaking.",
        verdict="FAIL — the flag was removed with no deprecation path.",
    )
    score = scorer.score_case(
        case,
        report,
        executed_commands=["snow sql --help"],
        raw_stdout='{"type":"result"}',
    )
    assert score.verdict == "FAIL"
    assert score.verdict_correct is True
    assert score.all_sections_present is True
    assert score.reported_breaking_changes is True
    assert score.breaking_changes_correct is True
    assert score.command_coverage == 1.0
    assert score.judge is None  # no provider → no judge


def test_score_case_marker_from_raw_stdout_when_stripped_from_report():
    # Real report.md has the marker stripped by _parse_stream_json; it survives
    # only in the raw stream, which is where the check must look.
    case = _case()
    report = _report(marker=False)
    assert scorer.REPORT_MARKER not in report
    score = scorer.score_case(
        case, report, [], raw_stdout=f'{{"result":"{scorer.REPORT_MARKER} ..."}}'
    )
    assert score.marker_present is True


def test_score_case_marker_absent_everywhere():
    score = scorer.score_case(_case(), _report(marker=False), [], raw_stdout="{}")
    assert score.marker_present is False


def test_score_case_wrong_verdict_and_breaking_mismatch():
    # Expected a benign PASS with no breaking change; reviewer wrongly FAILs and
    # claims a breaking change → both correctness flags flip false.
    case = _case(expected_verdict="PASS", expected_breaking_changes=False)
    report = _report(
        breaking="Removed a command — breaking!",
        verdict="FAIL — I think this breaks things.",
    )
    score = scorer.score_case(case, report, [])
    assert score.verdict_correct is False
    assert score.breaking_changes_correct is False


def test_score_case_verdict_none_for_malformed_report():
    # A report with no parseable "### Verdict" line yields verdict=None and
    # verdict_correct=False (None != any expected string).
    case = _case(expected_verdict="PASS")
    malformed = "Some text with no verdict section."
    score = scorer.score_case(case, malformed, [])
    assert score.verdict is None
    assert score.verdict_correct is False


# ---------------------------------------------------------------------------
# score_case — judge path (mocked provider)
# ---------------------------------------------------------------------------


class _FakeProvider:
    """Stand-in for trulens.providers.cortex.Cortex."""

    def __init__(self, score=0.8, reason="looks good", raises=False):
        self._score = score
        self._reason = reason
        self._raises = raises
        self.calls: list[tuple[str, str]] = []

    def generate_score_and_reasons(self, system_prompt, user_prompt):
        self.calls.append((system_prompt, user_prompt))
        if self._raises:
            raise RuntimeError("cortex unavailable")
        return self._score, {"reason": self._reason}


def test_judge_report_populates_scores_and_injects_rubric():
    provider = _FakeProvider(score=0.9, reason="thorough")
    case = _case(rubric="RUBRIC-SENTINEL")
    result = scorer.judge_report(
        provider, _report(), case, executed_commands=["snow sql --help"]
    )
    assert set(result) == {"coverage", "no_overclaim"}
    assert result["coverage"].score == 0.9
    assert result["coverage"].reason == "thorough"
    # The rubric is injected into the coverage criterion's system prompt.
    coverage_prompt = next(sp for sp, _ in provider.calls if "RUBRIC-SENTINEL" in sp)
    assert "RUBRIC-SENTINEL" in coverage_prompt
    # The user prompt carries BOTH the report and the executed-command trajectory.
    for _sys, user in provider.calls:
        assert "## Reviewer report" in user
        assert "renames a flag" in user  # report body
        assert "snow sql --help" in user  # trajectory
        assert "Executed-command trajectory" in user


def test_judge_report_trajectory_absent_is_labeled():
    provider = _FakeProvider()
    scorer.judge_report(provider, _report(), _case(), executed_commands=[])
    assert all("(no commands captured)" in user for _sys, user in provider.calls)


def test_judge_report_captures_provider_errors():
    provider = _FakeProvider(raises=True)
    result = scorer.judge_report(provider, _report(), _case(), executed_commands=[])
    assert result["coverage"].score is None
    assert "RuntimeError" in result["coverage"].error


def test_invoke_provider_falls_back_to_bare_generate_score():
    class BareProvider:
        def generate_score(self, system_prompt, user_prompt):
            return 0.5

    score, reason = scorer._invoke_provider(  # noqa: SLF001
        BareProvider(), "sys", "usr"
    )
    assert score == 0.5 and reason == ""


def test_score_case_with_provider_attaches_judge():
    score = scorer.score_case(_case(), _report(), [], provider=_FakeProvider(0.7))
    assert score.judge is not None
    assert score.judge["coverage"].score == 0.7


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------


def _mk_score(expected, verdict, **overrides):
    """Minimal CaseScore for aggregate math (only verdict fields matter)."""
    defaults = dict(
        case="c",
        expected_verdict=expected,
        verdict=verdict,
        verdict_correct=verdict == expected,
        difficulty="hard",
        tags=["t"],
        marker_present=True,
        sections_present={s: True for s in scorer.REQUIRED_SECTIONS},
        all_sections_present=True,
        reported_breaking_changes=False,
        expected_breaking_changes=False,
        breaking_changes_correct=True,
        command_coverage=1.0,
        commands_covered=[],
        commands_missing=[],
    )
    defaults.update(overrides)
    return scorer.CaseScore(**defaults)


def test_aggregate_verdict_accuracy_and_error_rates():
    scores = [
        _mk_score("FAIL", "FAIL"),  # true positive
        _mk_score("FAIL", "PASS"),  # false negative (missed a real problem)
        _mk_score("PASS", "FAIL"),  # false positive (false alarm)
        _mk_score("PASS", "PASS"),  # true negative
        _mk_score("SKIP", "SKIP"),  # correct skip
    ]
    agg = scorer.aggregate(scores)
    assert agg.n == 5
    assert agg.verdict_accuracy == 3 / 5
    # FN: of the 1 expected-FAIL that is non-fail-verdict... 1 of 2 expected FAIL.
    assert agg.false_negative_rate == 1 / 2
    # FP: of 3 non-FAIL-expected (PASS,PASS,SKIP), 1 got FAIL.
    assert agg.false_positive_rate == 1 / 3


def test_aggregate_skip_precision_recall():
    scores = [
        _mk_score("SKIP", "SKIP"),  # correct
        _mk_score("PASS", "SKIP"),  # said SKIP but wasn't → hurts precision
        _mk_score("SKIP", "FAIL"),  # was SKIP but missed → hurts recall
    ]
    agg = scorer.aggregate(scores)
    assert agg.skip_precision == 1 / 2  # 2 said SKIP, 1 correct
    assert agg.skip_recall == 1 / 2  # 2 expected SKIP, 1 caught


def test_aggregate_empty_slices_are_none():
    agg = scorer.aggregate([_mk_score("PASS", "PASS")])
    # No expected-FAIL cases → FN rate undefined; no said-SKIP → precision undefined.
    assert agg.false_negative_rate is None
    assert agg.skip_precision is None
    assert agg.false_positive_rate == 0.0  # 1 non-FAIL, 0 wrong


def test_aggregate_mean_judge():
    s1 = _mk_score("PASS", "PASS")
    s2 = _mk_score("PASS", "PASS")
    s1.judge = {
        "coverage": scorer.JudgeScore(0.8),
        "no_overclaim": scorer.JudgeScore(1.0),
    }
    s2.judge = {
        "coverage": scorer.JudgeScore(0.6),
        "no_overclaim": scorer.JudgeScore(None, error="x"),
    }
    agg = scorer.aggregate([s1, s2])
    assert agg.mean_judge["coverage"] == 0.7
    assert agg.mean_judge["no_overclaim"] == 1.0  # None/error skipped


# ---------------------------------------------------------------------------
# Slices + report building
# ---------------------------------------------------------------------------


def test_build_report_slices():
    scores = [
        _mk_score("FAIL", "FAIL", difficulty="easy", tags=["sql", "sanity"]),
        _mk_score("PASS", "PASS", difficulty="hard", tags=["sql"]),
    ]
    report = scorer.build_report(scores)
    assert report.n == 2
    assert set(report.slices) == {"expected_verdict"}
    assert set(report.slices["expected_verdict"]) == {"FAIL", "PASS"}


# ---------------------------------------------------------------------------
# score_results_dir — persisted artifacts round-trip
# ---------------------------------------------------------------------------


def _write_case_artifacts(results_dir: Path, case_name: str, report: str, result: dict):
    case_dir = results_dir / case_name
    case_dir.mkdir(parents=True)
    (case_dir / "report.md").write_text(report)
    (case_dir / "stdout.jsonl").write_text('{"type":"result"}')
    (case_dir / "result.json").write_text(json.dumps(result))


def test_score_results_dir_reads_persisted_runs(tmp_path):
    case = _case(
        name="sql-rename-query-flag",
        expected_verdict="FAIL",
        expected_breaking_changes=True,
        expected_commands=["snow sql --help"],
    )
    report = _report(
        breaking="`--query` removed.", verdict="FAIL — breaking flag removal."
    )
    _write_case_artifacts(
        tmp_path,
        "sql-rename-query-flag",
        report,
        {
            "case": "sql-rename-query-flag",
            "verdict": "FAIL",
            "exit_code": 0,
            "executed_commands": ["snow sql --help"],
            "from_cache": False,
        },
    )
    result = scorer.score_results_dir(tmp_path, cases={case.name: case})
    assert result.n == 1
    s = result.cases[0]
    assert s.verdict_correct is True
    assert s.command_coverage == 1.0
    assert s.exit_code == 0


def test_score_results_dir_skips_unknown_and_missing(tmp_path, capsys):
    # A dir with no result.json is ignored; a result.json with no matching case
    # definition is skipped with a warning.
    (tmp_path / "no-result").mkdir()
    _write_case_artifacts(
        tmp_path, "orphan", _report(), {"case": "orphan", "executed_commands": []}
    )
    result = scorer.score_results_dir(tmp_path, cases={})
    assert result.n == 0
    assert "no matching case definition" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def test_render_markdown_smoke():
    scores = [
        _mk_score("FAIL", "FAIL", case="a"),
        _mk_score("PASS", "SKIP", case="b"),
    ]
    scores[0].judge = {"coverage": scorer.JudgeScore(0.9)}
    md = scorer.render_markdown(scorer.build_report(scores))
    assert "# E2E reviewer eval — scores" in md
    assert "## Overall" in md
    assert "## By expected_verdict" in md
    assert "## Per case" in md
    assert "coverage=0.90" in md


def test_render_markdown_judge_scores_in_slices():
    # Judge scores must appear in the per-slice sections, not just in ## Overall.
    scores = [
        _mk_score("FAIL", "FAIL", case="a"),
        _mk_score("PASS", "PASS", case="b"),
    ]
    scores[0].judge = {
        "coverage": scorer.JudgeScore(0.9),
        "no_overclaim": scorer.JudgeScore(0.8),
    }
    md = scorer.render_markdown(scorer.build_report(scores))
    # Find the "By expected_verdict" section and confirm judge scores appear there.
    by_verdict_pos = md.index("## By expected_verdict")
    by_verdict_section = md[by_verdict_pos:]
    assert "coverage=0.90" in by_verdict_section
    assert "no_overclaim=0.80" in by_verdict_section


def test_report_is_json_serializable():
    report = scorer.build_report([_mk_score("PASS", "PASS")])
    # asdict must round-trip through json without custom encoders.
    text = json.dumps(scorer._report_to_dict(report))  # noqa: SLF001
    assert '"verdict_accuracy": 1.0' in text


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_main_writes_outputs(tmp_path):
    case = _case(name="c1", expected_verdict="PASS")
    _write_case_artifacts(
        tmp_path,
        "c1",
        _report(),
        {"case": "c1", "executed_commands": [], "exit_code": 0},
    )
    with mock.patch.object(scorer, "load_all_cases", return_value=[case]):
        rc = scorer.main(["--results-dir", str(tmp_path)])
    assert rc == 0
    assert (tmp_path / "scores.json").exists()
    assert (tmp_path / "scores.md").exists()


def test_main_empty_results_returns_error(tmp_path):
    with mock.patch.object(scorer, "load_all_cases", return_value=[]):
        rc = scorer.main(["--results-dir", str(tmp_path)])
    assert rc == 1


def test_main_closes_judge_session(tmp_path):
    # Even when --judge is used, the Snowpark session must be closed on exit.
    case = _case(name="c1", expected_verdict="PASS")
    _write_case_artifacts(
        tmp_path,
        "c1",
        _report(),
        {"case": "c1", "executed_commands": [], "exit_code": 0},
    )
    mock_session = mock.MagicMock()
    with (
        mock.patch.object(scorer, "load_all_cases", return_value=[case]),
        mock.patch.object(scorer, "build_snowpark_session", return_value=mock_session),
        mock.patch.object(
            scorer, "build_cortex_provider", return_value=_FakeProvider()
        ),
    ):
        rc = scorer.main(["--results-dir", str(tmp_path), "--judge"])
    assert rc == 0
    mock_session.close.assert_called_once()
