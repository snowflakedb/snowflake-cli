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

"""Bug-bash schema-invariant tests.

These tests pin the column-set invariant the operator hits in
[docs/BUG_BASH.md] step 7:

* the ``CLICKSTREAM_EVENTS`` datasource YAML in the doc,
* the ``events.json`` heredoc the doc tells the operator to write,
* the ``USER_CLICK_STATS_DECL`` FV's ``sources[].columns`` block in
  the same doc, and
* the matching ``events.json`` heredoc in
  [scripts/verify_bug_bash.sh],

must all declare the same column set.  Drift between any of these
four locations produces the silent "passes client validation, fails
on the server" failure mode that the bug bash exposed
(``MISSING_REQUIRED_FIELD: PAGE_URL``).

The tests below act as a regression net: any future edit to one of
the four sites that does not propagate to the others fails CI before
shipping.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Repo-root resolution
# ---------------------------------------------------------------------------
#
# This file lives at:
#   <repo_root>/snowflake-cli/tests/feature/test_bug_bash_consistency.py
#
# so the four parents up is the repo root.  Anchoring on ``__file__``
# instead of CWD means the tests work regardless of where pytest is
# invoked from (CI, IDE, ad-hoc shell).
_REPO_ROOT = Path(__file__).resolve().parents[3]
_BUG_BASH_MD = _REPO_ROOT / "docs" / "BUG_BASH.md"
_VERIFY_SCRIPT = _REPO_ROOT / "scripts" / "verify_bug_bash.sh"


# ---------------------------------------------------------------------------
# Tiny markdown-aware extractors
# ---------------------------------------------------------------------------
#
# We deliberately avoid pulling a full markdown parser — the four
# code blocks we care about all sit under stable ``###`` headings or
# right after a known prose line, and their fence shape is uniform
# (``` followed by an optional language tag).  A targeted regex per
# block keeps the test self-contained and obvious to read.

_FENCE_RE = re.compile(
    r"```(?P<lang>[a-zA-Z0-9_+-]*)\n(?P<body>.*?)```",
    re.DOTALL,
)


def _extract_first_fenced_block_after(text: str, anchor: str, *, lang: str) -> str:
    """Return the body of the first ```<lang> block that appears
    after ``anchor`` in ``text``.

    Args:
        text: full markdown source.
        anchor: substring whose first occurrence anchors the search
            (e.g. a ``###`` heading).
        lang: required code-fence language tag (``yaml``, ``json``,
            ``bash``, etc.).

    Returns:
        The fenced block's body (everything between the fences,
        excluding the trailing newline before the closing fence).

    Raises:
        AssertionError: if either ``anchor`` is missing from
            ``text`` or no matching fenced block follows it.
    """
    idx = text.find(anchor)
    assert idx >= 0, f"anchor not found in markdown: {anchor!r}"
    for m in _FENCE_RE.finditer(text, idx):
        if m.group("lang") == lang:
            return m.group("body")
    raise AssertionError(f"no ```{lang} block found after anchor {anchor!r}")


def _columns_from_yaml_block(body: str) -> set[str]:
    """Return the set of ``columns[].name`` values from a YAML
    fragment containing a top-level ``columns:`` list."""
    doc = yaml.safe_load(body)
    cols = doc.get("columns") or []
    return {c["name"] for c in cols}


def _fv_source_columns_from_yaml_block(body: str, source_name: str) -> set[str]:
    """From a ``StreamingFeatureView`` YAML, return the
    ``sources[].columns[].name`` set for the source whose ``name``
    matches ``source_name``."""
    doc = yaml.safe_load(body)
    for src in doc.get("sources") or []:
        if src.get("name") == source_name:
            return {c["name"] for c in src.get("columns") or []}
    raise AssertionError(f"FV YAML did not declare sources[name={source_name!r}]")


def _events_json_keys_union(events: list[dict]) -> set[str]:
    """Union of every record's key set in an events.json payload.

    A single missing column on any record would still flag the
    invariant — the runtime requires every record to carry every
    column.
    """
    out: set[str] = set()
    for record in events:
        out |= set(record.keys())
    return out


# ---------------------------------------------------------------------------
# Heredoc extraction from verify_bug_bash.sh
# ---------------------------------------------------------------------------
#
# The script's events.json heredoc is delimited by
# ``cat > events.json <<'JSONEOF' ... JSONEOF`` so a single anchored
# regex is unambiguous.

_HEREDOC_RE = re.compile(
    r"cat\s*>\s*events\.json\s*<<\s*'JSONEOF'\s*\n" r"(?P<body>.*?)\n" r"JSONEOF",
    re.DOTALL,
)


def _verify_script_events_json_keys() -> set[str]:
    text = _VERIFY_SCRIPT.read_text()
    m = _HEREDOC_RE.search(text)
    assert m, (
        "scripts/verify_bug_bash.sh no longer contains the "
        "events.json heredoc — update the regex or the script."
    )
    payload = json.loads(m.group("body"))
    assert (
        isinstance(payload, list) and payload
    ), f"events.json heredoc is not a non-empty list: {payload!r}"
    return _events_json_keys_union(payload)


# ---------------------------------------------------------------------------
# Anchors — keep these aligned with docs/BUG_BASH.md headings.
# ---------------------------------------------------------------------------

_DATASOURCE_HEADING = "### `sources/datasources/CLICKSTREAM_EVENTS.yaml`"
_FV_HEADING = "### `sources/feature_views/USER_CLICK_STATS_DECL.yaml`"
_EVENTS_JSON_PROSE = "▶ Write a tiny payload of synthetic click events"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bug_bash_md() -> str:
    return _BUG_BASH_MD.read_text()


def test_bugbash_clickstream_yaml_matches_events_json_keys(bug_bash_md):
    """The doc's CLICKSTREAM_EVENTS datasource YAML and the doc's
    events.json payload must declare the same column set.

    Drift here is the exact failure mode that produced the BUG_BASH
    step-7 TODO: the YAML and the events.json agreed (4 cols), so
    snowml-core's pre-POST validator did not fire, and the request
    surfaced a server-side ``MISSING_REQUIRED_FIELD`` only after the
    HTTP round-trip.
    """
    yaml_body = _extract_first_fenced_block_after(
        bug_bash_md, _DATASOURCE_HEADING, lang="yaml"
    )
    yaml_cols = _columns_from_yaml_block(yaml_body)

    json_body = _extract_first_fenced_block_after(
        bug_bash_md, _EVENTS_JSON_PROSE, lang="bash"
    )
    payload = _extract_doc_events_payload(json_body)
    json_keys = _events_json_keys_union(payload)

    missing = sorted(yaml_cols - json_keys)
    extra = sorted(json_keys - yaml_cols)
    assert not missing and not extra, (
        f"docs/BUG_BASH.md datasource YAML vs events.json key drift: "
        f"missing_in_events={missing!r}, extra_in_events={extra!r}, "
        f"yaml_cols={sorted(yaml_cols)!r}, "
        f"events_keys={sorted(json_keys)!r}"
    )


def test_bugbash_clickstream_yaml_matches_verify_script_events_json(
    bug_bash_md,
):
    """The doc's CLICKSTREAM_EVENTS YAML and the verify-script's
    events.json heredoc must declare the same column set.

    Without this lock, the doc and the script can drift silently
    and the script's step-7 ingest would fail with a server-side
    error every run.
    """
    yaml_body = _extract_first_fenced_block_after(
        bug_bash_md, _DATASOURCE_HEADING, lang="yaml"
    )
    yaml_cols = _columns_from_yaml_block(yaml_body)
    script_keys = _verify_script_events_json_keys()

    missing = sorted(yaml_cols - script_keys)
    extra = sorted(script_keys - yaml_cols)
    assert not missing and not extra, (
        f"docs/BUG_BASH.md YAML vs scripts/verify_bug_bash.sh "
        f"events.json drift: missing_in_script={missing!r}, "
        f"extra_in_script={extra!r}, "
        f"yaml_cols={sorted(yaml_cols)!r}, "
        f"script_keys={sorted(script_keys)!r}"
    )


def test_bugbash_clickstream_yaml_matches_fv_sources_block(bug_bash_md):
    """The doc inlines CLICKSTREAM_EVENTS' columns in three places:
    the datasource YAML, the FV ``sources[name=CLICKSTREAM_EVENTS]
    .columns`` block, and the events.json payload.  This test pins
    that the first two agree (the third is covered by the test
    above).
    """
    ds_body = _extract_first_fenced_block_after(
        bug_bash_md, _DATASOURCE_HEADING, lang="yaml"
    )
    ds_cols = _columns_from_yaml_block(ds_body)

    fv_body = _extract_first_fenced_block_after(bug_bash_md, _FV_HEADING, lang="yaml")
    fv_cols = _fv_source_columns_from_yaml_block(fv_body, "CLICKSTREAM_EVENTS")

    missing = sorted(ds_cols - fv_cols)
    extra = sorted(fv_cols - ds_cols)
    assert not missing and not extra, (
        f"docs/BUG_BASH.md datasource YAML vs FV sources block "
        f"drift for CLICKSTREAM_EVENTS: missing_in_fv={missing!r}, "
        f"extra_in_fv={extra!r}, ds_cols={sorted(ds_cols)!r}, "
        f"fv_cols={sorted(fv_cols)!r}"
    )


# ---------------------------------------------------------------------------
# Internal: events.json payload extraction
# ---------------------------------------------------------------------------
#
# The doc's events.json fence is a ``bash`` block whose body
# embeds a ``cat > events.json <<'EOF' ... EOF`` heredoc.  Pull the
# JSON payload between the EOF markers out of that body.

_DOC_EVENTS_HEREDOC_RE = re.compile(
    r"cat\s*>\s*events\.json\s*<<\s*'EOF'\s*\n" r"(?P<body>.*?)\n" r"EOF",
    re.DOTALL,
)


def _extract_doc_events_payload(bash_body: str) -> list[dict]:
    """Extract and parse the JSON list embedded in the doc's
    ``events.json`` heredoc."""
    m = _DOC_EVENTS_HEREDOC_RE.search(bash_body)
    assert m, (
        "docs/BUG_BASH.md events.json bash block no longer contains "
        "a ``cat > events.json <<'EOF'`` heredoc — update the regex "
        "or the doc."
    )
    payload = json.loads(m.group("body"))
    assert (
        isinstance(payload, list) and payload
    ), f"events.json heredoc is not a non-empty list: {payload!r}"
    return payload


# ---------------------------------------------------------------------------
# Step 7 verifier-parser brittleness contract
# ---------------------------------------------------------------------------
#
# Background.  scripts/verify_bug_bash.sh step 7 invokes
#
#   snow feature query USER_CLICK_STATS_DECL --version V1 --keys '...'
#
# and then attempts ``json.loads(text[first_brace:])`` on the captured
# stdout.  Without ``--format JSON`` the snow CLI's ``ObjectResult``
# renders a Rich key-value table whose ``rows`` cell contains a
# Python ``repr()`` of a ``list[dict]`` (single-quoted, with surrounding
# pipes / box-drawing characters).  The first ``[`` in the captured
# text falls inside that cell, ``json.loads`` raises a
# ``JSONDecodeError``, and the verifier reports a misleading
# "[fail] query response missing expected feature columns" TODO —
# even when the underlying Online Service query returned the right
# data with the right column names.
#
# Steps 8 and 17 already pass ``--format JSON`` for exactly this
# reason — their inline comments in scripts/verify_bug_bash.sh
# (search for "deterministic substring search") call out the same
# rationale.  The fix is to add ``--format JSON`` to the step-7
# invocation so the captured stdout is unambiguous JSON and the
# column-name assertions are stable.
#
# This test pins that contract so any future revert of the flag
# fails CI.

_STEP7_BANNER = "# Step 7 \u2014"
_STEP8_BANNER = "# Step 8 \u2014"


def _extract_step7_block_from_verify(text: str) -> str:
    """Return the bash text bounded by the ``# Step 7 —`` and
    ``# Step 8 —`` section banners in ``verify_bug_bash.sh``."""
    start = text.find(_STEP7_BANNER)
    assert start >= 0, f"verify_bug_bash.sh missing {_STEP7_BANNER!r} section banner"
    end = text.find(_STEP8_BANNER, start)
    assert end > start, "verify_bug_bash.sh missing # Step 8 — boundary banner"
    return text[start:end]


def test_bugbash_step7_query_uses_format_json():
    """Step 7's ``snow feature query`` call must pass ``--format JSON``.

    Without the flag, the verifier's downstream ``json.loads(text[start:])``
    parser fails on a Rich-rendered ``ObjectResult`` whose ``rows`` cell
    is a Python repr of a list-of-dicts (single-quoted, embedded inside
    pipe-delimited table rows).  The result is a misleading
    "[fail] query response missing expected feature columns" TODO even
    when the underlying Online Service query returned the right data.
    See the comment block immediately above this test for the full
    failure cascade.
    """
    text = _VERIFY_SCRIPT.read_text()
    block = _extract_step7_block_from_verify(text)
    query_lines = [
        ln
        for ln in block.splitlines()
        if "feature query" in ln and "USER_CLICK_STATS_DECL" in ln
    ]
    assert query_lines, (
        "verify_bug_bash.sh step 7 no longer contains a "
        "'feature query USER_CLICK_STATS_DECL' invocation — update "
        "this test or fix the script."
    )
    invocation_lines = [ln for ln in query_lines if "snow_run" in ln or "$SNOW " in ln]
    assert invocation_lines, (
        "verify_bug_bash.sh step 7 has 'feature query USER_CLICK_STATS_DECL' "
        "lines but none of them is the actual invocation (no snow_run / "
        "$SNOW prefix found).  The test's heuristic is out of date — "
        "update the matcher."
    )
    for ln in invocation_lines:
        assert ("--format JSON" in ln) or ("--format json" in ln), (
            "verify_bug_bash.sh step 7's 'snow feature query' invocation "
            "must pass '--format JSON' so the captured stdout is "
            "parseable JSON.  Without this flag, the verifier's "
            "json.loads(text[start:]) parser fails on a Rich-rendered "
            "key-value table whose 'rows' cell contains a Python repr "
            "of a list-of-dicts.  See the comment block above this "
            "test for the full failure cascade.  Found: " + ln.strip()
        )


# ---------------------------------------------------------------------------
# Step 17 declarative-cleanup contract
# ---------------------------------------------------------------------------
#
# Background.  Step 17 historically taught operators to clean up the
# bug-bash feature view with::
#
#   snow sql -q "DROP ONLINE FEATURE TABLE IF EXISTS USER_CLICK_STATS_DECL"
#
# That command drops only the OFT.  The wrapper view, the
# ``$UDF_TRANSFORMED`` / ``$UDF_TRANSFORMED$BACKFILL`` tables, the
# ``_FEATURE_STORE_METADATA`` row and the stream-source ref count all
# survive, and the next ``snow feature apply`` silently no-ops the
# CREATE_FV (snowml-core's ``register_feature_view`` hits the leftover
# registration and routes through ``_get_feature_view_if_exists``).
# ``get_feature_view`` then reports ``online=False`` because
# ``_determine_online_config_from_oft`` derives ``enable`` from OFT
# presence — and ``snow feature query`` fails with ``(2110) Online
# store is not enabled``.
#
# The fix is to replace the raw SQL drop with a declarative cleanup —
# remove the FV YAML/Python files from ``sources/feature_views/`` and
# run ``snow feature plan`` + ``snow feature apply``.  Full-sync mode emits
# ``DROP_FV USER_CLICK_STATS_DECL``, which routes through
# ``imperative_executor.py`` → ``fs.delete_feature_view``, the single
# code path that drops *all* the FV's side-effects in one go.
#
# These two tests pin that contract on both surfaces (doc + verify
# script) so any regression to the partial-teardown shape fails CI.

_STEP17_HEADING = "## 17. Clean up the bug-bash feature view"
_STEP18_HEADING = "## 18. Drop the runtime"
# Match the section banner at the bottom of verify_bug_bash.sh, not the
# ``# Step 17 (drop the bug-bash feature view) IS exercised on Path B``
# comment that appears in the file's header docstring at the top.  The
# em-dash (\u2014) in the banner uniquely identifies the section.
_VERIFY_STEP17_BANNER = "# Step 17 \u2014"


def _extract_step17_block_from_doc(text: str) -> str:
    start = text.find(_STEP17_HEADING)
    assert start >= 0, f"BUG_BASH.md missing {_STEP17_HEADING!r}"
    end = text.find(_STEP18_HEADING, start)
    assert end > start, f"BUG_BASH.md missing step 18 boundary heading"
    return text[start:end]


def _extract_step17_block_from_verify(text: str) -> str:
    start = text.find(_VERIFY_STEP17_BANNER)
    assert (
        start >= 0
    ), f"verify_bug_bash.sh missing {_VERIFY_STEP17_BANNER!r} section banner"
    end = text.find("# Summary", start)
    assert end > start, "verify_bug_bash.sh missing summary boundary banner"
    return text[start:end]


def _doc_step17_command_blocks(block: str) -> str:
    """Concatenate the *executable* fenced code blocks from a step-17
    section.  We deliberately skip prose so a discussion of the
    anti-pattern (e.g. naming ``DROP ONLINE FEATURE TABLE`` in a
    ``Why declarative…`` callout) does not trip the test.

    Args:
        block: The full step-17 markdown block.

    Returns:
        Concatenation of every ``bash`` fenced block's body.
    """
    parts: list[str] = []
    for m in _FENCE_RE.finditer(block):
        if m.group("lang") == "bash":
            parts.append(m.group("body"))
    return "\n".join(parts)


def _bash_step17_executable_lines(block: str) -> str:
    """Drop comment-only lines from a bash step-17 section so the
    declarative-cleanup tests can grep for the *commands* the script
    actually runs without tripping on prose-style block comments
    that legitimately reference the anti-pattern.

    Lines whose first non-whitespace character is ``#`` are removed.
    Inline trailing comments are preserved (we don't try to be a
    bash-aware tokenizer — keeping logic simple is more important
    than handling pathological cases).

    Args:
        block: The full step-17 bash block (already extracted).

    Returns:
        Same block with comment-only lines stripped.
    """
    keep: list[str] = []
    for line in block.splitlines():
        if line.lstrip().startswith("#"):
            continue
        keep.append(line)
    return "\n".join(keep)


def test_bugbash_step17_doc_uses_declarative_cleanup(bug_bash_md):
    """Step 17 in docs/BUG_BASH.md must use the declarative
    ``rm YAML → snow feature plan → snow feature apply`` cleanup
    instead of a raw ``DROP ONLINE FEATURE TABLE`` SQL command.

    The raw SQL drops only the OFT and leaves the wrapper view,
    UDF_TRANSFORMED / BACKFILL tables, and the FS metadata row in
    place — see the docstring above for the full failure cascade
    (``snow feature query`` fails with ``(2110) Online store is not
    enabled``).
    """
    block = _extract_step17_block_from_doc(bug_bash_md)
    commands = _doc_step17_command_blocks(block)
    assert "DROP ONLINE FEATURE TABLE" not in commands, (
        "docs/BUG_BASH.md step 17 still hands the operator a "
        "'DROP ONLINE FEATURE TABLE' command — this is the "
        "partial-teardown footgun.  Replace it with "
        "rm sources/feature_views/USER_CLICK_STATS_DECL.{yaml,py} "
        "followed by 'snow feature plan' and 'snow feature apply' "
        "so the planner emits DROP_FV and snowml-core's "
        "delete_feature_view drops every side-effect in one shot."
    )
    assert "rm " in commands and "USER_CLICK_STATS_DECL" in commands, (
        "docs/BUG_BASH.md step 17 must instruct the operator to remove "
        "the USER_CLICK_STATS_DECL spec files so full-sync apply emits "
        "DROP_FV — no 'rm ... USER_CLICK_STATS_DECL ...' line found in "
        "the executable bash blocks."
    )
    assert "snow feature plan" in commands, (
        "docs/BUG_BASH.md step 17 must run 'snow feature plan' against "
        "the manifest project after removing the YAML so the operator "
        "can confirm the DROP_FV op before it executes."
    )
    assert "snow feature apply" in commands, (
        "docs/BUG_BASH.md step 17 must run 'snow feature apply' after "
        "the plan so the declarative DROP_FV op actually executes."
    )
    assert "DROP_FV" in block, (
        "docs/BUG_BASH.md step 17's ✓ Expect block must mention DROP_FV — "
        "this is the contract: full-sync apply emits exactly one "
        "DROP_FV USER_CLICK_STATS_DECL row."
    )


def test_bugbash_step17_verify_script_uses_declarative_cleanup():
    """scripts/verify_bug_bash.sh step 17 must mirror the doc — no raw
    ``DROP ONLINE FEATURE TABLE`` SQL, and a real declarative
    ``rm`` / ``feature plan`` / ``feature apply`` sequence.
    """
    text = _VERIFY_SCRIPT.read_text()
    block = _extract_step17_block_from_verify(text)
    code = _bash_step17_executable_lines(block)
    assert "DROP ONLINE FEATURE TABLE" not in code, (
        "scripts/verify_bug_bash.sh step 17 still issues a raw "
        "'DROP ONLINE FEATURE TABLE' SQL command — this is the same "
        "partial-teardown footgun the doc fix removes.  Mirror the "
        "doc's declarative cleanup instead."
    )
    assert "rm " in code and "USER_CLICK_STATS_DECL" in code, (
        "scripts/verify_bug_bash.sh step 17 must rm the bug-bash FV "
        "spec files from the workspace before re-running plan/apply."
    )
    assert "feature plan" in code, (
        "scripts/verify_bug_bash.sh step 17 must run 'snow feature plan' "
        "after removing the YAML to surface the DROP_FV op."
    )
    assert "feature apply" in code, (
        "scripts/verify_bug_bash.sh step 17 must run 'snow feature apply' "
        "after the plan so the declarative DROP_FV is actually executed."
    )
    assert "DROP_FV" in code, (
        "scripts/verify_bug_bash.sh step 17 must assert the plan shows "
        "DROP_FV USER_CLICK_STATS_DECL — without this the declarative "
        "contract is not pinned."
    )
