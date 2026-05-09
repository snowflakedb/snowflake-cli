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

_DATASOURCE_HEADING = "### `$DECL/datasources/CLICKSTREAM_EVENTS.yaml`"
_FV_HEADING = "### `$DECL/feature_views/USER_CLICK_STATS_DECL.yaml`"
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
