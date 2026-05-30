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

"""End-to-end re-plan idempotency tests for ``FeatureManager.{plan,write_plan}``.

The unit tests in ``snowml/.../test_planner_revalidate_identical_spec.py``
and ``test_planner_revalidate_golden_describe.py`` pin the validator
and planner against an identical re-plan.  Those tests stop at the
``decl_api`` boundary.

This module drives the full CLI pipeline — ``manager.plan`` and
``manager.write_plan`` — with the *real* ``decl_api`` functions
(``validate_specs``, ``generate_plan``, ``fetch_applied_state``,
``serialize_plan``).  Only the I/O surface is mocked:

- ``execute_query`` returns a single ``SHOW ONLINE FEATURE TABLES`` row
  for the BUG_BASH FV.
- ``_fetch_oft_state`` returns ``{"USER_CLICK_STATS_DECL$V1$ONLINE":
  golden}`` (the captured DESCRIBE TYPE = SPECIFICATION payload).
- ``_fetch_entity_rows`` returns one ``SHOW TAGS``-shaped row for
  ``USER_ID``.
- ``get_cli_context`` reports ``database=JKEW_DB``, ``schema=JKEW_SCHEMA``
  (matching the manifest written under ``tmp_path``).

Each test then writes the BUG_BASH §5 spec tree under
``<tmp_path>/sources/`` (Phase 3+4 layout: ``entities/``,
``datasources/``, ``feature_views/``) along with a ``manifest.yml``
pointing at JKEW_DB.JKEW_SCHEMA, and invokes
``manager.plan(from_dir=tmp_path, target_name=None, ...)`` (or
``manager.write_plan(from_dir=tmp_path, ...)``).  This is the path the
live ``snow feature plan`` actually executes after step 6 deploy.  A
regression in any of the four planner code paths (``manager.plan``
adding a second ``generate_plan`` call site, ``_full_spec_hash``
skipping the strip-before-hash contract, the nested-features lookup
re-breaking, or the strict-``<`` version semantics flipping back)
surfaces here.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from tests.feature.test_manager import (  # noqa: F401 (autouse fixtures)
    mock_account_identifier,
    mock_build_session,
    mock_cli_context,
)

# ---------------------------------------------------------------------------
# Shared BUG_BASH §5 fixtures — match scripts/verify_bug_bash.sh and
# snowml/.../tests/golden_specs/USER_CLICK_STATS_DECL.json byte-for-byte.
# ---------------------------------------------------------------------------


_BUG_BASH_DB = "JKEW_DB"
_BUG_BASH_SCHEMA = "JKEW_SCHEMA"
_BUG_BASH_FV_NAME = "USER_CLICK_STATS_DECL"
_BUG_BASH_FV_VERSION = "V1"
_BUG_BASH_OFT_NAME = f"{_BUG_BASH_FV_NAME}${_BUG_BASH_FV_VERSION}$ONLINE"


def _bug_bash_manifest_yaml() -> str:
    """Phase 3+4 manifest pointing at JKEW_DB.JKEW_SCHEMA.

    ``account_identifier`` matches the autouse
    ``mock_account_identifier`` fixture (TEST_ORG-TEST_ACCT) so
    ``_resolve_project`` does not refuse the request.
    """
    return textwrap.dedent(
        f"""\
        manifest_version: 1
        type: feature_store
        default_target: BUG_BASH
        targets:
          BUG_BASH:
            account_identifier: TEST_ORG-TEST_ACCT
            database: {_BUG_BASH_DB}
            schema: {_BUG_BASH_SCHEMA}
        """
    )


def _bug_bash_entity_yaml() -> str:
    return (
        "kind: Entity\n"
        "name: USER_ID\n"
        'description: "Bug-bash user identifier."\n'
        "join_keys:\n"
        "  - name: USER_ID\n"
        "    type: StringType\n"
    )


def _bug_bash_source_yaml() -> str:
    return (
        "kind: StreamingSource\n"
        "name: CLICKSTREAM_EVENTS\n"
        "type: REST\n"
        "columns:\n"
        "  - name: USER_ID\n"
        "    type: StringType\n"
        "  - name: SESSION_ID\n"
        "    type: StringType\n"
        "  - name: PAGE_URL\n"
        "    type: StringType\n"
        "  - name: EVENT_TYPE\n"
        "    type: StringType\n"
        "  - name: TIMESTAMP\n"
        "    type: TimestampType\n"
        "  - name: TIME_ON_PAGE_SECONDS\n"
        "    type: DoubleType\n"
    )


def _bug_bash_fv_yaml() -> str:
    return (
        "kind: StreamingFeatureView\n"
        f"name: {_BUG_BASH_FV_NAME}\n"
        f"version: {_BUG_BASH_FV_VERSION}\n"
        "online: true\n"
        "entities:\n"
        "  - USER_ID\n"
        "timestamp_col: TIMESTAMP\n"
        "feature_granularity_sec: 300\n"
        "feature_aggregation_method: tiles\n"
        "sources:\n"
        "  - name: CLICKSTREAM_EVENTS\n"
        "    columns:\n"
        "      - name: USER_ID\n"
        "        type: StringType\n"
        "      - name: SESSION_ID\n"
        "        type: StringType\n"
        "      - name: PAGE_URL\n"
        "        type: StringType\n"
        "      - name: EVENT_TYPE\n"
        "        type: StringType\n"
        "      - name: TIMESTAMP\n"
        "        type: TimestampType\n"
        "      - name: TIME_ON_PAGE_SECONDS\n"
        "        type: DoubleType\n"
        "    source_type: Stream\n"
        "features:\n"
        "  - output_column:\n"
        "      name: TOTAL_ENGAGEMENT_1H\n"
        "      type: DoubleType\n"
        "    window_sec: 3600\n"
        "    function: sum\n"
        "    source_column:\n"
        "      name: ENGAGEMENT_SCORE\n"
        "      type: DoubleType\n"
        "  - output_column:\n"
        "      name: HAS_CONVERSION_24H\n"
        "      type: BooleanType\n"
        "    window_sec: 86400\n"
        "    function: max\n"
        "    source_column:\n"
        "      name: IS_CONVERSION\n"
        "      type: BooleanType\n"
        "udf:\n"
        "  name: compute_engagement_metrics\n"
        "  engine: pandas\n"
        "  output_columns:\n"
        "    - name: USER_ID\n"
        "      type: StringType\n"
        "    - name: TIMESTAMP\n"
        "      type: TimestampType\n"
        "    - name: IS_CONVERSION\n"
        "      type: BooleanType\n"
        "    - name: ENGAGEMENT_SCORE\n"
        "      type: DoubleType\n"
        f"  file: {_BUG_BASH_FV_NAME}.py\n"
    )


def _bug_bash_udf_py() -> str:
    # MUST match the captured golden's ``udf.function_definition``
    # byte-for-byte (see snowml/.../golden_specs/USER_CLICK_STATS_DECL.json).
    # The compiler inlines the file content verbatim into the spec, and the
    # planner hashes it; any drift (an added import, a quoted annotation,
    # a stray newline) flips the local hash off the deployed hash and the
    # NO_CHANGE invariant collapses into a destructive RECREATE_FV.
    #
    # The bare ``pd.DataFrame`` annotation is unbound at module load time,
    # but the project-mode loader's UDF-companion rule
    # (``_is_udf_companion_py``) detects this ``.py`` as the body for the
    # sibling ``USER_CLICK_STATS_DECL.yaml`` (whose ``udf.file:`` matches
    # this basename) and skips ``importlib`` entirely. The compiler's
    # ``inline_udf_source`` reads the file as text instead.
    return (
        "def compute_engagement_metrics(clickstream: pd.DataFrame) -> pd.DataFrame:\n"
        '    """Compute engagement metrics from click-stream events."""\n'
        "    df = clickstream.copy()\n"
        "\n"
        '    conversion_events = ["purchase", "signup", "subscribe"]\n'
        '    df["IS_CONVERSION"] = df["EVENT_TYPE"].isin(conversion_events)\n'
        "\n"
        "    weights = {\n"
        '        "page_view": 1.0,\n'
        '        "click": 2.0,\n'
        '        "form_submit": 5.0,\n'
        '        "purchase": 10.0,\n'
        '        "signup": 8.0,\n'
        "    }\n"
        '    df["ENGAGEMENT_SCORE"] = df["EVENT_TYPE"].map(weights).fillna(1.0)\n'
        "\n"
        '    return df[["USER_ID", "TIMESTAMP", "IS_CONVERSION", "ENGAGEMENT_SCORE"]]\n'
    )


def _write_bug_bash_project(tmp_path: Path) -> Path:
    """Lay out the BUG_BASH §5 working directory as a Phase 3+4 project.

    Tree::

        <tmp_path>/
          manifest.yml
          sources/
            entities/USER_ID.yaml
            datasources/CLICKSTREAM_EVENTS.yaml
            feature_views/
              USER_CLICK_STATS_DECL.yaml   (udf.file: USER_CLICK_STATS_DECL.py)
              USER_CLICK_STATS_DECL.py     (UDF companion - not importlib-loaded)
          out/plan/  (created by write_plan)

    The UDF body sits beside its YAML inside ``sources/feature_views/``.
    The project-mode loader's UDF-companion rule
    (``_is_udf_companion_py``) sees that the YAML's ``udf.file:`` basename
    equals the ``.py`` basename and skips ``importlib`` entirely so the
    bare ``pd.DataFrame`` annotation never crashes the walk.
    ``compiler.inline_udf_source`` reads the file as text via the YAML
    pointer.

    Returns:
        Path to ``tmp_path`` (the project root the manager resolves
        via ``_resolve_project``).
    """
    (tmp_path / "manifest.yml").write_text(_bug_bash_manifest_yaml())
    sources = tmp_path / "sources"
    entity_dir = sources / "entities"
    source_dir = sources / "datasources"
    fv_dir = sources / "feature_views"
    entity_dir.mkdir(parents=True)
    source_dir.mkdir(parents=True)
    fv_dir.mkdir(parents=True)
    (entity_dir / "USER_ID.yaml").write_text(_bug_bash_entity_yaml())
    (source_dir / "CLICKSTREAM_EVENTS.yaml").write_text(_bug_bash_source_yaml())
    (fv_dir / f"{_BUG_BASH_FV_NAME}.yaml").write_text(_bug_bash_fv_yaml())
    (fv_dir / f"{_BUG_BASH_FV_NAME}.py").write_text(_bug_bash_udf_py())
    return tmp_path


def _golden_specification() -> dict[str, Any]:
    """Load the captured DESCRIBE TYPE = SPECIFICATION payload.

    Resolves the JSON by walking up from this test file until a
    sibling ``snowml/snowflake/ml/feature_store/decl/tests/golden_specs/``
    is found, then reading ``USER_CLICK_STATS_DECL.json``.  The
    installed wheel intentionally strips the ``tests/`` package so we
    can't import the golden through ``snowflake.ml.feature_store.decl``
    — the source-tree path is the only reliable lookup.

    Re-capturing the golden in snowml flows here automatically because
    we read the source-tree file.

    Returns:
        Parsed golden dict — the BUG_BASH §5 FV's DESCRIBE payload.

    Raises:
        FileNotFoundError: If the golden cannot be located after
            walking 10 parents up from this test file.
    """
    here = Path(__file__).resolve()
    for parent in [here, *here.parents][:10]:
        candidate = (
            parent
            / "snowml"
            / "snowflake"
            / "ml"
            / "feature_store"
            / "decl"
            / "tests"
            / "golden_specs"
            / f"{_BUG_BASH_FV_NAME}.json"
        )
        if candidate.exists():
            return json.loads(candidate.read_text())
    raise FileNotFoundError(
        f"Could not locate {_BUG_BASH_FV_NAME}.json under any sibling "
        f"snowml/.../golden_specs/ tree starting from {here}"
    )


def _bug_bash_show_oft_row() -> dict[str, Any]:
    """Return the ``SHOW ONLINE FEATURE TABLES`` row for the BUG_BASH FV.

    The row schema mirrors what ``snowflake.connector.DictCursor``
    actually surfaces from the SHOW command; ``state.fetch_applied_state``
    only reads ``name`` (and optional ``database_name`` /
    ``schema_name``), so we keep the row minimal and let the
    ``specification_map`` carry the full payload.

    Returns:
        Single dict row.
    """
    return {
        "name": _BUG_BASH_OFT_NAME,
        "database_name": _BUG_BASH_DB,
        "schema_name": _BUG_BASH_SCHEMA,
        "version": _BUG_BASH_FV_VERSION,
    }


def _bug_bash_entity_row() -> dict[str, Any]:
    """Return the ``SHOW TAGS LIKE 'SNOWML_FEATURE_STORE_ENTITY_%'`` row.

    Mirrors :func:`state._build_entity_object`'s expected row shape.

    Returns:
        Single dict row carrying the BUG_BASH USER_ID entity tag.
    """
    return {
        "name": "SNOWML_FEATURE_STORE_ENTITY_USER_ID",
        "database_name": _BUG_BASH_DB,
        "schema_name": _BUG_BASH_SCHEMA,
        "allowed_values": '["USER_ID"]',
        "comment": "Bug-bash user identifier.",
    }


# ---------------------------------------------------------------------------
# Fixtures — drive the *real* decl_api end-to-end through manager.{plan,
# write_plan}.  Only the I/O surface (SQL execution, OFT/entity fetch) is
# mocked; the validator, planner, and serializer all run for real.
# ---------------------------------------------------------------------------


@pytest.fixture
def bug_bash_cli_context():
    """Override ``mock_cli_context`` to point at JKEW_DB.JKEW_SCHEMA.

    The default ``mock_cli_context`` from ``test_manager.py`` reports
    ``TEST_DB.TEST_SCHEMA``, which would not match the BUG_BASH FV
    payload's ``metadata.database`` / ``metadata.schema`` — the spec
    key collision required for ``NO_CHANGE`` matching would never fire.
    """
    with mock.patch("snowflake.cli._plugins.feature.manager.get_cli_context") as m:
        ctx = mock.MagicMock()
        ctx.connection.database = _BUG_BASH_DB
        ctx.connection.schema = _BUG_BASH_SCHEMA
        ctx.connection.warehouse = "TEST_WH"
        ctx.connection.role = "TEST_ROLE"
        ctx.connection.account = "TEST_ORG-TEST_ACCT"
        m.return_value = ctx
        yield m


@pytest.fixture
def bug_bash_io():
    """Mock the I/O surface needed by ``manager.plan`` / ``write_plan``.

    Yields a dict carrying ``execute_query``, ``fetch_oft_state``, and
    ``fetch_entity_rows`` mocks so individual tests can override return
    values per-scenario (e.g. inject a divergent payload).
    """
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as exec_q, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_oft_state"
    ) as oft, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_entity_rows"
    ) as ent, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_feature_view_rows"
    ) as fvs, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_feature_group_rows"
    ) as fgs, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._assert_initialized"
    ) as assert_init:
        exec_q.return_value = iter([_bug_bash_show_oft_row()])
        oft.return_value = {_BUG_BASH_OFT_NAME: _golden_specification()}
        ent.return_value = [_bug_bash_entity_row()]
        # The streaming BUG_BASH FV is online (OFT-backed); the
        # offline-FV merge path in fetch_applied_state silently
        # skips on key collision so we can return the same row from
        # ``list_feature_views()`` without affecting the OFT-driven
        # state reconstruction.  Returning ``[]`` is also fine —
        # both shapes leave the test pinning the OFT path.
        fvs.return_value = []
        # The bug-bash project does not author any FeatureGroups, so
        # ``list_feature_groups()`` returns an empty list.  Stubbed
        # for the same reason as ``_fetch_feature_view_rows``: keeps
        # the imperative call off the mock session.
        fgs.return_value = []
        # Bypass the Phase 8 init-first guard so these idempotency
        # tests can exercise plan/write_plan without a live Snowflake
        # connection.  The negative-path tests for the guard live in
        # ``tests/feature/test_uninitialized_schema_errors.py``.
        assert_init.return_value = None
        yield {
            "execute_query": exec_q,
            "fetch_oft_state": oft,
            "fetch_entity_rows": ent,
            "fetch_feature_view_rows": fvs,
            "fetch_feature_group_rows": fgs,
            "assert_initialized": assert_init,
        }


# ---------------------------------------------------------------------------
# Acceptance criteria
# ---------------------------------------------------------------------------


class TestReplanIdenticalSpec:
    """``manager.plan`` and ``manager.write_plan`` against an identical
    just-deployed spec must surface as a clean ``NO_CHANGE`` end-to-end.
    """

    def test_replan_identical_spec_returns_no_change_status(
        self,
        tmp_path,
        bug_bash_cli_context,
        bug_bash_io,
    ):
        """``manager.plan`` end-to-end must report ``status=ready`` with
        every op ``NO_CHANGE`` and zero errors.

        This is the contract for BUG_BASH §9 — re-planning against the
        FV step 6 just deployed.  A regression in any of the four code
        paths named in this module's docstring surfaces here as either
        ``status=validation_failed`` (errors non-empty) or one or more
        ops with ``operation != "NO_CHANGE"``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_bug_bash_project(tmp_path)

        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "ready", (
            f"expected status='ready'; got status={result['status']!r} "
            f"errors={result.get('errors')!r}"
        )
        assert result["errors"] == [], (
            f"expected zero errors on identical-spec re-plan; got "
            f"{result['errors']!r}"
        )
        non_no_change = [op for op in result["ops"] if op["operation"] != "NO_CHANGE"]
        assert non_no_change == [], (
            "expected every op to be NO_CHANGE on identical-spec re-plan; "
            f"got non-NO_CHANGE ops={non_no_change!r}"
        )
        column_added = [w for w in result["warnings"] if "COLUMN_ADDED" in str(w)]
        assert column_added == [], (
            "expected zero COLUMN_ADDED warnings on identical-spec re-plan; "
            f"got {column_added!r}"
        )

    def test_replan_identical_spec_writes_no_change_plan_to_disk(
        self,
        tmp_path,
        bug_bash_cli_context,
        bug_bash_io,
    ):
        """``manager.write_plan`` must serialize a plan whose every op is
        ``NO_CHANGE``.

        Pins the ``snow feature apply --plan <file>`` consumer side: a
        non-NO_CHANGE op in the disk plan would let a destructive
        ``RECREATE_FV`` slip through to apply on a genuinely-unchanged
        spec.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_bug_bash_project(tmp_path)
        out_path = tmp_path / "feature_plan.json"

        written = FeatureManager().write_plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            out_path=str(out_path),
        )

        assert Path(written).exists(), f"plan file not written at {written!r}"
        plan_doc = json.loads(Path(written).read_text())
        ops = plan_doc.get("plan", {}).get("ops", [])
        assert ops, f"plan file has no ops; full doc={plan_doc!r}"
        non_no_change = [op for op in ops if op.get("kind") != "NO_CHANGE"]
        assert non_no_change == [], (
            "expected every disk-plan op to be NO_CHANGE on identical-spec "
            f"re-plan; got non-NO_CHANGE ops={non_no_change!r}"
        )

    def test_replan_with_divergent_specification_payload_still_no_change(
        self,
        tmp_path,
        bug_bash_cli_context,
        bug_bash_io,
    ):
        """A SPECIFICATION payload carrying extra Snowflake-stamped
        top-level keys must still re-plan as ``NO_CHANGE``.

        Validates the strip-before-hash contract in
        ``invariants._full_spec_hash`` (``_VOLATILE_METADATA_KEYS`` and
        ``_DERIVED_TOP_LEVEL_KEYS``).  If a future change drops the
        strip, the live re-plan against any deployed FV would tip into
        the BUG_BASH §9 cascade because the Snowflake-stamped fields
        bump the ``content_hash`` away from
        ``_compute_local_spec_hash(local)``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        # Inject extra Snowflake-stamped keys the local compiler does
        # not produce.  The strip-before-hash contract must absorb these
        # so the SPECIFICATION-backed AppliedObject's content_hash
        # collides with _compute_local_spec_hash(local).
        divergent = _golden_specification()
        divergent.setdefault("metadata", {})
        divergent["metadata"]["oft_id"] = "9999999999999999"
        divergent["metadata"]["client_version"] = "999.0.0-stamped"
        divergent["offline_configs"] = [
            {
                "store_type": "snowflake",
                "table": f"{_BUG_BASH_FV_NAME}$V1$UDF_TRANSFORMED",
                "table_type": "UDFTransformed",
            }
        ]
        divergent["online_store_type"] = "postgres"
        bug_bash_io["fetch_oft_state"].return_value = {
            _BUG_BASH_OFT_NAME: divergent,
        }

        _write_bug_bash_project(tmp_path)

        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "ready", (
            "Snowflake-stamped fields on the SPECIFICATION payload must "
            "not push validation into validation_failed; got "
            f"status={result['status']!r} errors={result.get('errors')!r}"
        )
        non_no_change = [op for op in result["ops"] if op["operation"] != "NO_CHANGE"]
        assert non_no_change == [], (
            "Snowflake-stamped fields on the SPECIFICATION payload must "
            "not produce non-NO_CHANGE ops on identical-spec re-plan; got "
            f"non-NO_CHANGE ops={non_no_change!r}"
        )


# ---------------------------------------------------------------------------
# BUG_BASH §11 — manager.plan op stream must preserve original-case names
# ---------------------------------------------------------------------------


class TestPlanPreservesNameCase:
    """``manager.plan`` op-stream names must match the on-disk JSON case.

    The op-stream returned by ``manager.plan`` (and rendered as the
    user-facing plan table) must carry the spec's original-case name so
    operators / scripts grepping for ``UPDATE_ENTITY USER_ID`` see the
    same identifier the disk plan and the apply path use.  Without this
    parity, ``scripts/verify_bug_bash.sh`` step 11's grep for the
    canonical ``UPDATE_ENTITY USER_ID`` row mis-fires (the entity row
    is rendered as ``user_id`` and never matches the doc-aligned
    uppercase pattern), and the fix at the planner / fingerprint /
    applied-state layer is masked behind a UI-only formatting bug.
    """

    def test_update_entity_op_preserves_uppercase_name(
        self,
        tmp_path,
        bug_bash_cli_context,
        bug_bash_io,
    ):
        """Edited entity description must surface as ``UPDATE_ENTITY USER_ID``.

        Override the fetched entity row's ``comment`` to a value
        different from the local YAML's ``description:`` so the
        Entity-aware fingerprint widening flips the row to
        ``UPDATE_ENTITY``.  The op's ``name`` must equal ``"USER_ID"``
        (uppercase) — matching the spec's authoring case and the disk
        plan's ``op["name"]``.  A regression to ``op.name.lower()`` in
        ``manager.plan`` would surface here as ``"user_id"``.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        # Make the deployed entity's comment diverge from the YAML's
        # description so the planner emits a non-destructive
        # UPDATE_ENTITY for USER_ID (rather than NO_CHANGE).
        diverged_entity_row = dict(_bug_bash_entity_row())
        diverged_entity_row["comment"] = "Bug-bash user identifier — older version."
        bug_bash_io["fetch_entity_rows"].return_value = [diverged_entity_row]

        _write_bug_bash_project(tmp_path)

        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "ready", (
            f"expected status='ready'; got status={result['status']!r} "
            f"errors={result.get('errors')!r}"
        )

        update_entity_ops = [
            op for op in result["ops"] if op["operation"] == "UPDATE_ENTITY"
        ]
        assert len(update_entity_ops) == 1, (
            "expected exactly one UPDATE_ENTITY op when the deployed "
            "entity's COMMENT differs from the local YAML's description; "
            f"got ops={[(o['operation'], o['name']) for o in result['ops']]!r}"
        )
        assert update_entity_ops[0]["name"] == "USER_ID", (
            "manager.plan op stream must preserve the spec's original-case "
            "name (USER_ID) so verify_bug_bash.sh step 11's "
            "'UPDATE_ENTITY USER_ID' grep matches the rendered output and "
            "the disk JSON plan share one canonical identifier; got "
            f"name={update_entity_ops[0]['name']!r}"
        )

    def test_no_change_op_preserves_uppercase_fv_name(
        self,
        tmp_path,
        bug_bash_cli_context,
        bug_bash_io,
    ):
        """Identical-spec NO_CHANGE row for a FV must keep its uppercase name.

        Regression guard: the same lowercasing surface that masked
        ``UPDATE_ENTITY USER_ID`` also turns ``USER_CLICK_STATS_DECL``
        into ``user_click_stats_decl``, breaking step 11's
        ``NO_CHANGE.*USER_CLICK_STATS_DECL`` grep.  Pin the contract on
        the FV row too so the case-preservation invariant covers both
        kinds the verify script asserts on.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_bug_bash_project(tmp_path)

        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        assert result["status"] == "ready", (
            f"expected status='ready'; got status={result['status']!r} "
            f"errors={result.get('errors')!r}"
        )

        fv_ops = [op for op in result["ops"] if op["name"].upper() == _BUG_BASH_FV_NAME]
        assert fv_ops, (
            f"expected at least one op naming {_BUG_BASH_FV_NAME}; got "
            f"ops={[(o['operation'], o['name']) for o in result['ops']]!r}"
        )
        assert fv_ops[0]["name"] == _BUG_BASH_FV_NAME, (
            "manager.plan op stream must preserve the spec's original-case "
            f"FV name ({_BUG_BASH_FV_NAME}); got name={fv_ops[0]['name']!r}"
        )


# ---------------------------------------------------------------------------
# Branch P integration — leftover BatchFV from a prior bug-bash run must
# not block re-plan with BATCH_FV_TILING_* errors.
# ---------------------------------------------------------------------------


_BATCH_FV_DECL_NAME = "MY_BATCH_FV_BATCH_DECL"
_BATCH_FV_DECL_VERSION = "V1"


def _bug_bash_batch_entity_yaml() -> str:
    return (
        "kind: Entity\n"
        "name: USER_BATCH_DECL\n"
        "join_keys:\n"
        "  - name: USER_ID\n"
        "    type: StringType\n"
    )


def _bug_bash_batch_fv_exported_yaml() -> str:
    """Mirror the YAML ``snow feature init`` writes when re-exporting a
    deployed non-tiled BatchFV.

    The shape matches the actual export captured under
    ``/private/var/folders/.../bugbash-verify-*.bF6bi8H7kt/bash/sources/
    feature_views/MY_BATCH_FV_BATCH_DECL.yaml`` from the failing run that
    motivated this fix:

    - The authoring YAML originally declared *no* ``features:`` block
      (see ``docs/BATCH_FV_BUG_BASH.md`` §5) — but the
      ``DESCRIBE … TYPE = SPECIFICATION`` round-trip synthesises N 1:1
      passthrough features keyed off the BatchSource columns, and the
      exporter renders those back into the YAML.
    - ``sources: []`` and ``timestamp_col`` / ``feature_granularity_sec``
      / ``feature_aggregation_method`` / ``batch_schedule`` are all
      *missing* — the FV is non-tiled and the SPECIFICATION payload
      carries none of those keys.
    - ``target_lag_sec`` is the imperative-shape unit conversion of
      ``target_lag: 1 minute``.

    Without the ``_check_batch_feature_view_constraints`` strip-before-
    check fix on the snowml side, every ``snow feature plan`` against
    this exported tree fires BATCH_FV_TILING_TIMESTAMP / _GRANULARITY /
    _AGG_METHOD and pushes the plan into ``validation_failed``.
    """
    return (
        "kind: BatchFeatureView\n"
        f"name: {_BATCH_FV_DECL_NAME}\n"
        f"version: {_BATCH_FV_DECL_VERSION}\n"
        f"database: {_BUG_BASH_DB}\n"
        f"schema: {_BUG_BASH_SCHEMA}\n"
        "online: true\n"
        "entities:\n"
        "  - USER_ID\n"
        "sources: []\n"
        "features:\n"
        "  - output_column:\n"
        "      name: EVENT_TS\n"
        "      type: TimestampType\n"
        "    source_column:\n"
        "      name: EVENT_TS\n"
        "      type: TimestampType\n"
        "  - output_column:\n"
        "      name: METRIC_VAL\n"
        "      type: DoubleType\n"
        "    source_column:\n"
        "      name: METRIC_VAL\n"
        "      type: DoubleType\n"
        "target_lag_sec: 60\n"
    )


def _write_bug_bash_batch_only_project(tmp_path: Path) -> Path:
    """Write a project tree carrying ONLY the leftover BatchFV trio.

    Mirrors the minimum surface ``snow feature init`` would produce for
    a runtime that has just ``MY_BATCH_FV_BATCH_DECL`` deployed and no
    other authoring objects — exactly the cascade the verify script
    walks through against ``JKEW_DB.JKEW_SCHEMA``.
    """
    (tmp_path / "manifest.yml").write_text(_bug_bash_manifest_yaml())
    sources = tmp_path / "sources"
    entity_dir = sources / "entities"
    fv_dir = sources / "feature_views"
    entity_dir.mkdir(parents=True)
    fv_dir.mkdir(parents=True)
    (entity_dir / "USER_BATCH_DECL.yaml").write_text(_bug_bash_batch_entity_yaml())
    (fv_dir / f"{_BATCH_FV_DECL_NAME}.yaml").write_text(
        _bug_bash_batch_fv_exported_yaml()
    )
    return tmp_path


class TestBatchFvAutoDerivedFeaturesPlanIntegration:
    """``manager.plan`` against a re-exported non-tiled BatchFV must clear
    validation cleanly — the BATCH_FV_TILING_* checks must not fire on
    snowml-core's auto-derived 1:1 passthrough features.

    Pairs with the snowml-side unit test
    ``test_batch_fv_passthrough_auto_derived_features_skip_tiling_checks``
    in ``snowml/.../tests/test_batch_feature_view_validation.py`` and
    pins the entire snow-CLI → decl_api → invariants pipeline against
    the BUG_BASH §step-6 cascade.  Regressing
    ``_check_batch_feature_view_constraints`` to its pre-fix shape
    surfaces here as ``status='validation_failed'`` with at least one
    ``BATCH_FV_TILING_*`` error.
    """

    def test_plan_against_reexported_batch_fv_does_not_fire_tiling_errors(
        self,
        tmp_path,
        bug_bash_cli_context,
        bug_bash_io,
    ):
        """A re-exported non-tiled BatchFV YAML must validate cleanly.

        The applied state is empty (no OFT row, no entity row) so the
        plan is a fresh CREATE — but the validator still walks every
        spec in the batch, and the BatchFV constraint check is the
        first invariant to fire if the strip-before-check fix has
        regressed.  Asserting zero ``BATCH_FV_TILING_*`` errors is
        the narrowest possible pin on this contract; the CREATE
        op-stream is incidental.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        # No deployed BatchFV — the planner must clear the validator
        # even when the only thing in the project tree is the export
        # shape that previously tripped BATCH_FV_TILING_*.
        bug_bash_io["fetch_oft_state"].return_value = {}
        bug_bash_io["fetch_entity_rows"].return_value = []
        bug_bash_io["execute_query"].return_value = iter([])

        _write_bug_bash_batch_only_project(tmp_path)

        result = FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        tiling_errors = [
            err for err in result.get("errors") or [] if "BATCH_FV_TILING_" in str(err)
        ]
        assert tiling_errors == [], (
            "manager.plan must not fire BATCH_FV_TILING_* against a "
            "re-exported non-tiled BatchFV (auto-derived 1:1 passthrough "
            f"features must be stripped before the tiling checks); got "
            f"errors={tiling_errors!r}"
        )
        assert result["status"] != "validation_failed", (
            "expected the BatchFV constraint check to clear; got "
            f"status={result['status']!r} errors={result.get('errors')!r}"
        )
