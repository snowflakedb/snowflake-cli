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
- ``get_cli_context`` reports ``database=JKEW_DB``, ``schema=JKEW_SCHEMA``.

Each test then writes the BUG_BASH §5 spec tree to ``tmp_path`` and
invokes ``manager.plan(...)`` (or ``manager.write_plan(...)``).  This
is the path the live ``snow feature plan`` actually executes after
step 6 deploy.  A regression in any of the four planner code paths
(``manager.plan`` adding a second ``generate_plan`` call site,
``_full_spec_hash`` skipping the strip-before-hash contract, the
nested-features lookup re-breaking, or the strict-``<`` version
semantics flipping back) surfaces here.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from tests.feature.test_manager import (  # noqa: F401 (autouse fixtures)
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
        "scheduling_state: RUNNING\n"
        "ordered_entity_column_names:\n"
        "  - USER_ID\n"
        "timestamp_field: TIMESTAMP\n"
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


def _write_bug_bash_tree(tmp_path: Path) -> Path:
    """Lay out the BUG_BASH §5 working directory under *tmp_path*.

    Args:
        tmp_path: pytest tmp dir.

    Returns:
        Path to the ``$DECL_DIR/feature_views`` directory the
        ``manager.plan`` invocation reads from (we pass the FV YAML
        path so ``_expand_with_datasources`` walks sibling
        ``entities/`` and ``datasources/`` trees).
    """
    decl_dir = tmp_path / f"{_BUG_BASH_DB}.{_BUG_BASH_SCHEMA}"
    entity_dir = decl_dir / "entities"
    source_dir = decl_dir / "datasources"
    fv_dir = decl_dir / "feature_views"
    entity_dir.mkdir(parents=True)
    source_dir.mkdir(parents=True)
    fv_dir.mkdir(parents=True)
    (entity_dir / "USER_ID.yaml").write_text(_bug_bash_entity_yaml())
    (source_dir / "CLICKSTREAM_EVENTS.yaml").write_text(_bug_bash_source_yaml())
    (fv_dir / f"{_BUG_BASH_FV_NAME}.yaml").write_text(_bug_bash_fv_yaml())
    (fv_dir / f"{_BUG_BASH_FV_NAME}.py").write_text(_bug_bash_udf_py())
    return fv_dir


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
        "snowflake.cli._plugins.feature.manager.FeatureManager._ensure_session_setup"
    ) as setup:
        exec_q.return_value = iter([_bug_bash_show_oft_row()])
        oft.return_value = {_BUG_BASH_OFT_NAME: _golden_specification()}
        ent.return_value = [_bug_bash_entity_row()]
        setup.return_value = None
        yield {
            "execute_query": exec_q,
            "fetch_oft_state": oft,
            "fetch_entity_rows": ent,
            "ensure_session_setup": setup,
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

        fv_dir = _write_bug_bash_tree(tmp_path)

        result = FeatureManager().plan(
            input_files=[str(fv_dir / f"{_BUG_BASH_FV_NAME}.yaml")],
            config=None,
            dev_mode=False,
            allow_recreate=False,
            no_delete=False,
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

        fv_dir = _write_bug_bash_tree(tmp_path)
        out_path = tmp_path / "feature_plan.json"

        written = FeatureManager().write_plan(
            input_files=[str(fv_dir / f"{_BUG_BASH_FV_NAME}.yaml")],
            config=None,
            dev_mode=False,
            out_path=str(out_path),
            no_delete=False,
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

        fv_dir = _write_bug_bash_tree(tmp_path)

        result = FeatureManager().plan(
            input_files=[str(fv_dir / f"{_BUG_BASH_FV_NAME}.yaml")],
            config=None,
            dev_mode=False,
            allow_recreate=False,
            no_delete=False,
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
