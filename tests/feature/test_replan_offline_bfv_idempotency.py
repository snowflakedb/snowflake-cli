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

"""End-to-end re-plan idempotency for offline-only ``BatchFeatureView``s.

Pins the bug fix from
``plans/offline_bfv_state_fix_b9da0006.plan.md``: an operator authors
a BFV with ``online: false``, runs ``snow feature apply``, and the
next ``snow feature plan`` must report ``NO_CHANGE`` rather than a
spurious ``CREATE_FV "New object: not found in applied state."``.

The test drives the full CLI pipeline — ``manager.plan`` and
``manager.write_plan`` — with the *real* ``decl_api`` functions
(``validate_specs``, ``generate_plan``, ``fetch_applied_state``,
``serialize_plan``).  Only the I/O surface is mocked:

- ``execute_query`` returns an empty ``SHOW ONLINE FEATURE TABLES``
  (no OFT for an offline-only BFV) plus a dummy
  ``SHOW DYNAMIC TABLES`` row.
- ``_fetch_oft_state`` returns ``{}`` (no OFT, no spec).
- ``_fetch_dt_text_map`` returns the ``CREATE DYNAMIC TABLE`` text for
  the offline DT so the source binding is recoverable.
- ``_fetch_entity_rows`` returns one ``SHOW TAGS``-shaped row for
  ``USER_ID``.
- ``_fetch_feature_view_rows`` returns one ``list_feature_views()``
  row in the Phase-1 contract shape — this is the new I/O surface
  added by the fix.

This is the path the live ``snow feature plan`` actually executes
against an offline-only deploy.  A regression in the fix surfaces here
as ``operation != "NO_CHANGE"`` for the offline FV.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest import mock

import pytest

from tests.feature.test_manager import (  # noqa: F401  (autouse fixtures)
    mock_account_identifier,
    mock_build_session,
    mock_cli_context,
)

_DB = "JKEW_DB"
_SCH = "JKEW_SCHEMA"
_FV_NAME = "MY_BATCH_FV_BATCH_DECL"
_FV_VERSION = "V1"
_OFFLINE_DT_NAME = f"{_FV_NAME}${_FV_VERSION}"
_SOURCE_TABLE = "RAW_EVENTS_BATCH_DECL"


def _manifest_yaml() -> str:
    return textwrap.dedent(
        f"""\
        manifest_version: 1
        type: feature_store
        default_target: BUG_BASH
        targets:
          BUG_BASH:
            account_identifier: TEST_ORG-TEST_ACCT
            database: {_DB}
            schema: {_SCH}
        """
    )


def _entity_yaml() -> str:
    return (
        "kind: Entity\n"
        "name: USER_ID\n"
        'description: "Bug-bash user identifier."\n'
        "join_keys:\n"
        "  - name: USER_ID\n"
        "    type: StringType\n"
    )


def _datasource_yaml() -> str:
    return (
        "kind: BatchSource\n"
        "name: EVENTS_BATCH_DECL\n"
        f"table: {_SOURCE_TABLE}\n"
        "columns:\n"
        "  - name: USER_ID\n"
        "    type: StringType\n"
        "  - name: EVENT_TS\n"
        "    type: TimestampType\n"
        "  - name: METRIC_VAL\n"
        "    type: FloatType\n"
    )


def _offline_bfv_yaml() -> str:
    """Return the offline-only BFV YAML (the ``online: false`` repro)."""
    return (
        "kind: BatchFeatureView\n"
        f"name: {_FV_NAME}\n"
        f"version: {_FV_VERSION}\n"
        "online: false\n"
        "entities:\n"
        "  - USER_ID\n"
        "sources:\n"
        "  - name: EVENTS_BATCH_DECL\n"
        "    source_type: Batch\n"
        f"    table: {_SOURCE_TABLE}\n"
        "    columns:\n"
        "      - name: USER_ID\n"
        "        type: StringType\n"
        "      - name: EVENT_TS\n"
        "        type: TimestampType\n"
        "      - name: METRIC_VAL\n"
        "        type: FloatType\n"
        'batch_schedule: "1 minute"\n'
        'target_lag: "1 minute"\n'
    )


def _write_offline_bfv_project(tmp_path: Path) -> Path:
    (tmp_path / "manifest.yml").write_text(_manifest_yaml())
    sources = tmp_path / "sources"
    (sources / "entities").mkdir(parents=True)
    (sources / "datasources").mkdir(parents=True)
    (sources / "feature_views").mkdir(parents=True)
    (sources / "entities" / "USER_ID.yaml").write_text(_entity_yaml())
    (sources / "datasources" / "EVENTS_BATCH_DECL.yaml").write_text(_datasource_yaml())
    (sources / "feature_views" / f"{_FV_NAME}.yaml").write_text(_offline_bfv_yaml())
    return tmp_path


def _entity_row() -> dict:
    return {
        "name": "SNOWML_FEATURE_STORE_ENTITY_USER_ID",
        "database_name": _DB,
        "schema_name": _SCH,
        "allowed_values": '["USER_ID"]',
        "comment": "Bug-bash user identifier.",
    }


def _list_fv_row() -> dict:
    """Return the row shape ``imperative_executor.fetch_feature_view_rows``
    emits for the offline-only BFV."""
    return {
        "name": _FV_NAME,
        "version": _FV_VERSION,
        "database_name": _DB,
        "schema_name": _SCH,
        "kind": "BATCH",
        "entities": ["USER_ID"],
        "online_enabled": False,
        "target_lag": "1 minute",
        "refresh_freq": "1 minute",
        "warehouse": "TEST_WH",
        "desc": "",
        "physical_dt_name": _OFFLINE_DT_NAME,
    }


def _offline_dt_text() -> str:
    return (
        f"CREATE DYNAMIC TABLE {_DB}.{_SCH}.{_OFFLINE_DT_NAME}\n"
        "TARGET_LAG = '1 minute'\n"
        "WAREHOUSE = TEST_WH\n"
        f"AS SELECT * FROM {_DB}.{_SCH}.{_SOURCE_TABLE}"
    )


@pytest.fixture
def offline_bfv_cli_context():
    """Override the autouse ``mock_cli_context`` so the connection
    points at JKEW_DB.JKEW_SCHEMA — must match the FV YAML."""
    with mock.patch("snowflake.cli._plugins.feature.manager.get_cli_context") as m:
        ctx = mock.MagicMock()
        ctx.connection.database = _DB
        ctx.connection.schema = _SCH
        ctx.connection.warehouse = "TEST_WH"
        ctx.connection.role = "TEST_ROLE"
        ctx.connection.account = "TEST_ORG-TEST_ACCT"
        m.return_value = ctx
        yield m


@pytest.fixture
def offline_bfv_io():
    """Mock the I/O surface needed by ``manager.plan`` /
    ``write_plan`` for the offline-only BFV scenario.

    Specifically:

    - ``execute_query`` returns empty ``SHOW ONLINE FEATURE TABLES``
      (no OFT for offline-only) and an empty ``SHOW TABLES``.
    - ``_fetch_oft_state`` returns ``{}`` (no SPECIFICATION JSON).
    - ``_fetch_dt_text_map`` returns the offline DT's DDL text so
      :func:`state._inject_batch_fv_source_from_dt_text` can recover
      the source-table binding.
    - ``_fetch_entity_rows`` returns one ``SHOW TAGS`` row.
    - ``_fetch_feature_view_rows`` returns one
      ``list_feature_views`` row — the new arrow added by the fix.
    """
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as exec_q, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_oft_state"
    ) as oft, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_dt_text_map"
    ) as dt_text, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_entity_rows"
    ) as ent, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_feature_view_rows"
    ) as fvs, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._fetch_feature_group_rows"
    ) as fgs, mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._assert_initialized"
    ) as assert_init:
        exec_q.return_value = iter([])  # SHOW OFT / SHOW TABLES — empty
        oft.return_value = {}
        dt_text.return_value = {_OFFLINE_DT_NAME: _offline_dt_text()}
        ent.return_value = [_entity_row()]
        fvs.return_value = [_list_fv_row()]
        # No FeatureGroups in the offline-BFV scenario.  Stubbed empty
        # for the same reason as ``_fetch_feature_view_rows``: the
        # imperative call is gated on a real session and would
        # otherwise raise FeatureStoreNotInitializedError.
        fgs.return_value = []
        assert_init.return_value = None
        yield {
            "execute_query": exec_q,
            "fetch_oft_state": oft,
            "fetch_dt_text_map": dt_text,
            "fetch_entity_rows": ent,
            "fetch_feature_view_rows": fvs,
            "fetch_feature_group_rows": fgs,
            "assert_initialized": assert_init,
        }


class TestOfflineBfvReplanIdempotency:
    """``manager.plan`` against an unchanged offline-only BFV must
    report ``status=ready`` with every op ``NO_CHANGE``.  This is
    the bug fix from
    ``plans/offline_bfv_state_fix_b9da0006.plan.md`` — before the
    fix the planner re-emitted ``CREATE_FV`` with reason "New
    object: not found in applied state."
    """

    def test_replan_offline_bfv_returns_no_change(
        self,
        tmp_path,
        offline_bfv_cli_context,
        offline_bfv_io,
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_offline_bfv_project(tmp_path)

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
        assert result["errors"] == [], result["errors"]

        fv_ops = [op for op in result["ops"] if op["name"] == _FV_NAME]
        assert (
            len(fv_ops) == 1
        ), f"expected exactly one op for {_FV_NAME}; got {fv_ops!r}"
        assert fv_ops[0]["operation"] == "NO_CHANGE", (
            f"expected NO_CHANGE for offline-only BFV after applied state "
            f"surfaces it; got operation={fv_ops[0]['operation']!r} "
            f"reason={fv_ops[0]['reason']!r}"
        )
        # Negative pin: the bug would surface as a CREATE_FV with
        # this exact reason.  If a future regression silently
        # bypasses fetch_feature_view_rows, this catches it.
        assert "not found in applied state" not in fv_ops[0]["reason"]

    def test_replan_offline_bfv_calls_fetch_feature_view_rows(
        self,
        tmp_path,
        offline_bfv_cli_context,
        offline_bfv_io,
    ):
        """The manager MUST call ``_fetch_feature_view_rows`` during
        ``plan`` so offline-only FVs become visible to applied state.

        Pins the wiring contract: a future refactor that silently
        drops the call would let the bug regress.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_offline_bfv_project(tmp_path)

        FeatureManager().plan(
            from_dir=tmp_path,
            target_name=None,
            variables=[],
            dev_mode=False,
            allow_recreate=False,
        )

        offline_bfv_io["fetch_feature_view_rows"].assert_called()

    def test_write_plan_offline_bfv_writes_no_change_plan(
        self,
        tmp_path,
        offline_bfv_cli_context,
        offline_bfv_io,
    ):
        """``manager.write_plan`` must serialize a plan whose
        offline-BFV op is ``NO_CHANGE``.  Apply consumes plan files
        directly (no re-validation); a non-NO_CHANGE op slipping into
        the disk plan would let a destructive ``CREATE_FV`` execute
        on a genuinely-unchanged offline-only BFV.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_offline_bfv_project(tmp_path)
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
        fv_ops = [op for op in ops if op.get("name") == _FV_NAME]
        assert len(fv_ops) == 1, fv_ops
        assert fv_ops[0].get("kind") == "NO_CHANGE", fv_ops[0]
