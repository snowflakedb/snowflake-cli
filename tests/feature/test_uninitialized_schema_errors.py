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

"""CLI surfacing of :class:`FeatureStoreNotInitializedError` (Phase 6).

Pins NT6 from the
``remove_duplicate_entity_tag_prefix`` plan: every ``snow feature``
command except ``init`` must convert
:class:`snowflake.ml.feature_store.decl.errors.FeatureStoreNotInitializedError`
into a top-level :class:`click.ClickException` whose message:

1. Names the database/schema that failed the init-check.
2. Mentions the literal ``snow feature init`` so operators know
   the remediation.

``init`` itself must remain unaffected by the init-required guard —
it is the *bootstrap* command and must keep working against an
uninitialised schema.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest import mock

import pytest

# Re-use the autouse fixtures (CLI context, account identifier,
# build_session) that the broader feature-manager test suite already
# defines.  They make the L6 account check pass and avoid building a
# real Snowpark Session.
from tests.feature.test_manager import (  # noqa: F401
    mock_account_identifier,
    mock_build_session,
    mock_cli_context,
)

FEATURE_MANAGER = "snowflake.cli._plugins.feature.commands.FeatureManager"
MANAGER_MODULE = "snowflake.cli._plugins.feature.manager"


# ---------------------------------------------------------------------------
# Minimal manifest project — same shape as ``test_integration._write_minimal_project``
# but inlined here so this test file stays standalone.
# ---------------------------------------------------------------------------

_MANIFEST_YAML = textwrap.dedent(
    """\
    manifest_version: 1
    type: feature_store
    default_target: DEFAULT
    targets:
      DEFAULT:
        account_identifier: TEST_ORG-TEST_ACCT
        database: TEST_DB
        schema: TEST_SCHEMA
        role: TEST_ROLE
    """
)


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Write a minimal manifest project under ``tmp_path``."""
    (tmp_path / "manifest.yml").write_text(_MANIFEST_YAML)
    sources = tmp_path / "sources"
    (sources / "entities").mkdir(parents=True)
    (sources / "datasources").mkdir(parents=True)
    (sources / "feature_views").mkdir(parents=True)
    return tmp_path


def _make_not_initialized_error() -> Exception:
    """Construct the canonical
    :class:`FeatureStoreNotInitializedError` instance the manager
    should surface.

    Imported lazily because the exception lives in the snowml wheel,
    which is installed but not on the typing import path for the
    test runner's static check.
    """
    from snowflake.ml.feature_store.decl.errors import (
        FeatureStoreNotInitializedError,
    )

    wrapped = RuntimeError("Internal feature store tag not found")
    return FeatureStoreNotInitializedError(
        database="TEST_DB",
        schema="TEST_SCHEMA",
        wrapped=wrapped,
    )


def _assert_actionable_message(output: str) -> None:
    """Pin the user-facing remediation message.

    The CLI must say BOTH:

    1. That ``TEST_DB.TEST_SCHEMA`` is not initialised.
    2. That the remediation is to run ``snow feature init``.

    If either half is missing the operator has to guess at the next
    step from a backend stack trace — exactly the regression this
    phase exists to prevent.

    The Click error renderer wraps long messages across multiple
    lines inside a box (``+- Error -+ | ... |``), which means the
    literal phrase ``snow feature init`` may be broken by a line
    break.  Normalise whitespace before checking so the assertion is
    robust to box-wrapping.
    """
    import re

    # Strip Click's box-drawing pipes (``| ... |``) before collapsing
    # whitespace so a phrase that crosses a wrap boundary stays intact:
    # ``| ... `snow  |\n| feature init` ...``  →  ``... snow feature init ...``
    cleaned = re.sub(r"\s*\|\s*", " ", output)
    normalised = re.sub(r"\s+", " ", cleaned).lower()
    assert (
        "test_db" in normalised and "test_schema" in normalised
    ), f"Error message must name the target database/schema; got: {output!r}"
    assert (
        "snow feature init" in normalised
    ), f"Error message must direct operator at `snow feature init`; got: {output!r}"


# ---------------------------------------------------------------------------
# Per-command CLI guard tests (NT6)
# ---------------------------------------------------------------------------


class TestFeatureStoreNotInitializedSurfacedAsClickException:
    """Each non-``init`` command, when the underlying ``FeatureManager``
    raises :class:`FeatureStoreNotInitializedError`, must exit
    non-zero with the actionable remediation message.
    """

    @mock.patch(FEATURE_MANAGER)
    def test_plan_command_exits_nonzero_with_actionable_message(
        self,
        mock_manager,
        runner,
        project_dir,
    ):
        mock_manager.return_value.plan.side_effect = _make_not_initialized_error()
        result = runner.invoke(
            ["feature", "plan", "--from", str(project_dir), "--target", "DEFAULT"]
        )
        assert result.exit_code != 0, result.output
        _assert_actionable_message(result.output)

    @mock.patch(FEATURE_MANAGER)
    def test_apply_command_exits_nonzero_with_actionable_message(
        self,
        mock_manager,
        runner,
        project_dir,
        tmp_path,
    ):
        plan_file = tmp_path / "plan.json"
        plan_file.write_text("{}")
        mock_manager.return_value.apply.side_effect = _make_not_initialized_error()
        result = runner.invoke(
            [
                "feature",
                "apply",
                "--from",
                str(project_dir),
                "--target",
                "DEFAULT",
                "--plan",
                str(plan_file),
            ]
        )
        assert result.exit_code != 0, result.output
        _assert_actionable_message(result.output)

    @mock.patch(FEATURE_MANAGER)
    def test_list_command_exits_nonzero_with_actionable_message(
        self,
        mock_manager,
        runner,
        project_dir,
    ):
        mock_manager.return_value.list_specs.side_effect = _make_not_initialized_error()
        result = runner.invoke(
            ["feature", "list", "--from", str(project_dir), "--target", "DEFAULT"]
        )
        assert result.exit_code != 0, result.output
        _assert_actionable_message(result.output)

    @mock.patch(FEATURE_MANAGER)
    def test_describe_command_exits_nonzero_with_actionable_message(
        self,
        mock_manager,
        runner,
        project_dir,
    ):
        mock_manager.return_value.describe.side_effect = _make_not_initialized_error()
        result = runner.invoke(
            [
                "feature",
                "describe",
                "user_clicks",
                "--from",
                str(project_dir),
                "--target",
                "DEFAULT",
            ]
        )
        assert result.exit_code != 0, result.output
        _assert_actionable_message(result.output)

    @mock.patch(FEATURE_MANAGER)
    def test_ingest_command_exits_nonzero_with_actionable_message(
        self,
        mock_manager,
        runner,
        project_dir,
        tmp_path,
        monkeypatch,
    ):
        monkeypatch.setenv("SNOWFLAKE_PAT", "test_pat")
        data_file = tmp_path / "events.json"
        data_file.write_text(json.dumps([{"USER_ID": "u1"}]))
        mock_manager.return_value.ingest.side_effect = _make_not_initialized_error()
        result = runner.invoke(
            [
                "feature",
                "ingest",
                "CLICKSTREAM",
                "--data",
                str(data_file),
                "--from",
                str(project_dir),
                "--target",
                "DEFAULT",
            ]
        )
        assert result.exit_code != 0, result.output
        _assert_actionable_message(result.output)

    @mock.patch(FEATURE_MANAGER)
    def test_query_command_exits_nonzero_with_actionable_message(
        self,
        mock_manager,
        runner,
        project_dir,
    ):
        mock_manager.return_value.query.side_effect = _make_not_initialized_error()
        result = runner.invoke(
            [
                "feature",
                "query",
                "user_clicks",
                "--version",
                "V1",
                "--keys",
                json.dumps([{"USER_ID": "u1"}]),
                "--from",
                str(project_dir),
                "--target",
                "DEFAULT",
            ]
        )
        assert result.exit_code != 0, result.output
        _assert_actionable_message(result.output)


# ---------------------------------------------------------------------------
# ``init`` regression — must keep working against an uninitialised schema.
# ---------------------------------------------------------------------------


class TestInitNotBlockedByUninitialisedSchema:
    """``snow feature init`` is the bootstrap command and is the *only*
    way to take a schema from "no feature-store tags" to "ready".
    The init-required guard must NOT apply to it.
    """

    @mock.patch(FEATURE_MANAGER)
    def test_init_command_runs_against_uninitialised_schema(self, mock_manager, runner):
        """If ``init`` itself were guarded, operators would be stuck —
        the only command that can fix the situation would be the one
        the guard refuses to run.
        """
        mock_manager.return_value.init.return_value = {
            "status": "initialized",
            "directory": "/tmp/dummy",
            "files": [],
        }
        result = runner.invoke(["feature", "init"])
        assert result.exit_code == 0, result.output
        # The init guard regression check: under no circumstance may
        # the init command surface the "Run snow feature init first"
        # message — that would be circular.
        import re

        cleaned = re.sub(r"\s*\|\s*", " ", result.output)
        normalised = re.sub(r"\s+", " ", cleaned).lower()
        assert "run `snow feature init`" not in normalised
