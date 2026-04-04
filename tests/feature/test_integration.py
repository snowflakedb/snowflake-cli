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

"""Integration tests: CLI FeatureManager wired to the real decl library.

The Snowflake connection (execute_query) is mocked; all decl library calls
use the real implementation installed from the decl wheel.
"""

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path
from unittest import mock

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ENTITY_YAML = textwrap.dedent(
    """\
    kind: Entity
    name: user
    database: DB
    schema: SCH
    join_keys:
      - name: user_id
        type: StringType
    """
)

_FV_YAML = textwrap.dedent(
    """\
    kind: StreamingFeatureView
    name: user_clicks
    database: DB
    schema: SCH
    version: V1
    ordered_entity_column_names:
      - user_id
    sources: []
    features:
      - source_column:
          name: event
          type: StringType
        output_column:
          name: event
          type: StringType
    """
)


@pytest.fixture
def spec_dir():
    """Temporary directory with two YAML spec files."""
    with tempfile.TemporaryDirectory() as d:
        Path(d, "entity.yaml").write_text(_ENTITY_YAML)
        Path(d, "fv.yaml").write_text(_FV_YAML)
        yield d


@pytest.fixture
def mock_execute_query():
    """Patch FeatureManager.execute_query to avoid a real Snowflake connection."""
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as m:
        m.return_value = iter([])
        yield m


# ---------------------------------------------------------------------------
# apply — dry_run
# ---------------------------------------------------------------------------


class TestApplyDryRunIntegration:
    def test_dry_run_returns_status_dry_run(self, spec_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=[f"{spec_dir}/*.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert result["status"] == "dry_run"

    def test_dry_run_executes_no_ddl(self, spec_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=[f"{spec_dir}/*.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert result["executed"] == 0

    def test_dry_run_plans_ops(self, spec_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=[f"{spec_dir}/*.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert result["ops"] >= 0
        assert result["executed"] == 0

    def test_dry_run_calls_show_queries(self, spec_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.apply(
            input_files=[f"{spec_dir}/*.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        calls = [c[0][0] for c in mock_execute_query.call_args_list]
        assert any("SHOW" in sql.upper() for sql in calls)


# ---------------------------------------------------------------------------
# apply — non-dry-run (executes SQL stmts via real generate_sql)
# ---------------------------------------------------------------------------


class TestApplyExecuteIntegration:
    def test_apply_returns_applied_status(self, spec_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=[f"{spec_dir}/*.yaml"],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert result["status"] == "applied"

    def test_apply_result_has_ops_and_executed_keys(self, spec_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=[f"{spec_dir}/*.yaml"],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert "ops" in result
        assert "executed" in result


# ---------------------------------------------------------------------------
# list_specs — real library
# ---------------------------------------------------------------------------


class TestListSpecsIntegration:
    def test_list_specs_from_files_returns_source_files(
        self, spec_dir, mock_execute_query
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.list_specs(
            input_files=(f"{spec_dir}/entity.yaml",),
            config=None,
        )
        assert result["source"] == "files"
        assert "specs" in result

    def test_list_specs_from_snowflake_returns_source_snowflake(
        self, mock_execute_query
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.list_specs(input_files=(), config=None)
        assert result["source"] == "snowflake"

    def test_list_specs_bad_file_returns_empty(self, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.list_specs(
            input_files=("/nonexistent/path/spec.yaml",),
            config=None,
        )
        # Loader silently skips unresolvable files — returns empty spec list
        assert result["source"] == "files"
        assert result["specs"] == []


# ---------------------------------------------------------------------------
# convert — real library
# ---------------------------------------------------------------------------


class TestConvertIntegration:
    def test_convert_returns_converted_status(self, spec_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.convert(
            input_files=[f"{spec_dir}/entity.yaml"],
            file_format="yaml",
            output_dir=None,
            recursive=False,
            config=None,
        )
        assert result["status"] == "converted"
        assert result["format"] == "yaml"

    def test_convert_reports_spec_count(self, spec_dir, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.convert(
            input_files=[f"{spec_dir}/*.yaml"],
            file_format="json",
            output_dir=None,
            recursive=False,
            config=None,
        )
        assert result["count"] >= 0


# ---------------------------------------------------------------------------
# Validation error path
# ---------------------------------------------------------------------------


class TestValidationErrorPath:
    def test_validation_error_returns_validation_failed(self, mock_execute_query):
        """A spec with MISSING_VERSION should surface as validation_failed."""
        import tempfile
        import textwrap
        from pathlib import Path

        bad_fv = textwrap.dedent(
            """\
            kind: StreamingFeatureView
            name: bad_fv
            database: DB
            schema: SCH
            version: null
            ordered_entity_column_names: []
            sources: []
            features: []
            """
        )
        with tempfile.TemporaryDirectory() as d:
            Path(d, "bad.yaml").write_text(bad_fv)
            from snowflake.cli._plugins.feature.manager import FeatureManager

            mgr = FeatureManager()
            result = mgr.apply(
                input_files=[f"{d}/bad.yaml"],
                config=None,
                dry_run=True,
                dev_mode=False,
                overwrite=False,
                allow_recreate=False,
            )
        assert result["status"] == "validation_failed"
        assert "errors" in result
