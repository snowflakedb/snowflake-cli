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

"""Tests for FeatureManager — mocks the decl library."""

import json
from unittest import mock

import pytest


@pytest.fixture
def mock_execute_query():
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as m:
        m.return_value = iter([])
        yield m


@pytest.fixture
def mock_decl():
    """Patch the entire decl api module used inside the manager."""
    with mock.patch("snowflake.cli._plugins.feature.manager.decl_api") as m:
        m.load_specs.return_value = mock.MagicMock(name="batch")
        m.fetch_applied_state.return_value = mock.MagicMock(name="state")
        m.validate_specs.return_value = []
        m.generate_plan.return_value = mock.MagicMock(name="plan", ops=[])
        yield m


class TestFeatureManagerApply:
    def test_apply_dry_run_returns_dict(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert isinstance(result, dict)

    def test_apply_dry_run_does_not_execute_sql(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        # execute_query should only be called for SHOW queries (state fetch), not for plan ops
        for call in mock_execute_query.call_args_list:
            sql = call[0][0] if call[0] else call[1].get("query", "")
            assert "SHOW" in sql.upper() or sql == ""

    def test_apply_calls_load_specs(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        mock_decl.load_specs.assert_called_once()

    def test_apply_load_specs_error_propagates(self, mock_execute_query, mock_decl):
        """If load_specs raises, the exception propagates to the caller."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.load_specs.side_effect = ValueError("bad spec file")
        mgr = FeatureManager()
        with pytest.raises(ValueError, match="bad spec file"):
            mgr.apply(
                input_files=["specs.yaml"],
                config=None,
                dry_run=False,
                dev_mode=False,
                overwrite=False,
                allow_recreate=False,
            )


class TestFeatureManagerListSpecs:
    def test_list_specs_returns_dict(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.list_specs(input_files=(), config=None)
        assert isinstance(result, dict)

    def test_list_specs_load_specs_error_propagates(
        self, mock_execute_query, mock_decl
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.load_specs.side_effect = ValueError("bad spec file")
        mgr = FeatureManager()
        with pytest.raises(ValueError, match="bad spec file"):
            mgr.list_specs(input_files=("specs.yaml",), config=None)


class TestFeatureManagerDescribe:
    def test_describe_returns_dict(self, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.describe(name="MY_ENTITY")
        assert isinstance(result, dict)


class TestFeatureManagerDrop:
    def test_drop_returns_dict(self, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.drop(names=("MY_ENTITY",))
        assert isinstance(result, dict)


class TestFeatureManagerConvert:
    def test_convert_returns_dict(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.convert(
            input_files=["specs.py"],
            file_format="yaml",
            output_dir=None,
            recursive=False,
            config=None,
        )
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_cli_context():
    """Patch get_cli_context used in service management methods."""
    with mock.patch("snowflake.cli._plugins.feature.manager.get_cli_context") as m:
        ctx = mock.MagicMock()
        ctx.connection.database = "TEST_DB"
        ctx.connection.schema = "TEST_SCHEMA"
        m.return_value = ctx
        yield m


class TestFeatureManagerGetStatus:
    STATUS_JSON = json.dumps(
        {
            "status": "RUNNING",
            "message": "All systems go",
            "endpoints": [{"name": "query", "url": "https://example.com/query"}],
            "created_at": "2024-01-01T00:00:00",
            "updated_at": "2024-01-02T00:00:00",
            "compute_pool": "active",  # extra field not in OnlineServiceStatus
        }
    )
    EMPTY_ENDPOINTS_JSON = json.dumps({"status": "PENDING", "endpoints": []})

    def test_get_status_returns_dict(self, mock_execute_query, mock_cli_context):
        """get_status() always returns a dict."""
        mock_execute_query.return_value = iter([(self.STATUS_JSON,)])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert isinstance(result, dict)

    def test_get_status_has_status_field(self, mock_execute_query, mock_cli_context):
        """Result dict includes 'status' with the correct value."""
        mock_execute_query.return_value = iter([(self.STATUS_JSON,)])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert "status" in result
        assert result["status"] == "RUNNING"

    def test_get_status_has_message_field(self, mock_execute_query, mock_cli_context):
        """Result dict includes 'message' key (aligned with OnlineServiceStatus)."""
        mock_execute_query.return_value = iter([(self.STATUS_JSON,)])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert "message" in result

    def test_get_status_has_endpoints_list(self, mock_execute_query, mock_cli_context):
        """Result dict includes 'endpoints' as a list."""
        mock_execute_query.return_value = iter([(self.STATUS_JSON,)])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert "endpoints" in result
        assert isinstance(result["endpoints"], list)

    def test_get_status_endpoints_are_plain_dicts(
        self, mock_execute_query, mock_cli_context
    ):
        """Endpoint items in result must be plain dicts with 'name' and 'url'."""
        mock_execute_query.return_value = iter([(self.STATUS_JSON,)])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        for ep in result["endpoints"]:
            assert isinstance(ep, dict), f"Expected dict endpoint, got {type(ep)}"
            assert "name" in ep
            assert "url" in ep

    def test_get_status_with_parse_status_available(
        self, mock_execute_query, mock_cli_context
    ):
        """When _HAS_ONLINE_SERVICE is True, _online_service_parse_status is called."""
        mock_execute_query.return_value = iter([(self.STATUS_JSON,)])

        mock_parse = mock.MagicMock()
        mock_parsed_status = mock.MagicMock()
        mock_parsed_status.status = "RUNNING"
        mock_parsed_status.message = "All systems go"
        mock_parsed_status.endpoints = ()
        mock_parsed_status.created_at = None
        mock_parsed_status.updated_at = None
        mock_parse.return_value = mock_parsed_status

        from snowflake.cli._plugins.feature.manager import FeatureManager

        with (
            mock.patch(
                "snowflake.cli._plugins.feature.manager._HAS_ONLINE_SERVICE", True
            ),
            mock.patch(
                "snowflake.cli._plugins.feature.manager._online_service_parse_status",
                mock_parse,
            ),
        ):
            mgr = FeatureManager()
            result = mgr.get_status()

        mock_parse.assert_called_once()
        assert result["status"] == "RUNNING"

    def test_get_status_fallback_preserves_raw_fields(
        self, mock_execute_query, mock_cli_context
    ):
        """When _HAS_ONLINE_SERVICE is False, raw JSON dict is returned (fallback)."""
        mock_execute_query.return_value = iter([(self.STATUS_JSON,)])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        with mock.patch(
            "snowflake.cli._plugins.feature.manager._HAS_ONLINE_SERVICE", False
        ):
            mgr = FeatureManager()
            result = mgr.get_status()

        assert result["status"] == "RUNNING"
        # In fallback mode extra raw JSON fields are preserved unchanged
        assert "compute_pool" in result

    def test_get_status_error_on_execute_exception(
        self, mock_execute_query, mock_cli_context
    ):
        """When execute_query raises, get_status returns an error dict."""
        mock_execute_query.side_effect = RuntimeError("DB connection failed")
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert result["status"] == "error"
        assert "error" in result

    def test_get_status_empty_endpoints(self, mock_execute_query, mock_cli_context):
        """Empty 'endpoints' in JSON produces an empty list in the result."""
        mock_execute_query.return_value = iter([(self.EMPTY_ENDPOINTS_JSON,)])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert result["endpoints"] == []


# ---------------------------------------------------------------------------
# export_specs
# ---------------------------------------------------------------------------

_SPEC_ROW_SINGLE = {
    "name": "my_fv$v1$ONLINE",
    "database_name": "TEST_DB",
    "schema_name": "TEST_SCHEMA",
    "specification": json.dumps(
        {
            "kind": "StreamingFeatureView",
            "metadata": {
                "name": "my_fv",
                "database": "TEST_DB",
                "schema": "TEST_SCHEMA",
                "version": "v1",
            },
            "spec": {
                "ordered_entity_column_names": ["user_id"],
                "sources": [
                    {
                        "name": "click_events",
                        "source_type": "Stream",
                        "columns": [
                            {"name": "user_id", "type": "StringType"},
                            {"name": "event_type", "type": "StringType"},
                            {"name": "timestamp", "type": "TimestampType"},
                        ],
                    },
                ],
                "features": [
                    {
                        "source_column": {"name": "event_type", "type": "StringType"},
                        "output_column": {
                            "name": "click_count_1h",
                            "type": "IntegerType",
                        },
                        "function": "count",
                        "window_sec": 3600,
                    },
                ],
                "timestamp_field": "timestamp",
            },
        }
    ),
}


def _make_fv_row(fv_name, entity_col="user_id", source_name=None, source_type="Stream"):
    if source_name is None:
        source_name = f"src_{fv_name}"
    return {
        "name": f"{fv_name}$v1$ONLINE",
        "database_name": "TEST_DB",
        "schema_name": "TEST_SCHEMA",
        "specification": json.dumps(
            {
                "kind": "StreamingFeatureView",
                "metadata": {
                    "name": fv_name,
                    "database": "TEST_DB",
                    "schema": "TEST_SCHEMA",
                    "version": "v1",
                },
                "spec": {
                    "ordered_entity_column_names": [entity_col],
                    "sources": [
                        {"name": source_name, "source_type": source_type, "columns": []}
                    ],
                    "features": [],
                    "timestamp_field": "ts",
                },
            }
        ),
    }


class TestFeatureManagerExportSpecs:
    def test_export_creates_directory_structure(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """export_specs creates entities/, datasources/, and feature_views/ with correct files."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_execute_query.return_value = iter([_SPEC_ROW_SINGLE])
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        base = tmp_path / "TEST_DB.TEST_SCHEMA"
        assert (base / "feature_views" / "my_fv.yaml").exists()
        assert (base / "entities" / "user_id.yaml").exists()
        assert (base / "datasources" / "click_events.yaml").exists()

    def test_export_deduplicates_entities(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """Two FVs sharing an entity column should produce only one entity file."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_execute_query.return_value = iter(
            [
                _make_fv_row("fv_a", entity_col="user_id"),
                _make_fv_row("fv_b", entity_col="user_id"),
            ]
        )
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        entity_dir = tmp_path / "TEST_DB.TEST_SCHEMA" / "entities"
        entity_files = list(entity_dir.iterdir())
        assert len(entity_files) == 1, f"Expected 1 entity file, got: {entity_files}"

    def test_export_deduplicates_sources(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """Two FVs sharing a source should produce only one datasource file."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_execute_query.return_value = iter(
            [
                _make_fv_row("fv_x", entity_col="cust_id", source_name="shared_stream"),
                _make_fv_row("fv_y", entity_col="cust_id", source_name="shared_stream"),
            ]
        )
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        src_dir = tmp_path / "TEST_DB.TEST_SCHEMA" / "datasources"
        src_files = list(src_dir.iterdir())
        assert len(src_files) == 1, f"Expected 1 datasource file, got: {src_files}"

    def test_export_yaml_content(self, mock_execute_query, mock_cli_context, tmp_path):
        """Exported YAML files have correct structure and field values."""
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_execute_query.return_value = iter([_SPEC_ROW_SINGLE])
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        base = tmp_path / "TEST_DB.TEST_SCHEMA"

        fv_data = yaml.safe_load((base / "feature_views" / "my_fv.yaml").read_text())
        assert fv_data["kind"] == "StreamingFeatureView"
        assert fv_data["name"] == "my_fv"
        assert fv_data["version"] == "v1"
        assert fv_data["database"] == "TEST_DB"
        assert fv_data["schema"] == "TEST_SCHEMA"
        assert fv_data["timestamp_field"] == "timestamp"
        assert fv_data["ordered_entity_column_names"] == ["user_id"]

        entity_data = yaml.safe_load((base / "entities" / "user_id.yaml").read_text())
        assert entity_data["kind"] == "Entity"
        assert entity_data["name"] == "user_id"
        assert entity_data["join_keys"] == [{"name": "user_id", "type": "StringType"}]

        src_data = yaml.safe_load(
            (base / "datasources" / "click_events.yaml").read_text()
        )
        assert src_data["kind"] == "StreamingSource"
        assert src_data["name"] == "click_events"
        assert len(src_data["columns"]) == 3

    def test_export_returns_result_dict(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """export_specs returns a dict with status, directory, and files list."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_execute_query.return_value = iter([_SPEC_ROW_SINGLE])
        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))

        assert result["status"] == "exported"
        assert "directory" in result
        assert isinstance(result["files"], list)
        assert len(result["files"]) == 3  # fv + entity + source

    def test_export_batch_source_kind(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """A source with source_type 'Batch' should be written as kind: BatchSource."""
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_execute_query.return_value = iter(
            [
                _make_fv_row("fv_batch", source_name="my_batch", source_type="Batch"),
            ]
        )
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        src_data = yaml.safe_load(
            (
                tmp_path / "TEST_DB.TEST_SCHEMA" / "datasources" / "my_batch.yaml"
            ).read_text()
        )
        assert src_data["kind"] == "BatchSource"
