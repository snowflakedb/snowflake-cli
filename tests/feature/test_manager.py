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

_SHOW_ROW_SINGLE = {
    "name": "my_fv$v1$ONLINE",
    "database_name": "TEST_DB",
    "schema_name": "TEST_SCHEMA",
    "scheduling_state": "RUNNING",
}

_DESCRIBE_ROWS_SINGLE = [
    {"name": "USER_ID", "type": "VARCHAR(16777216)", "kind": "Y"},
    {"name": "CLICK_COUNT_1H", "type": "NUMBER(38,0)", "kind": ""},
    {"name": "TIMESTAMP", "type": "TIMESTAMP_NTZ(9)", "kind": ""},
]


def _make_show_row(fv_name, entity_col="USER_ID"):
    return {
        "name": f"{fv_name}$v1$ONLINE",
        "database_name": "TEST_DB",
        "schema_name": "TEST_SCHEMA",
        "scheduling_state": "RUNNING",
    }


def _make_describe_rows(entity_col="USER_ID"):
    return [
        {"name": entity_col, "type": "VARCHAR(16777216)", "kind": "Y"},
        {"name": "FEATURE_COL", "type": "NUMBER(38,0)", "kind": ""},
    ]


class TestFeatureManagerExportSpecs:
    """Tests for export_specs which uses SHOW + DESCRIBE TABLE."""

    def _setup_mock(self, mock_execute_query, show_rows, describe_rows_map=None):
        """Configure mock to return SHOW rows first, then DESCRIBE rows per OFT.

        Args:
            show_rows: list of dicts for SHOW ONLINE FEATURE TABLES
            describe_rows_map: optional dict mapping OFT name → list of DESCRIBE dicts.
                If None, uses _DESCRIBE_ROWS_SINGLE for all.
        """
        default_desc = _DESCRIBE_ROWS_SINGLE

        def side_effect(query, **kwargs):
            if "SHOW ONLINE FEATURE TABLES" in query:
                return iter(show_rows)
            if "DESCRIBE TABLE" in query:
                if describe_rows_map:
                    for oft_name, desc_rows in describe_rows_map.items():
                        if oft_name in query:
                            return iter(desc_rows)
                return iter(default_desc)
            return iter([])

        mock_execute_query.side_effect = side_effect

    def test_export_creates_directory_structure(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """export_specs creates entities/ and feature_views/ with correct files."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._setup_mock(mock_execute_query, [_SHOW_ROW_SINGLE])
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        base = tmp_path / "TEST_DB.TEST_SCHEMA"
        assert (base / "feature_views" / "my_fv.yaml").exists()
        assert (base / "entities" / "USER_ID.yaml").exists()

    def test_export_deduplicates_entities(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """Two FVs sharing an entity column should produce only one entity file."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._setup_mock(
            mock_execute_query,
            [_make_show_row("fv_a"), _make_show_row("fv_b")],
        )
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        entity_dir = tmp_path / "TEST_DB.TEST_SCHEMA" / "entities"
        entity_files = list(entity_dir.iterdir())
        assert len(entity_files) == 1, f"Expected 1 entity file, got: {entity_files}"

    def test_export_yaml_content(self, mock_execute_query, mock_cli_context, tmp_path):
        """Exported YAML files have correct structure and field values."""
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._setup_mock(mock_execute_query, [_SHOW_ROW_SINGLE])
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        base = tmp_path / "TEST_DB.TEST_SCHEMA"

        fv_data = yaml.safe_load((base / "feature_views" / "my_fv.yaml").read_text())
        assert fv_data["kind"] == "StreamingFeatureView"
        assert fv_data["name"] == "my_fv"
        assert fv_data["version"] == "v1"
        assert fv_data["database"] == "TEST_DB"
        assert fv_data["schema"] == "TEST_SCHEMA"
        assert fv_data["ordered_entity_column_names"] == ["USER_ID"]

        entity_data = yaml.safe_load((base / "entities" / "USER_ID.yaml").read_text())
        assert entity_data["kind"] == "Entity"
        assert entity_data["name"] == "USER_ID"
        assert entity_data["join_keys"] == [{"name": "USER_ID", "type": "StringType"}]

    def test_export_returns_result_dict(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """export_specs returns a dict with status, directory, and files list."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._setup_mock(mock_execute_query, [_SHOW_ROW_SINGLE])
        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))

        assert result["status"] == "exported"
        assert "directory" in result
        assert isinstance(result["files"], list)
        # fv + entity = 2 files (no datasources since DESCRIBE doesn't provide source info)
        assert len(result["files"]) == 2

    def test_export_empty_schema(self, mock_execute_query, mock_cli_context, tmp_path):
        """export_specs with no OFTs returns empty file list."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._setup_mock(mock_execute_query, [])
        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))

        assert result["status"] == "exported"
        assert result["files"] == []

    def test_export_columns_from_describe(
        self, mock_execute_query, mock_cli_context, tmp_path
    ):
        """Exported FV YAML includes columns from DESCRIBE TABLE."""
        import yaml
        from snowflake.cli._plugins.feature.manager import FeatureManager

        self._setup_mock(mock_execute_query, [_SHOW_ROW_SINGLE])
        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        base = tmp_path / "TEST_DB.TEST_SCHEMA"
        fv_data = yaml.safe_load((base / "feature_views" / "my_fv.yaml").read_text())
        assert len(fv_data["columns"]) == 3
        assert fv_data["columns"][0]["name"] == "USER_ID"
