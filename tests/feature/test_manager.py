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
        ctx.connection.role = "TEST_ROLE"
        m.return_value = ctx
        yield m


class TestFeatureManagerGetStatus:
    def test_get_status_returns_dict(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """get_status() always returns a dict."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([("raw_json",)])
        mock_decl.parse_service_status.return_value = {
            "status": "RUNNING",
            "message": "ok",
            "endpoints": [],
        }
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert isinstance(result, dict)

    def test_get_status_calls_service_sql_with_db_and_schema(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """get_status() calls service_sql with the connection database and schema."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([("raw_json",)])
        mock_decl.parse_service_status.return_value = {
            "status": "RUNNING",
            "endpoints": [],
        }
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.get_status()
        mock_decl.service_sql.assert_called_once_with(
            "TEST_DB", "TEST_SCHEMA", "TEST_ROLE"
        )

    def test_get_status_calls_parse_service_status(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """get_status() delegates parsing to decl_api.parse_service_status."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([("some_raw_json",)])
        mock_decl.parse_service_status.return_value = {
            "status": "RUNNING",
            "message": "All systems go",
            "endpoints": [{"name": "query", "url": "https://example.com/query"}],
        }
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        mock_decl.parse_service_status.assert_called_once_with("some_raw_json")
        assert result["status"] == "RUNNING"

    def test_get_status_has_status_field(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """Result dict includes 'status' with the correct value."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([("raw_json",)])
        mock_decl.parse_service_status.return_value = {
            "status": "RUNNING",
            "message": "ok",
            "endpoints": [],
        }
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert "status" in result
        assert result["status"] == "RUNNING"

    def test_get_status_empty_response_returns_error(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """When execute_query returns no rows, get_status returns an error dict."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.return_value = iter([])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert result["status"] == "error"
        mock_decl.parse_service_status.assert_not_called()

    def test_get_status_error_on_execute_exception(
        self, mock_execute_query, mock_cli_context, mock_decl
    ):
        """When execute_query raises, get_status returns an error dict."""
        mock_decl.service_sql.return_value = {"get_status": "SELECT STATUS()"}
        mock_execute_query.side_effect = RuntimeError("DB connection failed")
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.get_status()
        assert result["status"] == "error"
        assert "error" in result


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
    {
        "name": "USER_ID",
        "type": "VARCHAR(16777216)",
        "kind": "COLUMN",
        "null?": "N",
        "primary key": "Y",
        "unique key": "N",
    },
    {
        "name": "CLICK_COUNT_1H",
        "type": "NUMBER(38,0)",
        "kind": "COLUMN",
        "null?": "Y",
        "primary key": "N",
        "unique key": "N",
    },
    {
        "name": "TIMESTAMP",
        "type": "TIMESTAMP_NTZ(9)",
        "kind": "COLUMN",
        "null?": "Y",
        "primary key": "N",
        "unique key": "N",
    },
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
        {
            "name": entity_col,
            "type": "VARCHAR(16777216)",
            "kind": "COLUMN",
            "null?": "N",
            "primary key": "Y",
            "unique key": "N",
        },
        {
            "name": "FEATURE_COL",
            "type": "NUMBER(38,0)",
            "kind": "COLUMN",
            "null?": "Y",
            "primary key": "N",
            "unique key": "N",
        },
    ]


class TestFeatureManagerExportSpecs:
    """Tests for export_specs — verifies SQL execution and decl_api delegation."""

    def _setup_show_describe(self, mock_execute_query, show_rows, describe_rows=None):
        """Configure execute_query mock for SHOW + DESCRIBE queries."""
        desc = describe_rows or []

        def side_effect(query, **kwargs):
            if "SHOW ONLINE FEATURE TABLES" in query:
                return iter(show_rows)
            if "DESCRIBE ONLINE FEATURE TABLE" in query:
                return iter(desc)
            return iter([])

        mock_execute_query.side_effect = side_effect

    def test_export_delegates_to_decl_api(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """export_specs calls decl_api.export_specs with collected rows."""
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        self._setup_show_describe(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))
        mock_decl.export_specs.assert_called_once()
        assert result["status"] == "exported"

    def test_export_passes_correct_args_to_decl_api(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """export_specs passes show_rows, describe_map, output_dir, db, schema."""
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        self._setup_show_describe(
            mock_execute_query, [_SHOW_ROW_SINGLE], _DESCRIBE_ROWS_SINGLE
        )
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))

        call_args = mock_decl.export_specs.call_args
        show_rows_arg, describe_map_arg, out_dir_arg, db_arg, schema_arg = call_args[0]
        assert show_rows_arg == [_SHOW_ROW_SINGLE]
        assert "my_fv$v1$ONLINE" in describe_map_arg
        assert out_dir_arg == str(tmp_path)
        assert db_arg == "TEST_DB"
        assert schema_arg == "TEST_SCHEMA"

    def test_export_collects_describe_per_oft(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """A DESCRIBE query is issued for each OFT returned by SHOW."""
        mock_decl.export_specs.return_value = {
            "status": "exported",
            "directory": str(tmp_path),
            "files": [],
        }
        show_rows = [_make_show_row("fv_a"), _make_show_row("fv_b")]
        describe_call_count = [0]

        def side_effect(query, **kwargs):
            if "SHOW ONLINE FEATURE TABLES" in query:
                return iter(show_rows)
            if "DESCRIBE ONLINE FEATURE TABLE" in query:
                describe_call_count[0] += 1
                return iter([])
            return iter([])

        mock_execute_query.side_effect = side_effect
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.export_specs(str(tmp_path))
        assert describe_call_count[0] == 2

    def test_export_returns_decl_api_result(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """export_specs returns the dict from decl_api.export_specs unchanged."""
        expected = {
            "status": "exported",
            "directory": "/some/dir",
            "files": ["a.yaml", "b.yaml"],
        }
        mock_decl.export_specs.return_value = expected
        self._setup_show_describe(mock_execute_query, [_SHOW_ROW_SINGLE])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))
        assert result == expected

    def test_export_empty_schema_skips_decl_api(
        self, mock_execute_query, mock_cli_context, mock_decl, tmp_path
    ):
        """When SHOW returns no rows, decl_api.export_specs is not called."""
        mock_execute_query.return_value = iter([])
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.export_specs(str(tmp_path))
        assert result["status"] == "exported"
        assert result["files"] == []
        mock_decl.export_specs.assert_not_called()
