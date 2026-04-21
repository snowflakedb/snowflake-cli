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
        m.generate_plan.return_value = mock.MagicMock(name="plan", ops=[], warnings=[])
        m.serialize_plan.return_value = '{"version": "1", "plan": {"ops": [], "warnings": []}, "summary": {}, "created_at": "", "target_database": "TEST_DB", "target_schema": "TEST_SCHEMA", "source_files": []}'
        m.state_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "show_tables": "SHOW TABLES LIKE '%' IN SCHEMA TEST_DB.TEST_SCHEMA",
        }
        # generate_apply_sql returns an ApplyResult-like mock (used for dry_run)
        apply_result = mock.MagicMock()
        apply_result.status = "ready"
        apply_result.ops = []
        apply_result.sql_statements = []
        apply_result.warnings = []
        apply_result.errors = []
        m.generate_apply_sql.return_value = apply_result
        # execute_plan returns an ApplyResult-like mock (used for wet_run)
        exec_result = mock.MagicMock()
        exec_result.status = "applied"
        exec_result.ops = []
        exec_result.warnings = []
        exec_result.errors = []
        m.execute_plan.return_value = exec_result
        m.list_query.return_value = (
            "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA"
        )
        m.describe_query.return_value = (
            "SHOW ONLINE FEATURE TABLES LIKE 'test' IN SCHEMA TEST_DB.TEST_SCHEMA"
        )
        m.drop_queries.return_value = [
            'DROP ONLINE FEATURE TABLE IF EXISTS "TEST_DB"."TEST_SCHEMA"."test"'
        ]
        m.export_queries.return_value = {
            "show_ofts": "SHOW ONLINE FEATURE TABLES IN SCHEMA TEST_DB.TEST_SCHEMA",
            "describe_template": 'DESCRIBE ONLINE FEATURE TABLE "TEST_DB"."TEST_SCHEMA"."{name}"',
        }
        yield m


@pytest.fixture(autouse=True)
def mock_cli_context():
    """Patch get_cli_context for all manager tests."""
    with mock.patch("snowflake.cli._plugins.feature.manager.get_cli_context") as m:
        ctx = mock.MagicMock()

        ctx.connection.database = "TEST_DB"
        ctx.connection.schema = "TEST_SCHEMA"
        ctx.connection.warehouse = "TEST_WH"
        ctx.connection.role = "TEST_ROLE"
        m.return_value = ctx
        yield m


@pytest.fixture(autouse=True)
def mock_build_session():
    """Patch _build_session so wet-run apply tests don't need a real connection."""
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._build_session",
        return_value=mock.MagicMock(name="session"),
    ):
        yield


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

        # Set up apply_result with SQL that should NOT be executed in dry_run
        mock_decl.generate_apply_sql.return_value.sql_statements = ["CREATE TABLE t"]
        mgr = FeatureManager()
        mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        # execute_query should only be called for state queries, not for DDL
        for call in mock_execute_query.call_args_list:
            sql = str(call[0][0]) if call[0] else ""
            assert "CREATE" not in sql.upper(), f"DDL executed in dry_run: {sql}"

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
        mock_decl.service_sql.assert_called_once_with("TEST_DB", "TEST_SCHEMA")

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


# ---------------------------------------------------------------------------
# target_info
# ---------------------------------------------------------------------------


class TestTargetInfo:
    def test_apply_result_contains_target_database(self, mock_execute_query, mock_decl):
        """apply() result includes target_database from the CLI connection context."""
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
        assert "target_database" in result
        assert result["target_database"] == "TEST_DB"

    def test_apply_result_contains_target_schema(self, mock_execute_query, mock_decl):
        """apply() result includes target_schema from the CLI connection context."""
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
        assert "target_schema" in result
        assert result["target_schema"] == "TEST_SCHEMA"

    def test_apply_result_contains_target_warehouse(
        self, mock_execute_query, mock_decl
    ):
        """apply() result includes target_warehouse from the CLI connection context."""
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
        assert "target_warehouse" in result
        assert result["target_warehouse"] == "TEST_WH"

    def test_apply_wet_run_result_contains_target_info(
        self, mock_execute_query, mock_decl
    ):
        """apply() wet run also returns target info."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert result.get("target_database") == "TEST_DB"
        assert result.get("target_schema") == "TEST_SCHEMA"

    def test_list_specs_from_snowflake_contains_target_info(
        self, mock_execute_query, mock_decl
    ):
        """list_specs() from Snowflake returns target info."""
        mock_decl.enrich_list_results.return_value = []
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.list_specs(input_files=(), config=None)
        assert "target_database" in result
        assert result["target_database"] == "TEST_DB"
        assert "target_schema" in result
        assert result["target_schema"] == "TEST_SCHEMA"

    def test_target_info_empty_when_connection_has_no_warehouse(
        self, mock_execute_query, mock_decl, mock_cli_context
    ):
        """target_warehouse is empty string when connection warehouse is None."""
        mock_cli_context.return_value.connection.warehouse = None
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
        assert result.get("target_warehouse") == ""


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


class TestFeatureManagerInit:
    def test_init_calls_feature_store_with_create_if_not_exist(
        self, mock_execute_query, mock_cli_context
    ):
        """init() constructs FeatureStore with CreationMode.CREATE_IF_NOT_EXIST."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ) as mock_fs_cls, mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            mgr.init(no_scaffold=True)
            mock_fs_cls.assert_called_once()
            call_kwargs = mock_fs_cls.call_args[1]
            assert call_kwargs["creation_mode"] == mock_cm.CREATE_IF_NOT_EXIST

    def test_init_no_scaffold_skips_directory_creation(
        self, mock_execute_query, mock_cli_context, tmp_path, monkeypatch
    ):
        """init(no_scaffold=True) should not create any local directories."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = mgr.init(no_scaffold=True)
        assert result["directories"] == []
        assert not (tmp_path / "entities").exists()
        assert not (tmp_path / "datasources").exists()
        assert not (tmp_path / "feature_views").exists()

    def test_init_creates_three_directories(
        self, mock_execute_query, mock_cli_context, tmp_path, monkeypatch
    ):
        """init() (scaffold=True default) creates entities/, datasources/, feature_views/."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = mgr.init(no_scaffold=False)
        assert set(result["directories"]) == {
            "entities",
            "datasources",
            "feature_views",
        }
        assert (tmp_path / "entities").is_dir()
        assert (tmp_path / "datasources").is_dir()
        assert (tmp_path / "feature_views").is_dir()

    def test_init_returns_status_initialized(
        self, mock_execute_query, mock_cli_context, tmp_path, monkeypatch
    ):
        """init() returns a dict with status='initialized'."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        monkeypatch.chdir(tmp_path)
        mgr = FeatureManager()
        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore"
        ), mock.patch(
            "snowflake.ml.feature_store.feature_store.CreationMode"
        ) as mock_cm:
            mock_cm.CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST"
            result = mgr.init(no_scaffold=True)
        assert result["status"] == "initialized"
        assert result["database"] == "TEST_DB"
        assert result["schema"] == "TEST_SCHEMA"


# ---------------------------------------------------------------------------
# export_specs
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# write_plan
# ---------------------------------------------------------------------------


class TestWritePlan:
    def test_write_plan_creates_file(self, mock_execute_query, mock_decl, tmp_path):
        """write_plan() writes a JSON file to the given path."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        assert (tmp_path / "plan.json").exists()

    def test_write_plan_file_is_valid_json(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """File written by write_plan() is valid JSON."""
        import json

        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        content = (tmp_path / "plan.json").read_text()
        parsed = json.loads(content)
        assert isinstance(parsed, dict)
        assert parsed["version"] == "1"

    def test_write_plan_returns_path(self, mock_execute_query, mock_decl, tmp_path):
        """write_plan() returns the path to the written file."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        result = mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        assert result == out_path

    def test_write_plan_creates_parent_dirs(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """write_plan() creates parent directories if they don't exist."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plans" / "subdir" / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        assert (tmp_path / "plans" / "subdir" / "plan.json").exists()

    def test_write_plan_calls_load_specs(self, mock_execute_query, mock_decl, tmp_path):
        """write_plan() calls decl_api.load_specs with the provided files."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        mock_decl.load_specs.assert_called_once()

    def test_write_plan_calls_generate_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """write_plan() calls decl_api.generate_plan."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        mock_decl.generate_plan.assert_called_once()

    def test_write_plan_calls_serialize_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """write_plan() calls decl_api.serialize_plan."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.serialize_plan.return_value = '{"version": "1", "plan": {"ops": [], "warnings": []}, "summary": {}, "created_at": "", "target_database": "TEST_DB", "target_schema": "TEST_SCHEMA", "source_files": []}'
        out_path = str(tmp_path / "plan.json")
        mgr = FeatureManager()
        mgr.write_plan(
            input_files=["specs.yaml"],
            config=None,
            dev_mode=False,
            out_path=out_path,
        )
        mock_decl.serialize_plan.assert_called_once()


# ---------------------------------------------------------------------------
# apply with plan_file
# ---------------------------------------------------------------------------


class TestApplyWithPlanFile:
    def _write_plan_file(self, path: str) -> None:
        """Write a minimal plan JSON file for testing apply(plan_file=...)."""
        import json

        plan_data = {
            "version": "1",
            "created_at": "2024-01-01T00:00:00",
            "target_database": "TEST_DB",
            "target_schema": "TEST_SCHEMA",
            "source_files": ["specs.yaml"],
            "plan": {"ops": [], "warnings": []},
            "summary": {},
        }
        with open(path, "w") as f:
            json.dump(plan_data, f)

    def test_apply_with_plan_file_returns_dict(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply(plan_file=...) returns a dict."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=[],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )
        assert isinstance(result, dict)

    def test_apply_with_plan_file_calls_deserialize(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply(plan_file=...) calls decl_api.deserialize_plan with the file content."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=[],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )
        mock_decl.deserialize_plan.assert_called_once()

    def test_apply_with_plan_file_skips_load_specs(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply(plan_file=...) does not call load_specs (plan is pre-computed)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=[],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )
        mock_decl.load_specs.assert_not_called()

    def test_apply_with_plan_file_skips_generate_plan(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply(plan_file=...) does not call generate_plan (plan is pre-computed)."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        plan_path = str(tmp_path / "plan.json")
        self._write_plan_file(plan_path)

        plan_file_obj = mock_decl.deserialize_plan.return_value
        plan_file_obj.plan = mock_decl.generate_plan.return_value
        plan_file_obj.target_database = "TEST_DB"
        plan_file_obj.target_schema = "TEST_SCHEMA"

        mgr = FeatureManager()
        mgr.apply(
            input_files=[],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
            plan_file=plan_path,
        )
        mock_decl.generate_plan.assert_not_called()

    def test_apply_without_plan_file_still_works(self, mock_execute_query, mock_decl):
        """apply() with no plan_file uses the normal code path."""
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
        mock_decl.load_specs.assert_called_once()


class TestDirectoryMode:
    """Tests that apply() sets full_directory_mode based on directory vs file input."""

    def test_apply_with_directory_sets_full_directory_mode(
        self, mock_execute_query, mock_decl, tmp_path
    ):
        """apply() with a directory path passes full_directory_mode=True to generate_plan."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.apply(
            input_files=[str(tmp_path)],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        call_args = mock_decl.generate_plan.call_args
        assert call_args is not None
        options = (
            call_args[0][2] if len(call_args[0]) >= 3 else call_args[1].get("options")
        )
        assert options is not None
        assert options.full_directory_mode is True

    def test_apply_with_file_list_sets_full_directory_mode_false(
        self, mock_execute_query, mock_decl
    ):
        """apply() with individual file paths passes full_directory_mode=False to generate_plan."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.apply(
            input_files=["entities/user.yaml", "feature_views/click_fv.yaml"],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        call_args = mock_decl.generate_plan.call_args
        assert call_args is not None
        options = (
            call_args[0][2] if len(call_args[0]) >= 3 else call_args[1].get("options")
        )
        assert options is not None
        assert options.full_directory_mode is False

    def test_apply_with_single_file_sets_full_directory_mode_false(
        self, mock_execute_query, mock_decl
    ):
        """apply() with a single file path (not a directory) uses full_directory_mode=False."""
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
        call_args = mock_decl.generate_plan.call_args
        assert call_args is not None
        options = (
            call_args[0][2] if len(call_args[0]) >= 3 else call_args[1].get("options")
        )
        assert options is not None
        assert options.full_directory_mode is False
