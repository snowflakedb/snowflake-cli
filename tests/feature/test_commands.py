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

"""Tests for 'snow feature' CLI commands."""

from unittest import mock

FEATURE_MANAGER = "snowflake.cli._plugins.feature.commands.FeatureManager"


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_apply_requires_at_least_one_file(mock_manager, runner):
    """apply with no files should exit with a usage error (code 2)."""
    result = runner.invoke(["feature", "apply"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_apply_single_file(mock_manager, runner):
    """apply with one file should call FeatureManager.apply."""
    mock_manager.return_value.apply.return_value = {"status": "ok"}
    result = runner.invoke(["feature", "apply", "my_specs.yaml"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.apply.assert_called_once()
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert "my_specs.yaml" in call_kwargs["input_files"]


@mock.patch(FEATURE_MANAGER)
def test_apply_dry_flag(mock_manager, runner):
    """apply --dry should pass dry_run=True to FeatureManager.apply."""
    mock_manager.return_value.apply.return_value = {"status": "dry"}
    result = runner.invoke(["feature", "apply", "specs.yaml", "--dry"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["dry_run"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_dev_flag(mock_manager, runner):
    """apply --dev should pass dev_mode=True."""
    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "apply", "specs.yaml", "--dev"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["dev_mode"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_overwrite_flag(mock_manager, runner):
    """apply --overwrite should pass overwrite=True."""
    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "apply", "specs.yaml", "--overwrite"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["overwrite"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_allow_recreate_flag(mock_manager, runner):
    """apply --allow-recreate should pass allow_recreate=True."""
    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "apply", "specs.yaml", "--allow-recreate"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["allow_recreate"] is True


@mock.patch(FEATURE_MANAGER)
def test_apply_help_shows_all_options(mock_manager, runner):
    """apply --help must show all documented flags."""
    result = runner.invoke(["feature", "apply", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--dry" in output
    assert "--dev" in output
    assert "--overwrite" in output
    assert "--allow-recreate" in output
    assert "--config" in output
    assert "--verbose" in output


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_plan_requires_at_least_one_file(mock_manager, runner):
    """plan with no files should exit with a usage error."""
    result = runner.invoke(["feature", "plan"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_plan_calls_apply_with_dry_run(mock_manager, runner):
    """plan should delegate to FeatureManager.apply(dry_run=True)."""
    mock_manager.return_value.apply.return_value = {}
    result = runner.invoke(["feature", "plan", "specs.yaml"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.apply.call_args[1]
    assert call_kwargs["dry_run"] is True


@mock.patch(FEATURE_MANAGER)
def test_plan_help_does_not_show_overwrite(mock_manager, runner):
    """plan --help must NOT show --overwrite or --allow-recreate."""
    result = runner.invoke(["feature", "plan", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--overwrite" not in output
    assert "--allow-recreate" not in output


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_list_no_files_lists_deployed(mock_manager, runner):
    """list with no file args should call list_specs with empty file list."""
    mock_manager.return_value.list_specs.return_value = {}
    result = runner.invoke(["feature", "list"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.list_specs.call_args[1]
    assert call_kwargs["input_files"] == ()


@mock.patch(FEATURE_MANAGER)
def test_list_with_file_passes_files(mock_manager, runner):
    """list with a file arg should pass that file to list_specs."""
    mock_manager.return_value.list_specs.return_value = {}
    result = runner.invoke(["feature", "list", "my_specs.yaml"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.list_specs.call_args[1]
    assert "my_specs.yaml" in call_kwargs["input_files"]


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_describe_requires_name(mock_manager, runner):
    """describe with no name should exit with usage error."""
    result = runner.invoke(["feature", "describe"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_describe_passes_name(mock_manager, runner):
    """describe MY_ENTITY should call FeatureManager.describe(name='MY_ENTITY')."""
    mock_manager.return_value.describe.return_value = {}
    result = runner.invoke(["feature", "describe", "MY_ENTITY"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.describe.call_args[1]
    assert call_kwargs["name"] == "MY_ENTITY"


# ---------------------------------------------------------------------------
# drop
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# convert
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_convert_requires_files(mock_manager, runner):
    """convert with no files should exit with usage error."""
    result = runner.invoke(["feature", "convert", "--file-format", "yaml"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_convert_requires_format(mock_manager, runner):
    """convert without --file-format should exit with usage error."""
    result = runner.invoke(["feature", "convert", "specs.py"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_convert_yaml_format(mock_manager, runner):
    """convert --file-format yaml should call FeatureManager.convert."""
    mock_manager.return_value.convert.return_value = {}
    result = runner.invoke(["feature", "convert", "specs.py", "--file-format", "yaml"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.convert.call_args[1]
    assert call_kwargs["file_format"] == "yaml"


@mock.patch(FEATURE_MANAGER)
def test_convert_json_format(mock_manager, runner):
    """convert --file-format json should call FeatureManager.convert."""
    mock_manager.return_value.convert.return_value = {}
    result = runner.invoke(["feature", "convert", "specs.py", "--file-format", "json"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.convert.call_args[1]
    assert call_kwargs["file_format"] == "json"


@mock.patch(FEATURE_MANAGER)
def test_convert_invalid_format(mock_manager, runner):
    """convert with an invalid --file-format should exit with usage error."""
    result = runner.invoke(["feature", "convert", "specs.py", "--file-format", "xml"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_convert_recursive_flag(mock_manager, runner):
    """convert -R should pass recursive=True to FeatureManager.convert."""
    mock_manager.return_value.convert.return_value = {}
    result = runner.invoke(
        ["feature", "convert", "specs.py", "--file-format", "yaml", "-R"]
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.convert.call_args[1]
    assert call_kwargs["recursive"] is True


# ---------------------------------------------------------------------------
# example
# ---------------------------------------------------------------------------

EXPECTED_FILES = [
    "entities/example_entity.yaml",
    "datasources/example_events_source.yaml",
    "feature_views/example_feature_view.yaml",
    "feature_views/example_udf.py",
]


def test_example_creates_files(runner, tmp_path):
    """example --dir <path> should create 3 YAML files in subdirectories."""
    result = runner.invoke(["feature", "example", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    for rel_path in EXPECTED_FILES:
        assert (tmp_path / rel_path).exists(), f"Missing: {rel_path}"


def test_example_entity_content(runner, tmp_path):
    """The generated entity YAML should contain correct spec fields."""
    import yaml

    runner.invoke(["feature", "example", "--dir", str(tmp_path)])
    data = yaml.safe_load((tmp_path / "entities/example_entity.yaml").read_text())
    assert data["kind"] == "Entity"
    assert data["name"] == "user"
    join_keys = data["join_keys"]
    assert len(join_keys) == 1
    assert join_keys[0]["name"] == "user_id"
    assert join_keys[0]["type"] == "StringType"


def test_example_source_content(runner, tmp_path):
    """The generated source YAML should contain correct spec fields."""
    import yaml

    runner.invoke(["feature", "example", "--dir", str(tmp_path)])
    data = yaml.safe_load(
        (tmp_path / "datasources/example_events_source.yaml").read_text()
    )
    assert data["kind"] == "StreamingSource"
    assert data["name"] == "user_events"
    assert data["type"] == "REST"
    col_names = {c["name"]: c["type"] for c in data["columns"]}
    assert col_names["user_id"] == "StringType"
    assert col_names["event_type"] == "StringType"
    assert col_names["event_value"] == "DoubleType"
    assert col_names["timestamp"] == "TimestampType"


def test_example_feature_view_content(runner, tmp_path):
    """The generated feature view YAML should contain correct spec fields."""
    import yaml

    runner.invoke(["feature", "example", "--dir", str(tmp_path)])
    data = yaml.safe_load(
        (tmp_path / "feature_views/example_feature_view.yaml").read_text()
    )
    assert data["kind"] == "StreamingFeatureView"
    assert data["name"] == "user_event_features"
    assert data["online"] is True
    assert data["timestamp_field"] == "timestamp"
    assert data["feature_granularity"] == "5m"
    assert data["ordered_entity_column_names"] == ["user_id"]
    sources = data["sources"]
    assert len(sources) == 1
    assert sources[0]["name"] == "user_events"
    assert sources[0]["source_type"] == "Stream"
    features = {f["output_column"]["name"]: f for f in data["features"]}
    assert "event_count_1h" in features
    assert features["event_count_1h"]["output_column"]["type"] == "LongType"
    assert "total_value_1h" in features
    assert features["total_value_1h"]["output_column"]["type"] == "DoubleType"
    # Verify UDF section is present
    assert "udf" in data
    assert data["udf"]["engine"] == "pandas"
    assert data["udf"]["file"].endswith(".py")


def test_example_default_dir(runner, tmp_path, monkeypatch):
    """example without --dir should write files into the current directory."""

    monkeypatch.chdir(tmp_path)
    result = runner.invoke(["feature", "example"])
    assert result.exit_code == 0, result.output
    for rel_path in EXPECTED_FILES:
        assert (tmp_path / rel_path).exists(), f"Missing: {rel_path}"


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_online_service_no_flags_returns_status(mock_manager, runner):
    """online-service with no flags should show runtime status."""
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "compute_pool": "active",
        "postgres": "active",
        "service": "active",
        "endpoints": [],
    }
    result = runner.invoke(["feature", "online-service"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.get_status.assert_called_once()


# ---------------------------------------------------------------------------
# online-service
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_already_running_is_noop(mock_manager, runner):
    """online-service --create should be a no-op when status is already RUNNING."""
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service already initialized",
    }
    result = runner.invoke(["feature", "online-service", "--create"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.initialize_service.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_online_service_create_and_polls(mock_manager, runner):
    """online-service --create should create the runtime and poll until RUNNING."""
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service initialized successfully",
    }
    result = runner.invoke(["feature", "online-service", "--create"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.initialize_service.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_online_service_drop(mock_manager, runner):
    """online-service --drop should drop OFTs then call FeatureManager.destroy_service."""
    mock_manager.return_value.destroy_service.return_value = {
        "status": "destroyed",
        "dropped_ofts": ["TABLE_A", "TABLE_B"],
    }
    result = runner.invoke(["feature", "online-service", "--drop"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.destroy_service.assert_called_once()


def test_online_service_both_flags_rejected(runner):
    """online-service with both --create and --drop should fail."""
    result = runner.invoke(["feature", "online-service", "--create", "--drop"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_ingest_requires_source_name(mock_manager, runner):
    """ingest with no arguments should exit with usage error (code 2)."""
    result = runner.invoke(["feature", "ingest"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_ingest_reads_data_from_file(mock_manager, runner, tmp_path):
    """ingest --data <file> should parse JSON and pass records to manager."""
    data_file = tmp_path / "records.json"
    data_file.write_text('[{"user_id": "u1", "val": 42}]')
    mock_manager.return_value.ingest.return_value = {"ingested": 1}
    result = runner.invoke(["feature", "ingest", "my_source", "--data", str(data_file)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.ingest.assert_called_once()
    call_kwargs = mock_manager.return_value.ingest.call_args[1]
    assert call_kwargs["source_name"] == "my_source"
    assert call_kwargs["records"] == [{"user_id": "u1", "val": 42}]


@mock.patch(FEATURE_MANAGER)
def test_ingest_reads_from_stdin(mock_manager, runner):
    """ingest without --data (defaults to stdin) should read records from stdin."""
    mock_manager.return_value.ingest.return_value = {"ingested": 2}
    result = runner.invoke(
        ["feature", "ingest", "my_source"],
        input='[{"a": 1}, {"a": 2}]',
    )
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.ingest.call_args[1]
    assert len(call_kwargs["records"]) == 2


@mock.patch(FEATURE_MANAGER)
def test_ingest_manager_error_propagates(mock_manager, runner):
    """ingest should propagate RuntimeError from manager (e.g. missing PAT)."""
    mock_manager.return_value.ingest.side_effect = RuntimeError(
        "SNOWFLAKE_PAT environment variable is required"
    )
    result = runner.invoke(
        ["feature", "ingest", "my_source"],
        input="[]",
    )
    assert result.exit_code != 0


@mock.patch(FEATURE_MANAGER)
def test_ingest_help_shows_data_option(mock_manager, runner):
    """ingest --help should show --data option."""
    result = runner.invoke(["feature", "ingest", "--help"])
    assert result.exit_code == 0, result.output
    assert "--data" in result.output


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_query_requires_feature_view_name(mock_manager, runner):
    """query with no arguments should exit with usage error (code 2)."""
    result = runner.invoke(["feature", "query"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_query_requires_keys(mock_manager, runner):
    """query without --keys should exit with usage error (code 2)."""
    result = runner.invoke(["feature", "query", "my_view"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_query_calls_manager_with_view_and_keys(mock_manager, runner):
    """query should pass feature_view_name and parsed keys to manager."""
    mock_manager.return_value.query.return_value = {"results": []}
    keys_json = '[{"user_id": "u1"}]'
    result = runner.invoke(["feature", "query", "my_view", "--keys", keys_json])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.query.assert_called_once()
    call_kwargs = mock_manager.return_value.query.call_args[1]
    assert call_kwargs["feature_view_name"] == "my_view"
    assert call_kwargs["keys"] == [{"user_id": "u1"}]


@mock.patch(FEATURE_MANAGER)
def test_query_manager_error_propagates(mock_manager, runner):
    """query should propagate RuntimeError from manager (e.g. missing PAT)."""
    mock_manager.return_value.query.side_effect = RuntimeError(
        "SNOWFLAKE_PAT environment variable is required"
    )
    result = runner.invoke(["feature", "query", "my_view", "--keys", '[{"id": "1"}]'])
    assert result.exit_code != 0


@mock.patch(FEATURE_MANAGER)
def test_query_help_shows_keys_option(mock_manager, runner):
    """query --help should show --keys option."""
    result = runner.invoke(["feature", "query", "--help"])
    assert result.exit_code == 0, result.output
    assert "--keys" in result.output


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_init_help_shows_command(mock_manager, runner):
    """init --help should show the init command with --no-scaffold option."""
    result = runner.invoke(["feature", "init", "--help"])
    assert result.exit_code == 0, result.output
    output = result.output.lower()
    assert "--no-scaffold" in output


@mock.patch(FEATURE_MANAGER)
def test_init_calls_manager_init(mock_manager, runner):
    """init should call FeatureManager.init()."""
    mock_manager.return_value.init.return_value = {
        "status": "initialized",
        "database": "DB",
        "schema": "SCH",
        "directories": ["entities", "datasources", "feature_views"],
    }
    result = runner.invoke(["feature", "init"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.init.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_init_no_scaffold_flag(mock_manager, runner):
    """init --no-scaffold should pass no_scaffold=True to FeatureManager.init."""
    mock_manager.return_value.init.return_value = {
        "status": "initialized",
        "database": "DB",
        "schema": "SCH",
        "directories": [],
    }
    result = runner.invoke(["feature", "init", "--no-scaffold"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.init.call_args[1]
    assert call_kwargs["no_scaffold"] is True


@mock.patch(FEATURE_MANAGER)
def test_init_default_no_scaffold_is_false(mock_manager, runner):
    """init without --no-scaffold should pass no_scaffold=False."""
    mock_manager.return_value.init.return_value = {
        "status": "initialized",
        "database": "DB",
        "schema": "SCH",
        "directories": ["entities", "datasources", "feature_views"],
    }
    result = runner.invoke(["feature", "init"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.init.call_args[1]
    assert call_kwargs["no_scaffold"] is False


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_export_calls_manager(mock_manager, runner, tmp_path):
    """export should call FeatureManager.export_specs with the given dir."""
    mock_manager.return_value.export_specs.return_value = {
        "status": "exported",
        "directory": str(tmp_path),
        "files": [str(tmp_path / "feature_views/my_fv.yaml")],
    }
    result = runner.invoke(["feature", "export", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.export_specs.assert_called_once_with(str(tmp_path))


@mock.patch(FEATURE_MANAGER)
def test_export_default_dir(mock_manager, runner):
    """export without --dir should call export_specs with '.'."""
    mock_manager.return_value.export_specs.return_value = {
        "status": "exported",
        "directory": ".",
        "files": [],
    }
    result = runner.invoke(["feature", "export"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.export_specs.assert_called_once_with(".")


@mock.patch(FEATURE_MANAGER)
def test_export_returns_file_list(mock_manager, runner, tmp_path):
    """export should render the list of written files."""
    files = [
        str(tmp_path / "feature_views/my_fv.yaml"),
        str(tmp_path / "entities/user_id.yaml"),
        str(tmp_path / "datasources/click_events.yaml"),
    ]
    mock_manager.return_value.export_specs.return_value = {
        "status": "exported",
        "directory": str(tmp_path),
        "files": files,
    }
    result = runner.invoke(["feature", "export", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "my_fv.yaml" in result.output
