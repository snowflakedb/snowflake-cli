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


@mock.patch(FEATURE_MANAGER)
def test_drop_requires_at_least_one_name(mock_manager, runner):
    """drop with no names should exit with usage error."""
    result = runner.invoke(["feature", "drop"])
    assert result.exit_code == 2, result.output


@mock.patch(FEATURE_MANAGER)
def test_drop_passes_names(mock_manager, runner):
    """drop ENTITY_A ENTITY_B should pass both names to FeatureManager.drop."""
    mock_manager.return_value.drop.return_value = {}
    result = runner.invoke(["feature", "drop", "ENTITY_A", "ENTITY_B"])
    assert result.exit_code == 0, result.output
    call_kwargs = mock_manager.return_value.drop.call_args[1]
    assert "ENTITY_A" in call_kwargs["names"]
    assert "ENTITY_B" in call_kwargs["names"]


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
    assert col_names["event_value"] == "FloatType"
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
    features = {f["name"]: f for f in data["features"]}
    assert "event_count_1h" in features
    assert features["event_count_1h"]["type"] == "IntegerType"
    assert "total_value_1h" in features
    assert features["total_value_1h"]["type"] == "FloatType"


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
def test_status_returns_parsed_json(mock_manager, runner):
    """status should call FeatureManager.get_status and return its result."""
    mock_manager.return_value.get_status.return_value = {
        "status": "RUNNING",
        "compute_pool": "active",
        "postgres": "active",
        "service": "active",
        "endpoints": [],
    }
    result = runner.invoke(["feature", "status"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.get_status.assert_called_once()


# ---------------------------------------------------------------------------
# initialize-service
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_initialize_service_already_running_is_noop(mock_manager, runner):
    """initialize-service should be a no-op when status is already RUNNING."""
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service already initialized",
    }
    result = runner.invoke(["feature", "initialize-service"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.initialize_service.assert_called_once()


@mock.patch(FEATURE_MANAGER)
def test_initialize_service_creates_and_polls(mock_manager, runner):
    """initialize-service should create the runtime and poll until RUNNING."""
    mock_manager.return_value.initialize_service.return_value = {
        "status": "RUNNING",
        "message": "Service initialized successfully",
    }
    result = runner.invoke(["feature", "initialize-service"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.initialize_service.assert_called_once()


# ---------------------------------------------------------------------------
# destroy-service
# ---------------------------------------------------------------------------


@mock.patch(FEATURE_MANAGER)
def test_destroy_service_drops_ofts_then_runtime(mock_manager, runner):
    """destroy-service should drop OFTs then call FeatureManager.destroy_service."""
    mock_manager.return_value.destroy_service.return_value = {
        "status": "destroyed",
        "dropped_ofts": ["TABLE_A", "TABLE_B"],
    }
    result = runner.invoke(["feature", "destroy-service"])
    assert result.exit_code == 0, result.output
    mock_manager.return_value.destroy_service.assert_called_once()
