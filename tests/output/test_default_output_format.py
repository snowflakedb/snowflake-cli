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

import json
from textwrap import dedent

import pytest
from snowflake.cli.api.output.formats import OutputFormat

_TABLE_MARKERS = ("| string | number |",)
_JSON_OBJECT_KEYS = {"string", "number", "array", "object", "date"}


def _config_with_output_format(output_format: str | None) -> str:
    config = dedent(
        """\
        [connections.default]
        password = "dummy"
        """
    )
    if output_format is not None:
        config += f"\n[cli]\noutput_format = {output_format}\n"
    return config


def _invoke_faker(
    runner, config_file, monkeypatch, config_value, *command_args, env=None
):
    monkeypatch.delenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", raising=False)
    if env is not None:
        monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", env)
    with config_file(_config_with_output_format(config_value)) as cfg:
        return runner.invoke_with_config_file(cfg, ["Faker", *command_args])


@pytest.mark.parametrize(
    "value, expected",
    [
        ("json", OutputFormat.JSON),
        (" JSON ", OutputFormat.JSON),
        ("csv", OutputFormat.CSV),
        ("JSON_EXT", OutputFormat.JSON_EXT),
        ("table", OutputFormat.TABLE),
    ],
)
def test_output_format_from_string_accepts_known_values(value, expected):
    assert OutputFormat.from_string(value) is expected


def test_output_format_from_string_rejects_unknown_values():
    with pytest.raises(ValueError, match="Must be one of: TABLE, JSON, JSON_EXT, CSV"):
        OutputFormat.from_string("xml")


@pytest.mark.usefixtures("faker_app")
def test_default_output_format_is_table_without_config(
    runner, config_file, monkeypatch
):
    result = _invoke_faker(runner, config_file, monkeypatch, None)
    assert result.exit_code == 0, result.output
    assert all(marker in result.output for marker in _TABLE_MARKERS)


@pytest.mark.usefixtures("faker_app")
@pytest.mark.parametrize("config_value", ['"JSON"', '"json"'])
def test_config_output_format_is_used_when_flag_omitted(
    runner, config_file, monkeypatch, config_value
):
    result = _invoke_faker(runner, config_file, monkeypatch, config_value)
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert isinstance(payload, list)
    assert payload and set(payload[0]) == _JSON_OBJECT_KEYS


@pytest.mark.usefixtures("faker_app")
def test_format_flag_overrides_config(runner, config_file, monkeypatch):
    result = _invoke_faker(
        runner, config_file, monkeypatch, '"JSON"', "--format", "TABLE"
    )
    assert result.exit_code == 0, result.output
    assert all(marker in result.output for marker in _TABLE_MARKERS)


@pytest.mark.usefixtures("faker_app")
def test_env_output_format_is_used_when_flag_omitted(runner, config_file, monkeypatch):
    result = _invoke_faker(runner, config_file, monkeypatch, None, env="CSV")
    assert result.exit_code == 0, result.output
    assert result.output.startswith("string,number,array,object,date\n")
    assert "| string |" not in result.output


@pytest.mark.usefixtures("faker_app")
def test_env_overrides_config_output_format(runner, config_file, monkeypatch):
    result = _invoke_faker(runner, config_file, monkeypatch, '"TABLE"', env="JSON")
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert isinstance(payload, list)


@pytest.mark.usefixtures("faker_app")
def test_empty_env_is_unset_and_does_not_override_config(
    runner, config_file, monkeypatch
):
    result = _invoke_faker(runner, config_file, monkeypatch, '"JSON"', env="")
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert isinstance(payload, list)
    assert payload and set(payload[0]) == _JSON_OBJECT_KEYS


@pytest.mark.usefixtures("faker_app")
def test_config_json_ext_output_format_is_used_when_flag_omitted(
    runner, config_file, monkeypatch
):
    result = _invoke_faker(runner, config_file, monkeypatch, '"JSON_EXT"')
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert isinstance(payload, list)
    assert payload and set(payload[0]) == _JSON_OBJECT_KEYS


@pytest.mark.usefixtures("faker_app")
def test_format_flag_overrides_env_output_format(runner, config_file, monkeypatch):
    result = _invoke_faker(
        runner, config_file, monkeypatch, None, "--format", "TABLE", env="JSON"
    )
    assert result.exit_code == 0, result.output
    assert all(marker in result.output for marker in _TABLE_MARKERS)


@pytest.mark.usefixtures("faker_app")
def test_invalid_config_output_format_is_rejected(runner, config_file, monkeypatch):
    result = _invoke_faker(runner, config_file, monkeypatch, '"XML"')
    assert result.exit_code != 0
    assert "Invalid output format: 'XML'" in result.output
    assert "TABLE, JSON, JSON_EXT, CSV" in result.output


@pytest.mark.usefixtures("faker_app")
def test_invalid_config_output_format_uses_argument_exit_code(
    runner, config_file, monkeypatch
):
    result = _invoke_faker(
        runner, config_file, monkeypatch, '"XML"', "--enhanced-exit-codes"
    )
    assert result.exit_code == 2, result.output


@pytest.mark.usefixtures("faker_app")
def test_non_string_config_output_format_is_rejected(runner, config_file, monkeypatch):
    result = _invoke_faker(runner, config_file, monkeypatch, "true")
    assert result.exit_code != 0
    assert "Invalid value for cli.output_format" in result.output
