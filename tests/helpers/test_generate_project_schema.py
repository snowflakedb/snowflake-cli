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

import pytest

COMMAND = "generate-project-schema"


def _extract_json(text: str) -> dict:
    """Find the first top-level JSON object in the CLI output and parse it."""
    start = text.find("{")
    assert start != -1, f"No JSON object found in output: {text!r}"
    depth = 0
    for idx in range(start, len(text)):
        ch = text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return json.loads(text[start : idx + 1])
    raise AssertionError(f"Unbalanced JSON object in output: {text!r}")


def test_default_version_is_v2(runner):
    result = runner.invoke(["helpers", COMMAND])
    assert result.exit_code == 0, result.output
    schema = _extract_json(result.output)
    assert schema["title"] == "Snowflake CLI project definition v2"
    assert "entities" in schema["properties"]
    assert "mixins" in schema["properties"]


@pytest.mark.parametrize(
    "version, expected_title, expected_top_level",
    [
        (
            "1",
            "Snowflake CLI project definition v1",
            {"native_app", "snowpark", "streamlit"},
        ),
        (
            "1.1",
            "Snowflake CLI project definition v1.1",
            {"native_app", "snowpark", "streamlit", "env"},
        ),
        ("2", "Snowflake CLI project definition v2", {"entities", "env", "mixins"}),
    ],
)
def test_supported_versions(runner, version, expected_title, expected_top_level):
    result = runner.invoke(["helpers", COMMAND, "--version", version])
    assert result.exit_code == 0, result.output
    schema = _extract_json(result.output)
    assert schema["title"] == expected_title
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["$id"].endswith(
        f"project_definition_v{version.replace('.', '_')}.json"
    )
    assert schema["type"] == "object"
    assert "definition_version" in schema["properties"]
    assert expected_top_level.issubset(set(schema["properties"].keys()))


def test_output_file_writes_schema(runner, tmp_path):
    out = tmp_path / "schema.json"
    result = runner.invoke(
        ["helpers", COMMAND, "--version", "2", "--output-file", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert f"Project definition schema written to {out}" in result.output
    assert out.exists()

    payload = json.loads(out.read_text())
    assert payload["title"] == "Snowflake CLI project definition v2"
    assert "entities" in payload["properties"]


def test_unsupported_version_fails(runner):
    result = runner.invoke(["helpers", COMMAND, "--version", "999"])
    assert result.exit_code != 0
    assert "Unsupported project definition version '999'" in result.output


def test_output_short_option(runner, tmp_path):
    out = tmp_path / "schema-v11.json"
    result = runner.invoke(["helpers", COMMAND, "--version", "1.1", "-o", str(out)])
    assert result.exit_code == 0, result.output
    payload = json.loads(out.read_text())
    assert payload["title"] == "Snowflake CLI project definition v1.1"
    assert "env" in payload["properties"]


def test_schema_is_stable_json(runner):
    """Running the command twice should produce byte-identical JSON."""
    first = runner.invoke(["helpers", COMMAND, "--version", "2"])
    second = runner.invoke(["helpers", COMMAND, "--version", "2"])
    assert first.exit_code == 0
    assert second.exit_code == 0
    assert _extract_json(first.output) == _extract_json(second.output)


def test_schema_validates_simple_v2_project(runner):
    """Minimal v2 project definition should validate against the generated schema."""
    jsonschema = pytest.importorskip("jsonschema")

    result = runner.invoke(["helpers", COMMAND, "--version", "2"])
    assert result.exit_code == 0
    schema = _extract_json(result.output)

    sample = {
        "definition_version": "2",
        "entities": {
            "hello": {
                "type": "streamlit",
                "identifier": "hello",
                "stage": "streamlit",
                "query_warehouse": "xsmall",
                "main_file": "app.py",
                "artifacts": ["app.py"],
            }
        },
    }

    try:
        jsonschema.validate(instance=sample, schema=schema)
    except jsonschema.ValidationError as err:  # pragma: no cover - aid debugging
        pytest.fail(f"Valid sample failed schema validation: {err}")
