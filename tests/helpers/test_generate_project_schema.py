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
    "version, expected_title, expected_top_level, pinned_version",
    [
        (
            "1",
            "Snowflake CLI project definition v1",
            {"native_app", "snowpark", "streamlit"},
            ["1", 1],
        ),
        (
            "1.1",
            "Snowflake CLI project definition v1.1",
            {"native_app", "snowpark", "streamlit", "env"},
            ["1.1", 1.1],
        ),
        (
            "2",
            "Snowflake CLI project definition v2",
            {"entities", "env", "mixins"},
            ["2", 2],
        ),
    ],
)
def test_supported_versions(
    runner, version, expected_title, expected_top_level, pinned_version
):
    result = runner.invoke(["helpers", COMMAND, "--definition-version", version])
    assert result.exit_code == 0, result.output
    schema = _extract_json(result.output)

    assert schema["title"] == expected_title
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["type"] == "object"
    # The dead "$id" pointing at a non-existent URL was removed; assert it stays gone.
    assert "$id" not in schema

    # Each schema must self-identify its version, otherwise the v2 schema would
    # happily accept `definition_version: 1`. Both YAML scalar forms are pinned.
    definition_version = schema["properties"]["definition_version"]
    assert definition_version["enum"] == pinned_version
    assert "definition_version" in schema["required"]

    # extra="forbid" on the models is what powers the "catch typos" promise.
    assert schema["additionalProperties"] is False

    assert expected_top_level.issubset(set(schema["properties"].keys()))


def test_removed_version_flag_is_rejected(runner):
    """The option is --definition-version; the old --version must not resurface."""
    result = runner.invoke(["helpers", COMMAND, "--version", "2"])
    assert result.exit_code != 0
    assert "No such option: --version" in result.output


def test_unsupported_version_fails(runner):
    result = runner.invoke(["helpers", COMMAND, "--definition-version", "999"])
    assert result.exit_code != 0
    # typer renders the allowed values from the version enum.
    assert "is not one of" in result.output
    assert "999" in result.output


def test_output_file_writes_schema(runner, tmp_path):
    out = tmp_path / "schema.json"
    result = runner.invoke(
        ["helpers", COMMAND, "--definition-version", "2", "--output-file", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert f"Project definition schema written to {out}" in result.output
    assert out.exists()

    text = out.read_text()
    assert text.endswith("\n")
    payload = json.loads(text)
    assert payload["title"] == "Snowflake CLI project definition v2"
    assert "entities" in payload["properties"]
    assert "$id" not in payload


def test_output_short_option(runner, tmp_path):
    out = tmp_path / "schema-v11.json"
    result = runner.invoke(
        ["helpers", COMMAND, "--definition-version", "1.1", "-o", str(out)]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(out.read_text())
    assert payload["title"] == "Snowflake CLI project definition v1.1"
    assert "env" in payload["properties"]


def test_output_file_missing_parent_raises(runner, tmp_path):
    """Writing to a path under a nonexistent directory fails with a clean
    ClickException rather than an unhandled FileNotFoundError."""
    out = tmp_path / "nonexistent_dir" / "schema.json"
    result = runner.invoke(
        ["helpers", COMMAND, "--definition-version", "2", "-o", str(out)]
    )
    assert result.exit_code != 0
    # The error panel word-wraps the long temp path, so assert on the stable
    # tail of the message instead of the wrapped directory name.
    assert "does not exist" in result.output
    assert not out.exists()


def test_schema_is_stable(runner, tmp_path):
    """Generation is deterministic: two runs produce byte-identical files."""
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    for out in (first, second):
        result = runner.invoke(
            ["helpers", COMMAND, "--definition-version", "2", "-o", str(out)]
        )
        assert result.exit_code == 0, result.output
    assert first.read_bytes() == second.read_bytes()


def test_json_format_emits_schema_object(runner):
    """Under --format json the schema is the document, not a stringified blob
    nested under a "message" key."""
    result = runner.invoke(
        ["helpers", COMMAND, "--definition-version", "2", "--format", "json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert "message" not in payload
    assert payload["title"] == "Snowflake CLI project definition v2"
    assert "$defs" in payload
    assert "entities" in payload["properties"]


def test_entities_forbid_unknown_keys(runner):
    """Structural guard for the "catch typos" promise that does not need a
    JSON Schema validator installed: entity models reject unknown keys."""
    result = runner.invoke(["helpers", COMMAND, "--definition-version", "2"])
    assert result.exit_code == 0, result.output
    schema = _extract_json(result.output)
    streamlit = schema["$defs"]["StreamlitEntityModel"]
    assert streamlit["additionalProperties"] is False


def _valid_v2_project() -> dict:
    return {
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


def test_schema_validates_valid_v2_project(runner):
    """A minimal valid v2 project validates against the generated schema."""
    jsonschema = pytest.importorskip("jsonschema")

    result = runner.invoke(["helpers", COMMAND, "--definition-version", "2"])
    assert result.exit_code == 0
    schema = _extract_json(result.output)

    try:
        jsonschema.validate(instance=_valid_v2_project(), schema=schema)
    except jsonschema.ValidationError as err:  # pragma: no cover - aid debugging
        pytest.fail(f"Valid sample failed schema validation: {err}")


@pytest.mark.parametrize(
    "mutate, reason",
    [
        (
            lambda d: d["entities"]["hello"].__setitem__("bogus_field", 1),
            "unknown entity key (typo)",
        ),
        (
            lambda d: d.__setitem__("definition_version", "1"),
            "wrong definition_version for this schema",
        ),
        (
            lambda d: d.__setitem__("native_app", {"name": "x"}),
            "v1-only top-level key",
        ),
    ],
)
def test_schema_rejects_invalid_v2_projects(runner, mutate, reason):
    """The schema is only useful if it rejects the mistakes it advertises:
    typos, the wrong definition_version, and v1-shaped keys."""
    jsonschema = pytest.importorskip("jsonschema")

    result = runner.invoke(["helpers", COMMAND, "--definition-version", "2"])
    assert result.exit_code == 0
    schema = _extract_json(result.output)

    sample = _valid_v2_project()
    mutate(sample)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance=sample, schema=schema)
