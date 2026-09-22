# Copyright (c) 2026 Snowflake Inc.
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

"""Integration tests for the `snow bundle` command group (code bundles).

These tests only use the `runner` fixture (no `snowflake_session`), so they work
against any account the `integration` connection points at, regardless of the
authenticator configured for it.

Objects are created in a throwaway schema inside the connection's database and
addressed with fully qualified names, because every `runner.invoke_*` call opens
its own session — `USE SCHEMA` does not carry over between invocations.

The JVM (`language: java` / `language: scala`) tests execute a real jar. Those
jars live in `tests_integration/test_data/code_bundle_jvm/`, together with their
sources and rebuild instructions; see the README there.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import List, Optional

import pytest

# `EXECUTE CODE BUNDLE` spins up a Spark runtime on the warehouse; a successful
# run took ~70s when these tests were written.
EXECUTION_TIMEOUT_SECONDS = 420
EXECUTION_POLL_SECONDS = 10
TERMINAL_STATUSES = {
    "SUCCESS",
    "FAILED_WITH_ERROR",
    "FAILED_WITH_INCIDENT",
    "ABORTING",
    "ABORTED",
    "DISCONNECTED",
    "RESTARTED",
    "BLOCKED",
}

# Minimal bundle that `EXECUTE CODE BUNDLE` accepts: the server requires a
# `code_bundle.yml` (or .yaml) at the root of the bundle, and rejects the
# execution outright when it is missing.
CODE_BUNDLE_YML = """\
bundle:
  type: spark
  compute_type: warehouse
  language: python
  compute_options:
    runtime_version: "1.29"
"""

ENTRYPOINT_PY = """\
import sys

from pyspark.sql import SparkSession


def main() -> None:
    table_name = sys.argv[1] if len(sys.argv) > 1 else "cli_it_bundle_result"
    spark = SparkSession.builder.getOrCreate()
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {table_name} (result STRING)")
        spark.sql(f"INSERT INTO {table_name} VALUES ('CLI_IT_OK')")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
"""


def _sql(runner, query: str):
    result = runner.invoke_with_connection_json(["sql", "-q", query])
    assert result.exit_code == 0, result.output
    return result.json


@pytest.fixture
def bundle_schema(runner):
    """Throwaway schema in the connection's database, dropped afterwards."""
    database = _sql(runner, "select current_database() as db")[0]["DB"]
    assert database, "the integration connection must have a database set"
    schema = f"cli_it_bundle_{uuid.uuid4().hex[:12]}"
    _sql(runner, f"create schema {database}.{schema}")
    try:
        yield f"{database}.{schema}"
    finally:
        _sql(runner, f"drop schema if exists {database}.{schema}")


@pytest.fixture
def local_bundle_source(tmp_path) -> Path:
    """Local directory laid out like a real (executable) code bundle project."""
    source = tmp_path / "project"
    (source / "src").mkdir(parents=True)
    (source / "venv" / "lib").mkdir(parents=True)

    (source / "code_bundle.yml").write_text(CODE_BUNDLE_YML)
    (source / "main.py").write_text(ENTRYPOINT_PY)
    (source / "src" / "helper.py").write_text("VALUE = 42\n")
    # Excluded by the --exclude patterns used in the tests below.
    (source / "src" / "helper.pyc").write_text("compiled\n")
    (source / "venv" / "lib" / "package.py").write_text("vendored\n")
    return source


def _version_files(runner, bundle_fqn: str, version: str = "version$1") -> List[str]:
    """File names stored in a bundle version, relative to the version root."""
    prefix = f"/versions/{version}/"
    rows = _sql(runner, f"ls 'snow://code bundle/{bundle_fqn}/versions/{version}/'")
    return sorted(row["name"].replace(prefix, "", 1) for row in rows)


def _error_text(output: str) -> str:
    """Flatten a CLI error box: it wraps at 80 columns, breaking messages up."""
    return " ".join(output.replace("│", " ").split())


def _describe(runner, bundle_fqn: str) -> dict:
    return _sql(runner, f"describe code bundle {bundle_fqn}")[0]


def _query_id_from_async_output(output: str) -> str:
    marker = "Query ID: "
    assert marker in output, output
    return output.split(marker, 1)[1].strip().splitlines()[0]


def _history(runner, extra_args: Optional[List[str]] = None) -> List[dict]:
    result = runner.invoke_with_connection_json(
        ["bundle", "history", *(extra_args or [])]
    )
    assert result.exit_code == 0, result.output
    return result.json


def _wait_for_terminal_status(runner, query_id: str) -> str:
    deadline = time.monotonic() + EXECUTION_TIMEOUT_SECONDS
    status = "UNKNOWN"
    while time.monotonic() < deadline:
        result = runner.invoke_with_connection(["bundle", "status", query_id])
        assert result.exit_code == 0, result.output
        assert f"Query {query_id}: " in result.output, result.output
        status = result.output.split(f"Query {query_id}: ", 1)[1].strip()
        if status in TERMINAL_STATUSES:
            return status
        time.sleep(EXECUTION_POLL_SECONDS)
    raise AssertionError(
        f"execution {query_id} did not reach a terminal status within "
        f"{EXECUTION_TIMEOUT_SECONDS}s (last status: {status})"
    )


@pytest.mark.integration
def test_create_from_local_source_uploads_tree_and_applies_exclude(
    runner, bundle_schema, local_bundle_source
):
    """`create` with a local --source uploads the tree, honouring --exclude."""
    bundle = f"{bundle_schema}.local_cb"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--comment",
            "created by integration test",
            "--exclude",
            "venv",
            "--exclude",
            "*.pyc",
        ]
    )
    assert result.exit_code == 0, result.output
    assert "LOCAL_CB successfully created." in result.output

    # Directory structure is preserved and excluded paths never reach the bundle.
    assert _version_files(runner, bundle) == [
        "code_bundle.yml",
        "main.py",
        "src/helper.py",
    ]

    described = _describe(runner, bundle)
    assert described["comment"] == "created by integration test"
    assert described["last_version_name"] == "VERSION$1"


@pytest.mark.integration
def test_create_from_local_source_without_exclude_uploads_everything(
    runner, bundle_schema, local_bundle_source
):
    bundle = f"{bundle_schema}.no_exclude_cb"

    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(local_bundle_source)]
    )
    assert result.exit_code == 0, result.output

    assert _version_files(runner, bundle) == [
        "code_bundle.yml",
        "main.py",
        "src/helper.py",
        "src/helper.pyc",
        "venv/lib/package.py",
    ]


@pytest.mark.integration
def test_create_from_local_source_with_file_protocol(
    runner, bundle_schema, local_bundle_source
):
    bundle = f"{bundle_schema}.file_proto_cb"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            f"file://{local_bundle_source}",
            "--exclude",
            "venv",
            "--exclude",
            "*.pyc",
        ]
    )
    assert result.exit_code == 0, result.output
    assert _version_files(runner, bundle) == [
        "code_bundle.yml",
        "main.py",
        "src/helper.py",
    ]


@pytest.mark.integration
def test_create_from_stage_source(runner, bundle_schema, local_bundle_source):
    """A `@stage` --source is passed through verbatim, no temporary stage."""
    stage = f"{bundle_schema}.bundle_src_stage"
    _sql(runner, f"create stage {stage}")
    result = runner.invoke_with_connection_json(
        ["stage", "copy", str(local_bundle_source / "main.py"), f"@{stage}/app"]
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke_with_connection_json(
        ["stage", "copy", str(local_bundle_source / "code_bundle.yml"), f"@{stage}/app"]
    )
    assert result.exit_code == 0, result.output

    bundle = f"{bundle_schema}.stage_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", f"@{stage}/app"]
    )
    assert result.exit_code == 0, result.output
    assert "STAGE_CB successfully created." in result.output
    assert _version_files(runner, bundle) == ["code_bundle.yml", "main.py"]


@pytest.mark.integration
def test_create_with_short_source_flag(runner, bundle_schema, local_bundle_source):
    bundle = f"{bundle_schema}.short_flag_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "-s", str(local_bundle_source)]
    )
    assert result.exit_code == 0, result.output
    assert "SHORT_FLAG_CB successfully created." in result.output


@pytest.mark.integration
def test_create_existing_bundle_fails_without_flags(
    runner, bundle_schema, local_bundle_source
):
    bundle = f"{bundle_schema}.duplicate_cb"
    args = ["bundle", "create", bundle, "--source", str(local_bundle_source)]

    result = runner.invoke_with_connection(args)
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(args)
    assert result.exit_code == 1, result.output
    assert "already exists" in result.output


@pytest.mark.integration
def test_create_skip_if_exists_is_idempotent(
    runner, bundle_schema, local_bundle_source
):
    bundle = f"{bundle_schema}.skip_cb"
    args = [
        "bundle",
        "create",
        bundle,
        "--source",
        str(local_bundle_source),
        "--skip-if-exists",
    ]

    result = runner.invoke_with_connection(args)
    assert result.exit_code == 0, result.output
    created_on = _describe(runner, bundle)["created_on"]

    result = runner.invoke_with_connection(args)
    assert result.exit_code == 0, result.output
    # Still the original bundle — nothing was replaced.
    assert _describe(runner, bundle)["created_on"] == created_on
    assert _describe(runner, bundle)["last_version_name"] == "VERSION$1"


@pytest.mark.integration
def test_create_overwrite_replaces_bundle(runner, bundle_schema, local_bundle_source):
    bundle = f"{bundle_schema}.overwrite_cb"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--exclude",
            "venv",
            "--exclude",
            "*.pyc",
            "--comment",
            "first",
        ]
    )
    assert result.exit_code == 0, result.output
    assert _describe(runner, bundle)["comment"] == "first"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--comment",
            "second",
        ]
    )
    assert result.exit_code == 1, result.output

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--comment",
            "second",
            "--overwrite",
        ]
    )
    assert result.exit_code == 0, result.output
    described = _describe(runner, bundle)
    assert described["comment"] == "second"
    # CREATE OR REPLACE resets the version history.
    assert described["last_version_name"] == "VERSION$1"
    # The replacement ran without --exclude, so the previously excluded files
    # are part of the bundle now.
    assert "venv/lib/package.py" in _version_files(runner, bundle)


@pytest.mark.integration
def test_create_with_quotes_in_comment(runner, bundle_schema, local_bundle_source):
    """The comment is escaped, so quotes cannot break the CREATE statement."""
    bundle = f"{bundle_schema}.comment_cb"
    comment = "it's a 'quoted' comment"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--comment",
            comment,
        ]
    )
    assert result.exit_code == 0, result.output
    assert _describe(runner, bundle)["comment"] == comment


@pytest.mark.integration
def test_create_rejects_overwrite_with_skip_if_exists(
    runner, bundle_schema, local_bundle_source
):
    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            f"{bundle_schema}.never_created_cb",
            "--source",
            str(local_bundle_source),
            "--overwrite",
            "--skip-if-exists",
        ]
    )
    # Incompatible parameters are a usage error, hence exit code 2.
    assert result.exit_code == 2, result.output
    assert "--overwrite" in result.output and "--skip-if-exists" in result.output


@pytest.mark.integration
def test_create_rejects_unsupported_source_protocol(runner, bundle_schema):
    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            f"{bundle_schema}.never_created_cb",
            "--source",
            "s3://my-bucket/project",
        ]
    )
    assert result.exit_code == 1, result.output
    assert "Invalid source: 's3://my-bucket/project'" in result.output


@pytest.mark.integration
def test_create_rejects_local_source_that_is_not_a_directory(
    runner, bundle_schema, local_bundle_source
):
    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            f"{bundle_schema}.never_created_cb",
            "--source",
            str(local_bundle_source / "main.py"),
        ]
    )
    assert result.exit_code == 1, result.output
    assert "is not a directory" in result.output


@pytest.mark.integration
def test_list_filters(runner, bundle_schema, local_bundle_source):
    database, schema = bundle_schema.split(".")
    for name in ("alpha_cb", "beta_cb"):
        result = runner.invoke_with_connection(
            [
                "bundle",
                "create",
                f"{bundle_schema}.{name}",
                "--source",
                str(local_bundle_source),
            ]
        )
        assert result.exit_code == 0, result.output

    # --in schema
    result = runner.invoke_with_connection_json(
        ["bundle", "list", "--in", "schema", bundle_schema]
    )
    assert result.exit_code == 0, result.output
    assert sorted(row["name"] for row in result.json) == ["ALPHA_CB", "BETA_CB"]
    assert {row["schema_name"] for row in result.json} == {schema.upper()}

    # --like narrows the result set
    result = runner.invoke_with_connection_json(
        ["bundle", "list", "--like", "alpha%", "--in", "schema", bundle_schema]
    )
    assert result.exit_code == 0, result.output
    assert [row["name"] for row in result.json] == ["ALPHA_CB"]

    # -l is the short form of --like
    result = runner.invoke_with_connection_json(
        ["bundle", "list", "-l", "beta%", "--in", "schema", bundle_schema]
    )
    assert result.exit_code == 0, result.output
    assert [row["name"] for row in result.json] == ["BETA_CB"]

    # A non-matching pattern yields no rows rather than an error.
    result = runner.invoke_with_connection_json(
        ["bundle", "list", "--like", "no_such_bundle%", "--in", "schema", bundle_schema]
    )
    assert result.exit_code == 0, result.output
    assert result.json == []

    # --in database sees bundles in the schema below it.
    result = runner.invoke_with_connection_json(
        ["bundle", "list", "--like", "alpha%", "--in", "database", database]
    )
    assert result.exit_code == 0, result.output
    assert [row["name"] for row in result.json] == ["ALPHA_CB"]

    # --in-account crosses databases.
    result = runner.invoke_with_connection_json(
        ["bundle", "list", "--like", "alpha_cb", "--in-account"]
    )
    assert result.exit_code == 0, result.output
    assert {
        (row["database_name"], row["schema_name"], row["name"]) for row in result.json
    } >= {(database.upper(), schema.upper(), "ALPHA_CB")}


@pytest.mark.integration
def test_list_rejects_in_with_in_account(runner, bundle_schema):
    result = runner.invoke_with_connection(
        ["bundle", "list", "--in", "schema", bundle_schema, "--in-account"]
    )
    assert result.exit_code == 2, result.output
    assert "--in-account" in result.output and "--in" in result.output


@pytest.mark.integration
def test_list_rejects_invalid_scope(runner, bundle_schema):
    result = runner.invoke_with_connection(
        ["bundle", "list", "--in", "table", bundle_schema]
    )
    assert result.exit_code == 1, result.output
    assert "Scope must be 'database' or 'schema'." in result.output


@pytest.mark.integration
def test_delete_and_if_exists(runner, bundle_schema, local_bundle_source):
    bundle = f"{bundle_schema}.delete_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(local_bundle_source)]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(["bundle", "delete", bundle])
    assert result.exit_code == 0, result.output
    assert "DELETE_CB successfully dropped." in result.output

    result = runner.invoke_with_connection_json(
        ["bundle", "list", "--like", "delete_cb", "--in", "schema", bundle_schema]
    )
    assert result.exit_code == 0, result.output
    assert result.json == []

    # Dropping again fails without --if-exists, succeeds with it.
    result = runner.invoke_with_connection(["bundle", "delete", bundle])
    assert result.exit_code == 1, result.output
    assert "does not exist or not authorized" in _error_text(result.output)

    result = runner.invoke_with_connection(["bundle", "delete", bundle, "--if-exists"])
    assert result.exit_code == 0, result.output
    assert "already dropped" in result.output


@pytest.mark.integration
def test_alter_add_version(runner, bundle_schema, local_bundle_source):
    """`alter --add-version` adds a version from the bundle's source location.

    The server requires the new version to come from the same location the
    bundle was created from, so this test creates the bundle from a stage path
    it can re-use (the temporary stage that a local --source uploads to is
    dropped when the CLI session ends).
    """
    stage = f"{bundle_schema}.version_stage"
    _sql(runner, f"create stage {stage}")
    for file_name in ("code_bundle.yml", "main.py"):
        result = runner.invoke_with_connection_json(
            ["stage", "copy", str(local_bundle_source / file_name), f"@{stage}/app"]
        )
        assert result.exit_code == 0, result.output

    bundle = f"{bundle_schema}.versioned_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", f"@{stage}/app"]
    )
    assert result.exit_code == 0, result.output
    assert _describe(runner, bundle)["last_version_name"] == "VERSION$1"

    result = runner.invoke_with_connection(
        ["bundle", "alter", bundle, "--add-version", f"@{stage}/app"]
    )
    assert result.exit_code == 0, result.output
    assert "Version successfully created." in result.output

    described = _describe(runner, bundle)
    assert described["last_version_name"] == "VERSION$2"
    assert _version_files(runner, bundle, version="version$2") == [
        "code_bundle.yml",
        "main.py",
    ]


@pytest.mark.integration
def test_alter_add_version_rejects_mismatched_source(
    runner, bundle_schema, local_bundle_source
):
    bundle = f"{bundle_schema}.mismatch_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(local_bundle_source)]
    )
    assert result.exit_code == 0, result.output

    stage = f"{bundle_schema}.other_stage"
    _sql(runner, f"create stage {stage}")
    result = runner.invoke_with_connection(
        ["bundle", "alter", bundle, "--add-version", f"@{stage}/app"]
    )
    assert result.exit_code == 1, result.output
    assert "must match existing notebook project source" in result.output


@pytest.mark.integration
def test_alter_rename_to(runner, bundle_schema, local_bundle_source):
    """`alter --rename-to` issues the rename; the server may not support it.

    Renaming a code bundle is rejected by the backend as an unsupported
    feature at the time of writing. The test accepts either outcome so it
    keeps verifying the CLI path without pinning the server behaviour.
    """
    bundle = f"{bundle_schema}.rename_me_cb"
    renamed = f"{bundle_schema}.renamed_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(local_bundle_source)]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(
        ["bundle", "alter", bundle, "--rename-to", renamed]
    )
    if result.exit_code == 0:
        listed = runner.invoke_with_connection_json(
            ["bundle", "list", "--in", "schema", bundle_schema]
        )
        assert listed.exit_code == 0, listed.output
        assert [row["name"] for row in listed.json] == ["RENAMED_CB"]
    else:
        assert "renaming" in result.output.lower(), result.output


@pytest.mark.integration
def test_alter_rejects_both_options(runner, bundle_schema, local_bundle_source):
    bundle = f"{bundle_schema}.alter_flags_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(local_bundle_source)]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(
        [
            "bundle",
            "alter",
            bundle,
            "--rename-to",
            f"{bundle_schema}.other_cb",
            "--add-version",
            "@some_stage/app",
        ]
    )
    assert result.exit_code == 2, result.output
    assert "--rename-to" in result.output and "--add-version" in result.output


@pytest.mark.integration
def test_alter_requires_one_option(runner, bundle_schema, local_bundle_source):
    bundle = f"{bundle_schema}.alter_noop_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(local_bundle_source)]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(["bundle", "alter", bundle])
    assert result.exit_code == 1, result.output
    assert "Exactly one of '--rename-to' or '--add-version'" in result.output


@pytest.mark.integration
def test_execute_requires_bundle_configuration(
    runner, bundle_schema, local_bundle_source
):
    """A bundle without `code_bundle.yml` cannot be executed."""
    bundle = f"{bundle_schema}.no_config_cb"
    # --exclude removes the configuration file from the uploaded tree.
    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--exclude",
            "code_bundle.yml",
        ]
    )
    assert result.exit_code == 0, result.output
    assert "code_bundle.yml" not in _version_files(runner, bundle)

    result = runner.invoke_with_connection(
        ["bundle", "execute", bundle, "--entrypoint", "main.py"]
    )
    assert result.exit_code == 1, result.output
    assert "Failed to load code bundle configuration" in _error_text(result.output)


@pytest.mark.integration
def test_execute_unknown_entrypoint_fails(runner, bundle_schema, local_bundle_source):
    bundle = f"{bundle_schema}.bad_entrypoint_cb"
    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(local_bundle_source)]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(
        ["bundle", "execute", bundle, "--entrypoint", "does/not/exist.py"]
    )
    assert result.exit_code == 1, result.output


@pytest.mark.integration
def test_execute_async_status_history_and_result(
    runner, bundle_schema, local_bundle_source
):
    """End-to-end `execute --async` → `status` → `history` on a real execution."""
    bundle = f"{bundle_schema}.exec_cb"
    result_table = f"{bundle_schema}.exec_result"
    execution_name = f"cli_it_exec_{uuid.uuid4().hex[:8]}"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--exclude",
            "venv",
            "--exclude",
            "*.pyc",
        ]
    )
    assert result.exit_code == 0, result.output

    # Arguments for the bundle must follow the connection flag: `execute`
    # accepts extra args, so anything after `--` is passed to the bundle -
    # including flags meant for the CLI itself.
    result = runner.invoke_passthrough_with_connection(
        [
            "bundle",
            "execute",
            bundle,
            "--entrypoint",
            "main.py",
            "--execution-name",
            execution_name,
            "--async",
        ],
        passthrough_args=["--", result_table],
    )
    assert result.exit_code == 0, result.output
    assert "Request submitted. Query ID: " in result.output
    query_id = _query_id_from_async_output(result.output)

    # `history` sees the execution while it is still running, with the
    # execution name and entrypoint the CLI passed through. --bundle-name is
    # resolved in the connection's schema, so the bundle's database and schema
    # have to be named alongside it.
    database, schema = bundle_schema.split(".")
    rows = _history(
        runner,
        [
            "--bundle-name",
            "EXEC_CB",
            "--bundle-database",
            database,
            "--bundle-schema",
            schema,
            "--execution-name",
            execution_name,
            "--result-limit",
            "10",
        ],
    )
    assert len(rows) == 1, rows
    assert rows[0]["QUERY_ID"] == query_id
    assert rows[0]["ENTRYPOINT"] == "main.py"
    assert rows[0]["EXECUTION_NAME"] == execution_name
    assert rows[0]["BUNDLE_TYPE"] == "SPARK"
    assert rows[0]["COMPUTE_TYPE"] == "WAREHOUSE"
    assert rows[0]["LANGUAGE_TYPE"] == "PYTHON"

    assert _wait_for_terminal_status(runner, query_id) == "SUCCESS"

    # The arguments passed after `--` reached the entrypoint: it wrote the
    # table whose name was given as the only argument.
    assert _sql(runner, f"select result from {result_table}") == [
        {"RESULT": "CLI_IT_OK"}
    ]

    # CODE_BUNDLE_HISTORY reports the finished execution as DONE, while
    # `bundle status` (get_query_status) calls the same state SUCCESS.
    rows = _history(runner, ["--execution-name", execution_name])
    assert len(rows) == 1, rows
    assert rows[0]["STATUS"] in {"DONE", "SUCCESS"}, rows[0]["STATUS"]
    assert rows[0]["END_TIME"] is not None


@pytest.mark.integration
def test_execute_async_then_cancel(runner, bundle_schema, local_bundle_source):
    bundle = f"{bundle_schema}.cancel_cb"
    execution_name = f"cli_it_cancel_{uuid.uuid4().hex[:8]}"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--exclude",
            "venv",
            "--exclude",
            "*.pyc",
        ]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(
        [
            "bundle",
            "execute",
            bundle,
            "--entrypoint",
            "main.py",
            "--execution-name",
            execution_name,
            "--async",
        ]
    )
    assert result.exit_code == 0, result.output
    query_id = _query_id_from_async_output(result.output)

    result = runner.invoke_with_connection(["bundle", "cancel", query_id])
    assert result.exit_code == 0, result.output
    assert query_id in result.output
    assert "terminated" in result.output

    assert _wait_for_terminal_status(runner, query_id) != "SUCCESS"


@pytest.mark.integration
def test_status_rejects_malformed_query_id(runner):
    result = runner.invoke_with_connection(["bundle", "status", "not-a-query-id"])
    assert result.exit_code == 1, result.output
    assert "Invalid query ID: not-a-query-id" in result.output


@pytest.mark.integration
def test_cancel_query_that_is_not_running(runner):
    """Cancelling a well-formed but idle query id reports it, it is not an error."""
    result = runner.invoke_with_connection(
        ["bundle", "cancel", "01000000-0000-0000-0000-000000000000"]
    )
    assert result.exit_code == 0, result.output
    assert "not currently executing" in result.output


@pytest.mark.integration
def test_cancel_with_malformed_query_id(runner):
    """`cancel` passes the id straight to SYSTEM$CANCEL_QUERY.

    Unlike `status`, which validates the id client-side and fails, `cancel`
    surfaces the server's reply and exits successfully.
    """
    result = runner.invoke_with_connection(["bundle", "cancel", "not-a-query-id"])
    assert result.exit_code == 0, result.output
    assert "invalid UUID" in result.output


@pytest.mark.integration
def test_history_filters(runner, bundle_schema, local_bundle_source):
    """History filters are accepted by the server and narrow the result set."""
    database, schema = bundle_schema.split(".")
    bundle = f"{bundle_schema}.history_cb"
    execution_name = f"cli_it_history_{uuid.uuid4().hex[:8]}"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(local_bundle_source),
            "--exclude",
            "venv",
            "--exclude",
            "*.pyc",
        ]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(
        [
            "bundle",
            "execute",
            bundle,
            "--entrypoint",
            "main.py",
            "--execution-name",
            execution_name,
            "--async",
        ]
    )
    assert result.exit_code == 0, result.output
    query_id = _query_id_from_async_output(result.output)

    try:
        start_time = _sql(
            runner,
            "select to_char(dateadd(hour, -1, current_timestamp()), "
            "'YYYY-MM-DD HH24:MI:SS') as ts",
        )[0]["TS"]
        end_time = _sql(
            runner,
            "select to_char(dateadd(hour, 1, current_timestamp()), "
            "'YYYY-MM-DD HH24:MI:SS') as ts",
        )[0]["TS"]

        rows = _history(
            runner,
            [
                "--bundle-name",
                "HISTORY_CB",
                "--bundle-database",
                database,
                "--bundle-schema",
                schema,
                "--entrypoint",
                "main.py",
                "--execution-name",
                execution_name,
                "--bundle-types",
                "SPARK",
                "--compute-types",
                "WAREHOUSE",
                "--language-types",
                "PYTHON",
                "--start-time-range-start",
                start_time,
                "--start-time-range-end",
                end_time,
                "--result-limit",
                "5",
            ],
        )
        assert [row["QUERY_ID"] for row in rows] == [query_id]
        assert rows[0]["DATABASE_NAME"] == database.upper()
        assert rows[0]["SCHEMA_NAME"] == schema.upper()

        # A --status filter that does not match the execution excludes it.
        # The filter takes the values CODE_BUNDLE_HISTORY accepts (pending,
        # running, done, succeeded, failed, cancelled, canceled, deleted),
        # which are not the values of the STATUS column it returns.
        rows = _history(
            runner,
            ["--execution-name", execution_name, "--status", "succeeded"],
        )
        assert [row["QUERY_ID"] for row in rows] == []

        rows = _history(
            runner,
            ["--execution-name", execution_name, "--status", "running"],
        )
        assert [row["QUERY_ID"] for row in rows] == [query_id]

        # An execution name that was never used yields no rows.
        rows = _history(runner, ["--execution-name", f"no_such_{execution_name}"])
        assert rows == []

        # The time window excludes executions that started after it.
        rows = _history(
            runner,
            [
                "--execution-name",
                execution_name,
                "--start-time-range-end",
                start_time,
            ],
        )
        assert rows == []
    finally:
        runner.invoke_with_connection(["bundle", "cancel", query_id])


@pytest.mark.integration
def test_history_result_limit(runner):
    rows = _history(runner, ["--result-limit", "1"])
    assert len(rows) <= 1
    assert json.dumps(rows), "history output must be JSON serializable"


# ---------------------------------------------------------------------------
# JVM code bundles (`language: java` / `language: scala`)
#
# A JVM bundle ships jars declared under `properties.java_dependencies.jars`
# and is executed with `--entrypoint <fully.qualified.MainClass>` rather than a
# file path. The server reports the bundle as LANGUAGE_TYPE JAVA or SCALA in
# `bundle history` according to the spec's `language`, and the same jar runs
# under either value.
# ---------------------------------------------------------------------------

JVM_DATA_DIR = "code_bundle_jvm"
HELLO_JAR = "scos-jvm-hello_2.12-1.0.0.jar"
HELLO_CLASS = "com.snowflake.scos.test.ScosJvmHelloApp"
ARGS_JAR = "scos-jvm-args_2.12-1.0.0.jar"
ARGS_CLASS = "com.snowflake.scos.test.ScosJvmArgsApp"


def _jvm_bundle_yml(jars: List[str], language: str = "java") -> str:
    jar_lines = "".join(f"        - {jar}\n" for jar in jars)
    return (
        "bundle:\n"
        "  type: spark\n"
        "  compute_type: warehouse\n"
        f"  language: {language}\n"
        "  compute_options:\n"
        '    runtime_version: "1.29"\n'
        '    language_version: "2.12"\n'
        "  properties:\n"
        "    java_dependencies:\n"
        "      jars:\n" + jar_lines
    )


@pytest.fixture
def jvm_jar_dir(test_root_path) -> Path:
    """In-repo directory holding the prebuilt JVM test jars."""
    jar_dir = Path(test_root_path) / "test_data" / JVM_DATA_DIR
    for jar in (HELLO_JAR, ARGS_JAR):
        assert (jar_dir / jar).is_file(), f"missing test artifact: {jar_dir / jar}"
    return jar_dir


@pytest.fixture
def java_bundle_source(tmp_path) -> Path:
    """Local Java project: a binary artifact, plus build output to exclude."""
    source = tmp_path / "java_project"
    (source / "target" / "classes").mkdir(parents=True)

    (source / "code_bundle.yaml").write_text(_jvm_bundle_yml(["app.jar"]))
    # Not a loadable jar - these tests only need bytes that are not text.
    (source / "app.jar").write_bytes(bytes(range(256)) * 8)
    (source / "README.md").write_text("how to build this bundle\n")
    (source / "target" / "app.jar.orig").write_bytes(b"\x00\x01stale build\xff")
    (source / "target" / "classes" / "App.class").write_bytes(b"\xca\xfe\xba\xbe stale")
    return source


@pytest.mark.integration
def test_create_java_bundle_keeps_binary_artifacts_intact(
    runner, bundle_schema, java_bundle_source, tmp_path
):
    """A jar survives the temporary-stage upload byte for byte.

    Uploads go through `StageManager.put(..., auto_compress=False)`, so what the
    bundle stores has to be the original bytes rather than a gzipped copy.
    """
    bundle = f"{bundle_schema}.java_binary_cb"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(java_bundle_source),
            "--exclude",
            "target",
        ]
    )
    assert result.exit_code == 0, result.output
    assert _version_files(runner, bundle) == [
        "README.md",
        "app.jar",
        "code_bundle.yaml",
    ]

    download_dir = tmp_path / "downloaded"
    download_dir.mkdir()
    result = runner.invoke_with_connection_json(
        [
            "stage",
            "copy",
            f"snow://code bundle/{bundle}/versions/version$1/app.jar",
            str(download_dir),
        ]
    )
    assert result.exit_code == 0, result.output
    assert (download_dir / "app.jar").read_bytes() == (
        java_bundle_source / "app.jar"
    ).read_bytes()


@pytest.mark.integration
def test_create_java_bundle_excludes_build_output(
    runner, bundle_schema, java_bundle_source
):
    """`--exclude` patterns match each path component, so a build tree drops out."""
    bundle = f"{bundle_schema}.java_exclude_cb"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(java_bundle_source),
            "--exclude",
            "*.class",
            "--exclude",
            "*.orig",
            "--exclude",
            "README.md",
        ]
    )
    assert result.exit_code == 0, result.output
    # `target/` itself is not excluded, only its contents match the patterns, so
    # the directory contributes no files at all.
    assert _version_files(runner, bundle) == ["app.jar", "code_bundle.yaml"]


@pytest.mark.integration
def test_execute_java_bundle_records_java_language_and_entrypoint(
    runner, bundle_schema, java_bundle_source
):
    """The class entrypoint and `language: java` reach the server.

    This runs without a loadable jar: the execution is expected to fail, and the
    failure itself proves the entrypoint was delivered verbatim, because the JVM
    class loader names the class it could not find.
    """
    bundle = f"{bundle_schema}.java_meta_cb"
    execution_name = f"cli_it_java_meta_{uuid.uuid4().hex[:8]}"
    entrypoint = "com.example.NoSuchApp"

    result = runner.invoke_with_connection(
        [
            "bundle",
            "create",
            bundle,
            "--source",
            str(java_bundle_source),
            "--exclude",
            "target",
        ]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_with_connection(
        [
            "bundle",
            "execute",
            bundle,
            "--entrypoint",
            entrypoint,
            "--execution-name",
            execution_name,
            "--async",
        ]
    )
    assert result.exit_code == 0, result.output
    query_id = _query_id_from_async_output(result.output)

    rows = _history(runner, ["--execution-name", execution_name])
    assert len(rows) == 1, rows
    assert rows[0]["QUERY_ID"] == query_id
    assert rows[0]["ENTRYPOINT"] == entrypoint
    assert rows[0]["LANGUAGE_TYPE"] == "JAVA"
    assert rows[0]["BUNDLE_TYPE"] == "SPARK"
    assert rows[0]["COMPUTE_TYPE"] == "WAREHOUSE"

    # --language-types filters on that same value.
    rows = _history(
        runner, ["--execution-name", execution_name, "--language-types", "JAVA"]
    )
    assert [row["QUERY_ID"] for row in rows] == [query_id]
    rows = _history(
        runner, ["--execution-name", execution_name, "--language-types", "PYTHON"]
    )
    assert rows == []

    assert _wait_for_terminal_status(runner, query_id) != "SUCCESS"
    rows = _history(runner, ["--execution-name", execution_name])
    assert entrypoint in rows[0]["ERROR_MESSAGE"], rows[0]["ERROR_MESSAGE"]


@pytest.mark.integration
@pytest.mark.parametrize("language", ["java", "scala"])
def test_execute_jvm_bundle_runs_main_class(
    runner, bundle_schema, jvm_jar_dir, tmp_path, language
):
    """End-to-end JVM run: the jar's main class writes its result table."""
    source = tmp_path / f"{language}_hello"
    source.mkdir()
    (source / "code_bundle.yaml").write_text(_jvm_bundle_yml([HELLO_JAR], language))
    (source / HELLO_JAR).write_bytes((jvm_jar_dir / HELLO_JAR).read_bytes())

    bundle = f"{bundle_schema}.{language}_hello_cb"
    result_table = f"{bundle_schema}.{language}_hello_result"
    execution_name = f"cli_it_{language}_hello_{uuid.uuid4().hex[:8]}"

    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(source)]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_passthrough_with_connection(
        [
            "bundle",
            "execute",
            bundle,
            "--entrypoint",
            HELLO_CLASS,
            "--execution-name",
            execution_name,
            "--async",
        ],
        passthrough_args=["--", result_table],
    )
    assert result.exit_code == 0, result.output
    query_id = _query_id_from_async_output(result.output)

    assert _wait_for_terminal_status(runner, query_id) == "SUCCESS"
    assert _sql(runner, f"select result from {result_table}") == [
        {"RESULT": "SCOS_JVM_OK"}
    ]

    rows = _history(runner, ["--execution-name", execution_name])
    assert len(rows) == 1, rows
    assert rows[0]["ENTRYPOINT"] == HELLO_CLASS
    assert rows[0]["LANGUAGE_TYPE"] == language.upper()
    assert rows[0]["STATUS"] in {"DONE", "SUCCESS"}, rows[0]["STATUS"]


@pytest.mark.integration
def test_execute_java_bundle_passes_arguments_to_main_class(
    runner, bundle_schema, jvm_jar_dir, tmp_path
):
    """Arguments after `--` arrive as separate `args[]` tokens, unsplit."""
    source = tmp_path / "java_args"
    source.mkdir()
    (source / "code_bundle.yaml").write_text(_jvm_bundle_yml([ARGS_JAR]))
    (source / ARGS_JAR).write_bytes((jvm_jar_dir / ARGS_JAR).read_bytes())

    bundle = f"{bundle_schema}.java_args_cb"
    result_table = f"{bundle_schema}.java_args_result"
    execution_name = f"cli_it_java_args_{uuid.uuid4().hex[:8]}"
    # The echo app takes the table name first, then one row per remaining token.
    arguments = ["--flag", "value with space", "--count", "42"]

    result = runner.invoke_with_connection(
        ["bundle", "create", bundle, "--source", str(source)]
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke_passthrough_with_connection(
        [
            "bundle",
            "execute",
            bundle,
            "--entrypoint",
            ARGS_CLASS,
            "--execution-name",
            execution_name,
            "--async",
        ],
        passthrough_args=["--", result_table, *arguments],
    )
    assert result.exit_code == 0, result.output
    query_id = _query_id_from_async_output(result.output)

    assert _wait_for_terminal_status(runner, query_id) == "SUCCESS"
    rows = _sql(runner, f"select idx, arg from {result_table} order by idx")
    assert [row["ARG"] for row in rows] == arguments
