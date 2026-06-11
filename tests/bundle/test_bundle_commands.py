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

from unittest import mock

import pytest
from snowflake.cli._plugins.bundle.manager import CodeBundleManager
from snowflake.cli.api.identifiers import FQN


def test_bundle_group_help(runner):
    result = runner.invoke(["bundle", "--help"])

    assert result.exit_code == 0, result.output
    assert "Snowflake Code Bundles" in result.output


def test_create_help(runner):
    result = runner.invoke(["bundle", "create", "--help"])

    assert result.exit_code == 0, result.output
    assert "--source" in result.output
    assert "--comment" in result.output
    assert "--overwrite" in result.output
    assert "--skip-if-exists" in result.output
    assert "--exclude" in result.output


@pytest.mark.parametrize(
    "comment_args, expected_comment",
    [
        ([], None),
        (["--comment", "hello"], "'hello'"),
    ],
)
@pytest.mark.parametrize(
    "create_mode_args, expected_overwrite, expected_skip_if_exists",
    [
        ([], False, False),
        (["--overwrite"], True, False),
        (["--skip-if-exists"], False, True),
    ],
)
@mock.patch.object(CodeBundleManager, "create")
def test_create_calls_manager(
    mock_create,
    runner,
    comment_args,
    expected_comment,
    create_mode_args,
    expected_overwrite,
    expected_skip_if_exists,
    mock_statement_success,
):
    mock_create.return_value = mock_statement_success()
    name = "my_bundle"
    source = "@stage/path"

    result = runner.invoke(
        [
            "bundle",
            "create",
            name,
            "--source",
            source,
            *comment_args,
            *create_mode_args,
        ]
    )
    assert result.exit_code == 0, result.output

    mock_create.assert_called_once_with(
        name=FQN.from_string(name),
        source=source,
        comment=expected_comment,
        overwrite=expected_overwrite,
        skip_if_exists=expected_skip_if_exists,
    )


@pytest.mark.parametrize(
    "source",
    [
        "@db.schema.stage/path",
        "snow://workspace/my_workspace/my_project",
    ],
)
@mock.patch("snowflake.connector.connect")
def test_create_query_no_comment(mock_connector, mock_ctx, runner, source):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    name = "my_bundle"

    result = runner.invoke(["bundle", "create", name, "--source", source])
    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        f"CREATE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.{name}') "
        f"FROM '{source}'"
    )


@mock.patch("snowflake.connector.connect")
def test_create_query_with_comment(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "create",
            "my_bundle",
            "--source",
            "@stage/path",
            "--comment",
            "hello",
        ]
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "CREATE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "FROM '@stage/path' COMMENT = 'hello'"
    )


@mock.patch("snowflake.connector.connect")
def test_create_query_escapes_comment(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "create",
            "my_bundle",
            "--source",
            "@stage/path",
            "--comment",
            "it's fine",
        ]
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "CREATE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "FROM '@stage/path' COMMENT = 'it''s fine'"
    )


@mock.patch("snowflake.connector.connect")
def test_create_query_with_overwrite(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "create",
            "my_bundle",
            "--source",
            "@stage/path",
            "--overwrite",
        ]
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "CREATE OR REPLACE CODE BUNDLE "
        "IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "FROM '@stage/path'"
    )


@mock.patch("snowflake.connector.connect")
def test_create_query_with_overwrite_and_comment(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "create",
            "my_bundle",
            "--source",
            "@stage/path",
            "--overwrite",
            "--comment",
            "hello",
        ]
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "CREATE OR REPLACE CODE BUNDLE "
        "IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "FROM '@stage/path' COMMENT = 'hello'"
    )


@mock.patch("snowflake.connector.connect")
def test_create_query_with_skip_if_exists(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "create",
            "my_bundle",
            "--source",
            "@stage/path",
            "--skip-if-exists",
        ]
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "CREATE CODE BUNDLE IF NOT EXISTS "
        "IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "FROM '@stage/path'"
    )


def test_create_rejects_overwrite_with_skip_if_exists(runner):
    result = runner.invoke(
        [
            "bundle",
            "create",
            "my_bundle",
            "--source",
            "@stage/path",
            "--overwrite",
            "--skip-if-exists",
        ]
    )
    assert result.exit_code != 0
    assert "--overwrite" in result.output
    assert "--skip-if-exists" in result.output
    assert "incompatible" in result.output


def test_create_rejects_empty_source(runner):
    result = runner.invoke(["bundle", "create", "my_bundle", "--source", ""])
    assert result.exit_code != 0
    assert "Source is required." in result.output


def test_create_requires_source(runner):
    result = runner.invoke(["bundle", "create", "my_bundle"])
    assert result.exit_code != 0
    assert "--source" in result.output or "Missing option" in result.output


def test_create_requires_identifier(runner):
    result = runner.invoke(["bundle", "create", "--source", "@stage/path"])
    assert result.exit_code != 0


def test_list_help(runner):
    result = runner.invoke(["bundle", "list", "--help"])

    assert result.exit_code == 0, result.output
    assert "code bundles" in result.output.lower()
    assert "--like" in result.output
    assert "--in" in result.output


@mock.patch.object(CodeBundleManager, "show")
def test_list_calls_manager(mock_show, runner, mock_cursor):
    mock_show.return_value = mock_cursor(rows=[], columns=[])

    result = runner.invoke(["bundle", "list"])

    assert result.exit_code == 0, result.output
    mock_show.assert_called_once_with(like=None, scope=(None, None), in_account=False)


@mock.patch.object(CodeBundleManager, "show")
def test_list_calls_manager_with_like(mock_show, runner, mock_cursor):
    mock_show.return_value = mock_cursor(rows=[], columns=[])

    result = runner.invoke(["bundle", "list", "--like", "my%"])

    assert result.exit_code == 0, result.output
    mock_show.assert_called_once_with(like="my%", scope=(None, None), in_account=False)


@mock.patch("snowflake.connector.connect")
def test_list_query(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES"


@mock.patch("snowflake.connector.connect")
def test_list_query_with_like(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list", "--like", "my%"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES LIKE 'my%'"


@mock.patch("snowflake.connector.connect")
def test_list_query_with_like_short_flag(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list", "-l", "foo%"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES LIKE 'foo%'"


@mock.patch("snowflake.connector.connect")
def test_list_query_escapes_like(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list", "--like", "it's%"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES LIKE 'it''s%'"


def test_list_invalid_scope(runner):
    result = runner.invoke(["bundle", "list", "--in", "account", "mydb"])

    assert result.exit_code != 0
    assert "Scope must be" in result.output


def test_list_empty_scope_name(runner):
    result = runner.invoke(["bundle", "list", "--in", "database", ""])

    assert result.exit_code != 0
    assert "cannot be empty" in result.output


@mock.patch("snowflake.connector.connect")
def test_list_query_with_in_database(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list", "--in", "database", "mydb"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES IN DATABASE IDENTIFIER('mydb')"


@mock.patch("snowflake.connector.connect")
def test_list_query_with_in_schema(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list", "--in", "schema", "mydb.myschema"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES IN SCHEMA IDENTIFIER('mydb.myschema')"


@mock.patch("snowflake.connector.connect")
def test_list_query_with_like_and_in(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["bundle", "list", "--like", "my%", "--in", "database", "mydb"]
    )

    assert result.exit_code == 0, result.output
    assert (
        ctx.get_query() == "SHOW CODE BUNDLES LIKE 'my%' IN DATABASE IDENTIFIER('mydb')"
    )


@mock.patch("snowflake.connector.connect")
def test_list_scope_case_insensitive(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list", "--in", "DATABASE", "mydb"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES IN DATABASE IDENTIFIER('mydb')"


def test_list_help_shows_in_account(runner):
    result = runner.invoke(["bundle", "list", "--help"])

    assert result.exit_code == 0, result.output
    assert "--in-account" in result.output


@mock.patch("snowflake.connector.connect")
def test_list_query_with_in_account(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list", "--in-account"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES IN ACCOUNT"


@mock.patch("snowflake.connector.connect")
def test_list_query_with_like_and_in_account(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "list", "--like", "my%", "--in-account"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "SHOW CODE BUNDLES LIKE 'my%' IN ACCOUNT"


def test_list_rejects_in_and_in_account(runner):
    result = runner.invoke(
        ["bundle", "list", "--in", "database", "mydb", "--in-account"]
    )

    assert result.exit_code != 0
    assert "--in-account" in result.output
    assert "--in" in result.output


def test_delete_help(runner):
    result = runner.invoke(["bundle", "delete", "--help"])

    assert result.exit_code == 0, result.output
    assert "--if-exists" in result.output
    assert "IDENTIFIER" in result.output


@pytest.mark.parametrize(
    "if_exists_args, expected_if_exists",
    [
        ([], False),
        (["--if-exists"], True),
    ],
)
@mock.patch.object(CodeBundleManager, "drop")
def test_delete_calls_manager(
    mock_drop,
    runner,
    if_exists_args,
    expected_if_exists,
    mock_statement_success,
):
    mock_drop.return_value = mock_statement_success()
    name = "my_bundle"

    result = runner.invoke(["bundle", "delete", name, *if_exists_args])

    assert result.exit_code == 0, result.output
    mock_drop.assert_called_once_with(
        name=FQN.from_string(name),
        if_exists=expected_if_exists,
    )


@mock.patch("snowflake.connector.connect")
def test_delete_query(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "delete", "my_bundle"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "DROP CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle')"
    )


@mock.patch("snowflake.connector.connect")
def test_delete_query_with_if_exists(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "delete", "my_bundle", "--if-exists"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "DROP CODE BUNDLE IF EXISTS " "IDENTIFIER('MockDatabase.MockSchema.my_bundle')"
    )


def test_delete_requires_identifier(runner):
    result = runner.invoke(["bundle", "delete"])
    assert result.exit_code != 0


def test_alter_help(runner):
    result = runner.invoke(["bundle", "alter", "--help"])

    assert result.exit_code == 0, result.output
    assert "--rename-to" in result.output
    assert "--add-version" in result.output


@mock.patch.object(CodeBundleManager, "alter")
def test_alter_calls_manager_with_rename_to(mock_alter, runner, mock_statement_success):
    mock_alter.return_value = mock_statement_success()
    name = "my_bundle"

    result = runner.invoke(["bundle", "alter", name, "--rename-to", "new_bundle"])

    assert result.exit_code == 0, result.output
    mock_alter.assert_called_once_with(
        name=FQN.from_string(name),
        rename_to="new_bundle",
        add_version=None,
    )


@mock.patch.object(CodeBundleManager, "alter")
def test_alter_calls_manager_with_add_version(
    mock_alter, runner, mock_statement_success
):
    mock_alter.return_value = mock_statement_success()
    name = "my_bundle"

    result = runner.invoke(["bundle", "alter", name, "--add-version", "@stage/path"])

    assert result.exit_code == 0, result.output
    mock_alter.assert_called_once_with(
        name=FQN.from_string(name),
        rename_to=None,
        add_version="@stage/path",
    )


@mock.patch("snowflake.connector.connect")
def test_alter_query_with_rename_to(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["bundle", "alter", "my_bundle", "--rename-to", "new_bundle"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "ALTER CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "RENAME TO IDENTIFIER('MockDatabase.MockSchema.new_bundle')"
    )


@mock.patch("snowflake.connector.connect")
def test_alter_query_with_add_version(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["bundle", "alter", "my_bundle", "--add-version", "@stage/path"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "ALTER CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "ADD VERSION FROM '@stage/path'"
    )


@mock.patch("snowflake.connector.connect")
def test_alter_query_escapes_add_version(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["bundle", "alter", "my_bundle", "--add-version", "it's_path"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "ALTER CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "ADD VERSION FROM 'it''s_path'"
    )


@mock.patch("snowflake.connector.connect")
def test_alter_query_with_fqn_rename_to(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "alter",
            "my_bundle",
            "--rename-to",
            "mydb.myschema.new_bundle",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "ALTER CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "RENAME TO IDENTIFIER('mydb.myschema.new_bundle')"
    )


def test_alter_rejects_both_options(runner):
    result = runner.invoke(
        [
            "bundle",
            "alter",
            "my_bundle",
            "--rename-to",
            "new_bundle",
            "--add-version",
            "@stage/path",
        ]
    )

    assert result.exit_code != 0
    assert "--rename-to" in result.output
    assert "--add-version" in result.output
    assert "incompatible" in result.output


def test_alter_rejects_neither_option(runner):
    result = runner.invoke(["bundle", "alter", "my_bundle"])

    assert result.exit_code != 0
    assert "--rename-to" in result.output
    assert "--add-version" in result.output


def test_alter_requires_identifier(runner):
    result = runner.invoke(["bundle", "alter"])
    assert result.exit_code != 0


def test_execute_help(runner):
    result = runner.invoke(["bundle", "execute", "--help"])

    assert result.exit_code == 0, result.output
    assert "--entrypoint" in result.output
    assert "IDENTIFIER" in result.output


@mock.patch.object(CodeBundleManager, "execute")
def test_execute_calls_manager(mock_execute, runner, mock_statement_success):
    mock_execute.return_value = mock_statement_success()
    name = "my_bundle"

    result = runner.invoke(["bundle", "execute", name, "--entrypoint", "src/main.py"])

    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        name=FQN.from_string(name),
        entrypoint="src/main.py",
        arguments=None,
        run_async=False,
    )


@mock.patch.object(CodeBundleManager, "execute")
def test_execute_calls_manager_with_arguments(
    mock_execute, runner, mock_statement_success
):
    mock_execute.return_value = mock_statement_success()
    name = "my_bundle"

    result = runner.invoke(
        [
            "bundle",
            "execute",
            name,
            "--entrypoint",
            "src/main.py",
            "--",
            "--custom-arg",
            "value",
            "--another-flag",
        ]
    )

    assert result.exit_code == 0, result.output
    mock_execute.assert_called_once_with(
        name=FQN.from_string(name),
        entrypoint="src/main.py",
        arguments=["--custom-arg", "value", "--another-flag"],
        run_async=False,
    )


@mock.patch("snowflake.connector.connect")
def test_execute_query(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["bundle", "execute", "my_bundle", "--entrypoint", "src/main.py"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "EXECUTE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "ENTRYPOINT='src/main.py'"
    )


@mock.patch("snowflake.connector.connect")
def test_execute_query_with_arguments(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "execute",
            "my_bundle",
            "--entrypoint",
            "src/main.py",
            "--",
            "--custom-arg",
            "value",
            "--another-flag",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "EXECUTE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "ENTRYPOINT='src/main.py' "
        "ARGUMENTS='--custom-arg value --another-flag'"
    )


@mock.patch("snowflake.connector.connect")
def test_execute_query_escapes_arguments(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "execute",
            "my_bundle",
            "--entrypoint",
            "src/main.py",
            "--",
            "it's",
            "fine",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "EXECUTE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "ENTRYPOINT='src/main.py' "
        "ARGUMENTS='it''s fine'"
    )


@mock.patch("snowflake.connector.connect")
def test_execute_query_escapes_entrypoint(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["bundle", "execute", "my_bundle", "--entrypoint", "it's_main.py"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "EXECUTE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "ENTRYPOINT='it''s_main.py'"
    )


def test_execute_requires_entrypoint(runner):
    result = runner.invoke(["bundle", "execute", "my_bundle"])
    assert result.exit_code != 0
    assert "--entrypoint" in result.output or "Missing option" in result.output


def test_execute_requires_identifier(runner):
    result = runner.invoke(["bundle", "execute", "--entrypoint", "src/main.py"])
    assert result.exit_code != 0


@mock.patch("snowflake.connector.connect")
def test_execute_async(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "bundle",
            "execute",
            "my_bundle",
            "--entrypoint",
            "src/main.py",
            "--async",
        ]
    )

    assert result.exit_code == 0, result.output
    assert result.output.startswith("Request submitted. Query ID:")
    assert ctx.kwargs[0]["_exec_async"] is True
    assert ctx.get_query() == (
        "EXECUTE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "ENTRYPOINT='src/main.py'"
    )


# ---------- create from local source / --exclude ----------


@mock.patch("snowflake.cli._plugins.bundle.manager.random.randint")
@mock.patch("snowflake.connector.connect")
def test_create_with_local_source_uploads_and_creates(
    mock_connector, mock_randint, mock_ctx, runner, tmp_path
):
    mock_randint.return_value = 1234567
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    (tmp_path / "main.py").write_text("print('hi')")

    result = runner.invoke(["bundle", "create", "my_bundle", "--source", str(tmp_path)])

    assert result.exit_code == 0, result.output
    queries = ctx.get_queries()
    assert queries[0] == "CREATE OR REPLACE TEMPORARY STAGE tmp_bundle_stage_1234567"
    assert any(
        q.startswith("PUT file://")
        and "main.py" in q
        and "@tmp_bundle_stage_1234567 " in q
        and "auto_compress=false" in q
        for q in queries[1:-1]
    ), queries
    assert queries[-1] == (
        "CREATE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "FROM '@tmp_bundle_stage_1234567'"
    )


@mock.patch("snowflake.cli._plugins.bundle.manager.random.randint")
@mock.patch("snowflake.connector.connect")
def test_create_with_file_protocol_source(
    mock_connector, mock_randint, mock_ctx, runner, tmp_path
):
    mock_randint.return_value = 7654321
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    (tmp_path / "main.py").write_text("print('hi')")

    result = runner.invoke(
        ["bundle", "create", "my_bundle", "--source", f"file://{tmp_path}"]
    )

    assert result.exit_code == 0, result.output
    queries = ctx.get_queries()
    assert queries[0] == "CREATE OR REPLACE TEMPORARY STAGE tmp_bundle_stage_7654321"
    assert queries[-1] == (
        "CREATE CODE BUNDLE IDENTIFIER('MockDatabase.MockSchema.my_bundle') "
        "FROM '@tmp_bundle_stage_7654321'"
    )


@mock.patch("snowflake.cli._plugins.bundle.manager.random.randint")
@mock.patch("snowflake.connector.connect")
def test_create_with_local_source_and_exclude(
    mock_connector, mock_randint, mock_ctx, runner, tmp_path
):
    mock_randint.return_value = 1234567
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    (tmp_path / "keep.py").write_text("keep")
    (tmp_path / "skip.pyc").write_text("skip")
    cache_dir = tmp_path / "__pycache__"
    cache_dir.mkdir()
    (cache_dir / "cached.pyc").write_text("cached")

    result = runner.invoke(
        [
            "bundle",
            "create",
            "my_bundle",
            "--source",
            str(tmp_path),
            "--exclude",
            "*.pyc",
            "--exclude",
            "__pycache__",
        ]
    )

    assert result.exit_code == 0, result.output
    queries = ctx.get_queries()
    put_queries = [q for q in queries if q.startswith("PUT file://")]
    assert len(put_queries) == 1, queries
    assert "keep.py" in put_queries[0]
    assert all("skip.pyc" not in q for q in queries)
    assert all("cached.pyc" not in q for q in queries)


@mock.patch.object(CodeBundleManager, "execute_query")
def test_create_with_invalid_protocol(mock_execute_query, runner):
    result = runner.invoke(
        ["bundle", "create", "my_bundle", "--source", "http://invalid/path"]
    )

    assert result.exit_code != 0
    assert "Invalid source: 'http://invalid/path'" in result.output
    mock_execute_query.assert_not_called()


# ---------- CodeBundleManager.process_source ----------


@mock.patch.object(CodeBundleManager, "execute_query")
def test_process_source_with_stage_path(mock_execute_query):
    manager = CodeBundleManager()
    assert manager.process_source("@my_stage/path") == "@my_stage/path"
    mock_execute_query.assert_not_called()


@mock.patch.object(CodeBundleManager, "execute_query")
def test_process_source_with_snow_protocol(mock_execute_query):
    manager = CodeBundleManager()
    assert (
        manager.process_source('snow://workspace/"test_workspace"')
        == 'snow://workspace/"test_workspace"'
    )
    mock_execute_query.assert_not_called()


@mock.patch.object(CodeBundleManager, "execute_query")
def test_process_source_with_snow_protocol_uppercase(mock_execute_query):
    manager = CodeBundleManager()
    assert (
        manager.process_source('SNOW://workspace/"test_workspace"')
        == 'SNOW://workspace/"test_workspace"'
    )
    mock_execute_query.assert_not_called()


@mock.patch.object(CodeBundleManager, "_upload_directory_recursive")
@mock.patch.object(CodeBundleManager, "execute_query")
@mock.patch("snowflake.cli._plugins.bundle.manager.random.randint")
def test_process_source_with_file_protocol(
    mock_randint, mock_execute_query, mock_upload
):
    mock_randint.return_value = 1234567
    manager = CodeBundleManager()
    assert manager.process_source("file:///path/to/local/dir") == (
        "@tmp_bundle_stage_1234567"
    )
    mock_execute_query.assert_called_once_with(
        "CREATE OR REPLACE TEMPORARY STAGE tmp_bundle_stage_1234567"
    )
    mock_upload.assert_called_once_with(
        "/path/to/local/dir", "tmp_bundle_stage_1234567", exclude=None
    )


@mock.patch.object(CodeBundleManager, "_upload_directory_recursive")
@mock.patch.object(CodeBundleManager, "execute_query")
@mock.patch("snowflake.cli._plugins.bundle.manager.random.randint")
def test_process_source_with_file_protocol_uppercase(
    mock_randint, mock_execute_query, mock_upload
):
    mock_randint.return_value = 7654321
    manager = CodeBundleManager()
    assert manager.process_source("FILE:///path/to/local/dir") == (
        "@tmp_bundle_stage_7654321"
    )
    mock_execute_query.assert_called_once_with(
        "CREATE OR REPLACE TEMPORARY STAGE tmp_bundle_stage_7654321"
    )
    mock_upload.assert_called_once_with(
        "/path/to/local/dir", "tmp_bundle_stage_7654321", exclude=None
    )


@mock.patch.object(CodeBundleManager, "_upload_directory_recursive")
@mock.patch.object(CodeBundleManager, "execute_query")
@mock.patch("snowflake.cli._plugins.bundle.manager.random.randint")
def test_process_source_with_local_path_no_protocol(
    mock_randint, mock_execute_query, mock_upload
):
    mock_randint.return_value = 9999999
    manager = CodeBundleManager()
    assert manager.process_source("/path/to/local/dir") == ("@tmp_bundle_stage_9999999")
    mock_execute_query.assert_called_once_with(
        "CREATE OR REPLACE TEMPORARY STAGE tmp_bundle_stage_9999999"
    )
    mock_upload.assert_called_once_with(
        "/path/to/local/dir", "tmp_bundle_stage_9999999", exclude=None
    )


@mock.patch.object(CodeBundleManager, "_upload_directory_recursive")
@mock.patch.object(CodeBundleManager, "execute_query")
@mock.patch("snowflake.cli._plugins.bundle.manager.random.randint")
def test_process_source_with_relative_local_path(
    mock_randint, mock_execute_query, mock_upload
):
    mock_randint.return_value = 1111111
    manager = CodeBundleManager()
    assert manager.process_source("relative/path/to/dir") == (
        "@tmp_bundle_stage_1111111"
    )
    mock_execute_query.assert_called_once_with(
        "CREATE OR REPLACE TEMPORARY STAGE tmp_bundle_stage_1111111"
    )
    mock_upload.assert_called_once_with(
        "relative/path/to/dir", "tmp_bundle_stage_1111111", exclude=None
    )


@mock.patch.object(CodeBundleManager, "_upload_directory_recursive")
@mock.patch.object(CodeBundleManager, "execute_query")
@mock.patch("snowflake.cli._plugins.bundle.manager.random.randint")
def test_process_source_passes_exclude(mock_randint, mock_execute_query, mock_upload):
    mock_randint.return_value = 1234567
    manager = CodeBundleManager()
    manager.process_source("/local/dir", exclude=["*.pyc", "__pycache__"])
    mock_upload.assert_called_once_with(
        "/local/dir", "tmp_bundle_stage_1234567", exclude=["*.pyc", "__pycache__"]
    )


@mock.patch.object(CodeBundleManager, "execute_query")
def test_process_source_with_invalid_protocol_raises(mock_execute_query):
    from snowflake.cli.api.exceptions import CliError

    manager = CodeBundleManager()
    with pytest.raises(CliError) as exc_info:
        manager.process_source("http://invalid/path")
    assert "Invalid source: 'http://invalid/path'" in str(exc_info.value)
    assert "Snowflake stage path" in str(exc_info.value)
    mock_execute_query.assert_not_called()


@mock.patch.object(CodeBundleManager, "execute_query")
def test_process_source_with_https_protocol_raises(mock_execute_query):
    from snowflake.cli.api.exceptions import CliError

    manager = CodeBundleManager()
    with pytest.raises(CliError) as exc_info:
        manager.process_source("https://example.com/path")
    assert "Invalid source: 'https://example.com/path'" in str(exc_info.value)
    mock_execute_query.assert_not_called()


# ---------- CodeBundleManager._upload_directory_recursive ----------


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_with_flat_structure(mock_execute_query, tmp_path):
    (tmp_path / "file1.py").write_text("content1")
    (tmp_path / "file2.py").write_text("content2")

    manager = CodeBundleManager()
    manager._upload_directory_recursive(str(tmp_path), "test_stage")  # noqa: SLF001

    assert mock_execute_query.call_count == 2
    calls = [str(call) for call in mock_execute_query.call_args_list]
    assert any(
        "PUT file://" in call and "file1.py" in call and "@test_stage" in call
        for call in calls
    )
    assert any(
        "PUT file://" in call and "file2.py" in call and "@test_stage" in call
        for call in calls
    )


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_with_nested_structure(mock_execute_query, tmp_path):
    (tmp_path / "root_file.py").write_text("root content")
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (subdir / "nested_file.py").write_text("nested content")
    deep_subdir = subdir / "deep"
    deep_subdir.mkdir()
    (deep_subdir / "deep_file.py").write_text("deep content")

    manager = CodeBundleManager()
    manager._upload_directory_recursive(str(tmp_path), "test_stage")  # noqa: SLF001

    assert mock_execute_query.call_count == 3
    calls = [str(call) for call in mock_execute_query.call_args_list]
    assert any(
        "@test_stage auto_compress=false" in call and "root_file.py" in call
        for call in calls
    )
    assert any(
        "@test_stage/subdir" in call and "nested_file.py" in call for call in calls
    )
    assert any(
        "@test_stage/subdir/deep" in call and "deep_file.py" in call for call in calls
    )


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_not_a_directory(mock_execute_query, tmp_path):
    from snowflake.cli.api.exceptions import CliError

    file_path = tmp_path / "not_a_dir.txt"
    file_path.write_text("content")

    manager = CodeBundleManager()
    with pytest.raises(CliError) as exc_info:
        manager._upload_directory_recursive(  # noqa: SLF001
            str(file_path), "test_stage"
        )
    assert "is not a directory" in str(exc_info.value)
    mock_execute_query.assert_not_called()


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_nonexistent_path(mock_execute_query, tmp_path):
    from snowflake.cli.api.exceptions import CliError

    nonexistent_path = tmp_path / "nonexistent"

    manager = CodeBundleManager()
    with pytest.raises(CliError) as exc_info:
        manager._upload_directory_recursive(  # noqa: SLF001
            str(nonexistent_path), "test_stage"
        )
    assert "is not a directory" in str(exc_info.value)
    mock_execute_query.assert_not_called()


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_empty_directory(mock_execute_query, tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    manager = CodeBundleManager()
    manager._upload_directory_recursive(str(empty_dir), "test_stage")  # noqa: SLF001

    mock_execute_query.assert_not_called()


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_skips_subdirectories(mock_execute_query, tmp_path):
    (tmp_path / "file.py").write_text("content")
    subdir = tmp_path / "subdir"
    subdir.mkdir()

    manager = CodeBundleManager()
    manager._upload_directory_recursive(str(tmp_path), "test_stage")  # noqa: SLF001

    assert mock_execute_query.call_count == 1
    call_str = str(mock_execute_query.call_args)
    assert "file.py" in call_str


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_excludes_by_filename(mock_execute_query, tmp_path):
    (tmp_path / "keep.py").write_text("keep")
    (tmp_path / "skip.pyc").write_text("skip")

    manager = CodeBundleManager()
    manager._upload_directory_recursive(  # noqa: SLF001
        str(tmp_path), "test_stage", exclude=["*.pyc"]
    )

    assert mock_execute_query.call_count == 1
    call_str = str(mock_execute_query.call_args)
    assert "keep.py" in call_str
    assert "skip.pyc" not in call_str


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_excludes_directory(mock_execute_query, tmp_path):
    (tmp_path / "keep.py").write_text("keep")
    cache_dir = tmp_path / "__pycache__"
    cache_dir.mkdir()
    (cache_dir / "cached.pyc").write_text("cached")

    manager = CodeBundleManager()
    manager._upload_directory_recursive(  # noqa: SLF001
        str(tmp_path), "test_stage", exclude=["__pycache__"]
    )

    assert mock_execute_query.call_count == 1
    call_str = str(mock_execute_query.call_args)
    assert "keep.py" in call_str
    assert "cached.pyc" not in call_str


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_excludes_multiple_patterns(
    mock_execute_query, tmp_path
):
    (tmp_path / "keep.py").write_text("keep")
    (tmp_path / "skip.pyc").write_text("skip pyc")
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    (git_dir / "config").write_text("git config")

    manager = CodeBundleManager()
    manager._upload_directory_recursive(  # noqa: SLF001
        str(tmp_path), "test_stage", exclude=["*.pyc", ".git"]
    )

    assert mock_execute_query.call_count == 1
    call_str = str(mock_execute_query.call_args)
    assert "keep.py" in call_str
    assert "skip.pyc" not in call_str
    assert "config" not in call_str


@mock.patch.object(CodeBundleManager, "execute_query")
def test_upload_directory_recursive_exclude_directory_does_not_exclude_similarly_named_file(
    mock_execute_query, tmp_path
):
    (tmp_path / "venv.txt").write_text("not excluded")
    venv_dir = tmp_path / "venv"
    venv_dir.mkdir()
    (venv_dir / "test.txt").write_text("excluded")

    manager = CodeBundleManager()
    manager._upload_directory_recursive(  # noqa: SLF001
        str(tmp_path), "test_stage", exclude=["venv"]
    )

    assert mock_execute_query.call_count == 1
    call_str = str(mock_execute_query.call_args)
    assert "venv.txt" in call_str
    assert "test.txt" not in call_str


# ---------- status ----------


def test_status_help(runner):
    result = runner.invoke(["bundle", "status", "--help"])

    assert result.exit_code == 0, result.output
    assert "QUERY_ID" in result.output


@mock.patch.object(CodeBundleManager, "get_status")
def test_status_calls_manager(mock_get_status, runner):
    mock_get_status.return_value = "RUNNING"
    query_id = "01b7f000-0000-0000-0000-000000000000"

    result = runner.invoke(["bundle", "status", query_id])

    assert result.exit_code == 0, result.output
    assert "RUNNING" in result.output
    assert query_id in result.output
    mock_get_status.assert_called_once_with(query_id=query_id)


@mock.patch("snowflake.connector.connect")
def test_status_calls_connection_get_query_status(mock_connector, mock_ctx, runner):
    from snowflake.connector.constants import QueryStatus

    ctx = mock_ctx()
    ctx.get_query_status = mock.MagicMock(return_value=QueryStatus.SUCCESS)
    mock_connector.return_value = ctx
    query_id = "01b7f000-0000-0000-0000-000000000000"

    result = runner.invoke(["bundle", "status", query_id])

    assert result.exit_code == 0, result.output
    assert "SUCCESS" in result.output
    ctx.get_query_status.assert_called_once_with(query_id)


@mock.patch("snowflake.connector.connect")
def test_status_invalid_query_id(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    ctx.get_query_status = mock.MagicMock(side_effect=ValueError("bad uuid"))
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "status", "not-a-uuid"])

    assert result.exit_code != 0
    assert "Invalid query ID" in result.output


def test_status_requires_query_id(runner):
    result = runner.invoke(["bundle", "status"])

    assert result.exit_code != 0


# ---------- cancel ----------


def test_cancel_help(runner):
    result = runner.invoke(["bundle", "cancel", "--help"])

    assert result.exit_code == 0, result.output
    assert "QUERY_ID" in result.output


@mock.patch.object(CodeBundleManager, "cancel")
def test_cancel_calls_manager(mock_cancel, runner):
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = ("query 01b... successfully cancelled.",)
    mock_cancel.return_value = cursor
    query_id = "01b7f000-0000-0000-0000-000000000000"

    result = runner.invoke(["bundle", "cancel", query_id])

    assert result.exit_code == 0, result.output
    assert "successfully cancelled" in result.output
    mock_cancel.assert_called_once_with(query_id=query_id)


@mock.patch("snowflake.connector.connect")
def test_cancel_executes_system_cancel_query(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    cursor = mock.MagicMock()
    cursor.execute.return_value = cursor
    cursor.fetchone.return_value = ("ok",)
    ctx.cursor = mock.MagicMock(return_value=cursor)
    mock_connector.return_value = ctx
    query_id = "01b7f000-0000-0000-0000-000000000000"

    result = runner.invoke(["bundle", "cancel", query_id])

    assert result.exit_code == 0, result.output
    cursor.execute.assert_called_once_with(
        "SELECT SYSTEM$CANCEL_QUERY(%s)", (query_id,)
    )


def test_cancel_requires_query_id(runner):
    result = runner.invoke(["bundle", "cancel"])

    assert result.exit_code != 0


# ---------- history ----------


def test_history_help(runner):
    result = runner.invoke(["bundle", "history", "--help"])

    assert result.exit_code == 0, result.output
    assert "IDENTIFIER" in result.output
    assert "--result-limit" in result.output


@mock.patch.object(CodeBundleManager, "history")
def test_history_calls_manager(mock_history, runner, mock_cursor):
    mock_history.return_value = mock_cursor(rows=[], columns=[])
    name = "my_bundle"

    result = runner.invoke(["bundle", "history", name])

    assert result.exit_code == 0, result.output
    mock_history.assert_called_once_with(name=FQN.from_string(name), result_limit=100)


@mock.patch.object(CodeBundleManager, "history")
def test_history_passes_result_limit(mock_history, runner, mock_cursor):
    mock_history.return_value = mock_cursor(rows=[], columns=[])
    name = "my_bundle"

    result = runner.invoke(["bundle", "history", name, "--result-limit", "50"])

    assert result.exit_code == 0, result.output
    mock_history.assert_called_once_with(name=FQN.from_string(name), result_limit=50)


@mock.patch("snowflake.connector.connect")
def test_history_query(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "history", "my_bundle"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "SELECT * FROM TABLE("
        "SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY("
        "BUNDLE_NAME => 'MockDatabase.MockSchema.my_bundle', "
        "RESULT_LIMIT => 100))"
    )


@mock.patch("snowflake.connector.connect")
def test_history_query_with_result_limit(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "history", "my_bundle", "--result-limit", "5"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "SELECT * FROM TABLE("
        "SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY("
        "BUNDLE_NAME => 'MockDatabase.MockSchema.my_bundle', "
        "RESULT_LIMIT => 5))"
    )


@mock.patch("snowflake.connector.connect")
def test_history_query_escapes_quote(mock_connector, mock_ctx, runner):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["bundle", "history", '"it\'s_bundle"'])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "SELECT * FROM TABLE("
        "SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY("
        "BUNDLE_NAME => 'MockDatabase.MockSchema.\"it''s_bundle\"', "
        "RESULT_LIMIT => 100))"
    )


def test_history_requires_identifier(runner):
    result = runner.invoke(["bundle", "history"])

    assert result.exit_code != 0
