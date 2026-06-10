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
