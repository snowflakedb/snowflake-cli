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
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest import mock

import pytest
from snowflake.cli._plugins.sql.snowsql_templating import transpile_snowsql_templates
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import SnowflakeSQLExecutionError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import identifier_to_show_like_pattern
from snowflake.cli.api.sql_execution import SqlExecutionMixin, VerboseCursor
from snowflake.connector.cursor import DictCursor
from snowflake.connector.errors import ProgrammingError

from tests.testing_utils.result_assertions import assert_that_result_is_usage_error


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_sql_execute_query(mock_execute, runner, mock_cursor):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))

    result = runner.invoke(["sql", "-q", "query"])

    assert result.exit_code == 0
    mock_execute.assert_called_once_with("query", cursor_class=VerboseCursor)


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_sql_execute_file(mock_execute, runner, mock_cursor, named_temporary_file):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))
    query = "query from file"

    with named_temporary_file() as tmp_file:
        tmp_file.write_text(query)
        result = runner.invoke(["sql", "-f", tmp_file])

    assert result.exit_code == 0
    mock_execute.assert_called_once_with(query, cursor_class=VerboseCursor)


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_sql_execute_multiple_file(mock_execute, runner, mock_cursor):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))
    query = "query from file"

    with TemporaryDirectory() as tmp_dir:
        f1 = Path(tmp_dir).joinpath("f1.sql")
        f2 = Path(tmp_dir).joinpath("f2.sql")

        f1.write_text(query)
        f2.write_text(query)
        result = runner.invoke(["sql", "-f", f1, "-f", f2])

    assert result.exit_code == 0
    mock_execute.assert_has_calls(
        [
            mock.call(query, cursor_class=VerboseCursor),
            mock.call(query, cursor_class=VerboseCursor),
        ]
    )


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_sql_execute_from_stdin(mock_execute, runner, mock_cursor):
    mock_execute.return_value = (mock_cursor(["row"], []) for _ in range(1))
    query = "query from input"

    result = runner.invoke(["sql", "-i"], input=query)

    assert result.exit_code == 0
    mock_execute.assert_called_once_with(query, cursor_class=VerboseCursor)


def test_sql_help_if_no_query_file_or_stdin(runner, os_agnostic_snapshot):
    result = runner.invoke(["sql"])
    assert result.exit_code == 0, result.output
    assert result.output == os_agnostic_snapshot


def test_sql_fails_if_query_and_stdin_and_file_provided(runner):
    with NamedTemporaryFile("r") as tmp_file:
        result = runner.invoke(["sql", "-i", "-q", "foo", "-f", tmp_file.name])
        assert_that_result_is_usage_error(
            result,
            f"Parameters '--query' and '--stdin' are incompatible and cannot be used simultaneously.",
        )


@pytest.mark.parametrize("inputs", [(("-i",), "stdin"), (("-q", "foo"), "query")])
def test_sql_fails_if_other_input_and_file_provided(runner, inputs):
    with NamedTemporaryFile("r") as tmp_file:
        result = runner.invoke(["sql", *(inputs[0]), "-f", tmp_file.name])
        assert_that_result_is_usage_error(
            result,
            f"Parameters '--filename' and '--{inputs[1]}' are incompatible and cannot be used simultaneously.",
        )


def test_sql_fails_if_query_and_stdin_provided(runner):
    result = runner.invoke(["sql", "-q", "fooo", "-i"])
    assert_that_result_is_usage_error(
        result,
        "Parameters '--stdin' and '--query' are incompatible and cannot be used simultaneously. ",
    )


@mock.patch("snowflake.cli._app.snow_connector.connect_to_snowflake")
def test_sql_overrides_connection_configuration(mock_conn, runner, mock_cursor):
    mock_conn.return_value.execute_string.return_value = [mock_cursor(["row"], [])]

    result = runner.invoke(
        [
            "sql",
            "-q",
            "select 1",
            "--connection",
            "connectionName",
            "--accountname",
            "accountnameValue",
            "--username",
            "usernameValue",
            "--dbname",
            "dbnameValue",
            "--schemaname",
            "schemanameValue",
            "--rolename",
            "rolenameValue",
            "--warehouse",
            "warehouseValue",
            "--password",
            "passFromTest",
            "--diag-log-path",
            "/tmp",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output
    mock_conn.assert_called_once_with(
        temporary_connection=False,
        mfa_passcode=None,
        enable_diag=False,
        diag_log_path="/tmp",
        diag_allowlist_path=None,
        connection_name="connectionName",
        account="accountnameValue",
        user="usernameValue",
        password="passFromTest",
        authenticator=None,
        private_key_file=None,
        token_file_path=None,
        session_token=None,
        master_token=None,
        database="dbnameValue",
        schema="schemanameValue",
        role="rolenameValue",
        warehouse="warehouseValue",
    )


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_query")
def test_show_specific_object(mock_execute, mock_cursor):
    mock_columns = ["id", "created_on"]
    mock_row_dict = {c: r for c, r in zip(mock_columns, ["EXAMPLE_ID", "dummy"])}
    cursor = mock_cursor(rows=[mock_row_dict], columns=mock_columns)
    mock_execute.return_value = cursor
    result = SqlExecutionMixin().show_specific_object(
        "objects", "example_id", name_col="id"
    )
    mock_execute.assert_called_once_with(
        r"show objects like 'EXAMPLE\\_ID'", cursor_class=DictCursor
    )
    assert result == mock_row_dict


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_query")
def test_show_specific_object_in_clause(mock_execute, mock_cursor):
    mock_columns = ["name", "created_on"]
    mock_row_dict = {c: r for c, r in zip(mock_columns, ["AbcDef", "dummy"])}
    cursor = mock_cursor(rows=[mock_row_dict], columns=mock_columns)
    mock_execute.return_value = cursor
    result = SqlExecutionMixin().show_specific_object(
        "objects", '"AbcDef"', in_clause="in database mydb"
    )
    mock_execute.assert_called_once_with(
        r"show objects like 'AbcDef' in database mydb", cursor_class=DictCursor
    )
    assert result == mock_row_dict


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_query")
def test_show_specific_object_no_match(mock_execute, mock_cursor):
    mock_columns = ["id", "created_on"]
    mock_row_dict = {c: r for c, r in zip(mock_columns, ["OTHER_ID", "dummy"])}
    cursor = mock_cursor(rows=[mock_row_dict], columns=mock_columns)
    mock_execute.return_value = cursor
    result = SqlExecutionMixin().show_specific_object(
        "objects", "example_id", name_col="id"
    )
    mock_execute.assert_called_once_with(
        r"show objects like 'EXAMPLE\\_ID'", cursor_class=DictCursor
    )
    assert result is None


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_query")
def test_show_specific_object_sql_execution_error(mock_execute):
    cursor = mock.Mock(spec=DictCursor)
    cursor.rowcount = None
    mock_execute.return_value = cursor
    with pytest.raises(SnowflakeSQLExecutionError):
        SqlExecutionMixin().show_specific_object("objects", "example_id", name_col="id")
    mock_execute.assert_called_once_with(
        r"show objects like 'EXAMPLE\\_ID'", cursor_class=DictCursor
    )


@pytest.mark.parametrize(
    "fqn, expected_in_clause",
    [
        (
            FQN(None, None, "func(number, number)"),
            None,
        ),
        (FQN(None, None, "name"), None),
        (FQN(None, "schema", "name"), "in schema schema"),
        (FQN("db", "schema", "name"), "in schema db.schema"),
    ],
)
def test_qualified_name_to_in_clause(fqn, expected_in_clause):
    assert SqlExecutionMixin._qualified_name_to_in_clause(fqn) == (  # noqa: SLF001
        fqn.name,
        expected_in_clause,
    )


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_query")
@mock.patch(
    "snowflake.cli.api.sql_execution.SqlExecutionMixin._qualified_name_to_in_clause"
)
def test_show_specific_object_qualified_name(
    mock_qualified_name_to_in_clause, mock_execute_query, mock_cursor
):
    name = "db.schema.obj"
    unqualified_name = "obj"
    name_in_clause = "in schema db.schema"
    mock_columns = ["name", "created_on"]
    mock_row_dict = {c: r for c, r in zip(mock_columns, [unqualified_name, "date"])}
    cursor = mock_cursor(rows=[mock_row_dict], columns=mock_columns)
    mock_execute_query.return_value = cursor

    mock_qualified_name_to_in_clause.return_value = (unqualified_name, name_in_clause)
    SqlExecutionMixin().show_specific_object("objects", name)
    mock_execute_query.assert_called_once_with(
        f"show objects like {identifier_to_show_like_pattern(unqualified_name)} {name_in_clause}",
        cursor_class=DictCursor,
    )


@mock.patch(
    "snowflake.cli.api.sql_execution.SqlExecutionMixin._qualified_name_to_in_clause"
)
def test_show_specific_object_qualified_name_and_in_clause_error(
    mock_qualified_name_to_in_clause,
):
    object_name = "db.schema.name"
    mock_qualified_name_to_in_clause.return_value = ("name", "in schema db.schema")
    with pytest.raises(SqlExecutionMixin.InClauseWithQualifiedNameError):
        SqlExecutionMixin().show_specific_object(
            "objects", object_name, in_clause="in database db"
        )
    mock_qualified_name_to_in_clause.assert_called_once_with(
        FQN.from_string(object_name)
    )


@mock.patch("snowflake.cli.api.sql_execution.SqlExecutionMixin._execute_query")
def test_show_specific_object_multiple_rows(mock_execute_query):
    cursor = mock.Mock(spec=DictCursor)
    cursor.rowcount = 2
    mock_execute_query.return_value = cursor
    with pytest.raises(ProgrammingError) as err:
        SqlExecutionMixin().show_specific_object("objects", "name", name_col="id")
    assert "Received multiple rows" in err.value.msg
    mock_execute_query.assert_called_once_with(
        r"show objects like 'NAME'", cursor_class=DictCursor
    )


@pytest.mark.parametrize(
    "_object",
    [
        ObjectType.WAREHOUSE,
        ObjectType.ROLE,
        ObjectType.DATABASE,
    ],
)
@mock.patch("snowflake.cli.api.sql_execution.SqlExecutionMixin._execute_query")
def test_use_command(mock_execute_query, _object):
    SqlExecutionMixin().use(object_type=_object, name="foo_name")
    mock_execute_query.assert_called_once_with(f"use {_object.value.sf_name} foo_name")


@pytest.mark.parametrize(
    "query",
    [
        "select &{ aaa }.&{ bbb }",
        "select &aaa.&bbb",
        "select &aaa.&{ bbb }",
        "select <% aaa %>.<% bbb %>",
    ],
)
@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager._execute_string")
def test_rendering_of_sql(mock_execute_query, query, runner):
    result = runner.invoke(["sql", "-q", query, "-D", "aaa=foo", "-D", "bbb=bar"])
    assert result.exit_code == 0, result.output
    mock_execute_query.assert_called_once_with(
        "select foo.bar", cursor_class=VerboseCursor
    )


@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager._execute_string")
def test_old_template_syntax_causes_warning(mock_execute_query, runner):
    result = runner.invoke(["sql", "-q", "select &{ aaa }", "-D", "aaa=foo"])
    assert result.exit_code == 0
    assert (
        "Warning: &{ ... } syntax is deprecated. Use <% ... %> syntax instead."
        in result.output
    )
    mock_execute_query.assert_called_once_with("select foo", cursor_class=VerboseCursor)


@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager._execute_string")
def test_mixed_template_syntax_error(mock_execute_query, runner):
    result = runner.invoke(
        ["sql", "-q", "select <% aaa %>.&{ bbb }", "-D", "aaa=foo", "-D", "bbb=bar"]
    )
    assert result.exit_code == 1
    assert "The SQL query mixes &{ ... } syntax and <% ... %> syntax." in result.output


@pytest.mark.parametrize(
    "query", ["select &{ foo }", "select &foo", "select <% foo %>"]
)
def test_execution_fails_if_unknown_variable(runner, query):
    result = runner.invoke(["sql", "-q", query, "-D", "bbb=1"])
    assert "SQL template rendering error: 'foo' is undefined" in result.output


@pytest.mark.parametrize(
    "text, expected",
    [
        # Test escaping
        ("&&foo", "&foo"),
        ("select *  from &&foo join bar", "select *  from &foo join bar"),
        # Test basic usage
        ("&foo", "&{ foo }"),
        ("select *  from &foo join bar", "select *  from &{ foo } join bar"),
        # Test templating is ignored
        ("&{ foo }", "&{ foo }"),
        ("select *  from &{ foo } join bar", "select *  from &{ foo } join bar"),
    ],
)
def test_snowsql_compatibility(text, expected):
    assert transpile_snowsql_templates(text) == expected


@pytest.mark.parametrize("template_start,template_end", [("&{", "}"), ("<%", "%>")])
@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager._execute_string")
def test_uses_variables_from_snowflake_yml(
    mock_execute_query, project_directory, runner, template_start, template_end
):
    with project_directory("sql_templating"):
        result = runner.invoke(
            ["sql", "-q", f"select {template_start} ctx.env.sf_var {template_end}"]
        )

    assert result.exit_code == 0
    mock_execute_query.assert_called_once_with(
        "select foo_value", cursor_class=VerboseCursor
    )


@pytest.mark.parametrize("template_start,template_end", [("&{", "}"), ("<%", "%>")])
@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager._execute_string")
def test_uses_variables_from_snowflake_local_yml(
    mock_execute_query, project_directory, runner, template_start, template_end
):
    with project_directory("sql_templating"):
        result = runner.invoke(
            [
                "sql",
                "-q",
                f"select {template_start} ctx.env.sf_var_override {template_end}",
            ]
        )

    assert result.exit_code == 0
    mock_execute_query.assert_called_once_with(
        "select foo_value_override", cursor_class=VerboseCursor
    )


@pytest.mark.parametrize("template_start,template_end", [("&{", "}"), ("<%", "%>")])
@mock.patch("snowflake.cli._plugins.sql.commands.SqlManager._execute_string")
def test_uses_variables_from_cli_are_added_outside_context(
    mock_execute_query, project_directory, runner, template_start, template_end
):
    with project_directory("sql_templating"):
        result = runner.invoke(
            [
                "sql",
                "-q",
                f"select {template_start} ctx.env.sf_var {template_end} {template_start} other {template_end}",
                "-D",
                "other=other_value",
            ]
        )

    assert result.exit_code == 0, result.output
    mock_execute_query.assert_called_once_with(
        "select foo_value other_value", cursor_class=VerboseCursor
    )
