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

import time
from unittest import mock

import pytest

from tests_integration.testing_utils import ObjectNameProvider


@pytest.mark.integration
def test_query_parameter(runner):
    result = runner.invoke_with_connection_json(["sql", "-q", "select pi()"])

    assert result.exit_code == 0
    assert _round_values(result.json) == [{"PI()": 3.14}]


@pytest.mark.integration
def test_serializing_various_datatypes_to_json(runner):
    sql = """
    SELECT 
        12::NUMBER as number_col, 
        12.345::FLOAT as float_col, 
        TO_BINARY('THIS SHOULD WORK', 'UTF-8') as binary_col,
        true::BOOLEAN as bool_true_col,
        false::BOOLEAN as bool_false_col,
        '2024-01-01'::DATE as date_col,
        '11:22:33'::TIME as time_col,
        '2024-01-01 11:22:33'::DATETIME as datetime_col
        
    """
    result = runner.invoke_with_connection_json(["sql", "-q", sql])

    assert result.exit_code == 0
    assert result.json == [
        {
            "NUMBER_COL": 12,
            "FLOAT_COL": 12.345,
            "BOOL_TRUE_COL": True,
            "BOOL_FALSE_COL": False,
            "DATE_COL": "2024-01-01",
            "TIME_COL": "11:22:33",
            "DATETIME_COL": "2024-01-01T11:22:33",
            "BINARY_COL": "544849532053484f554c4420574f524b",
        }
    ]


@pytest.mark.integration
def test_multi_queries_from_file(runner, test_root_path):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-f",
            f"{test_root_path}/test_data/sql_multi_queries.sql",
        ]
    )

    assert result.exit_code == 0
    assert _round_values_for_multi_queries(result.json) == [
        [{"ROUND(LN(1), 4)": 0.0}],
        [{"ROUND(LN(10), 4)": 2.3}],
        [{"ROUND(LN(100), 4)": 4.61}],
    ]


@pytest.mark.integration
def test_multiple_files(runner, test_root_path, snapshot):
    query_file = f"{test_root_path}/test_data/sql_multi_queries.sql"
    result = runner.invoke_with_connection(
        [
            "sql",
            "-f",
            query_file,
            "-f",
            f"{test_root_path}/test_data/empty.sql",
            "-f",
            query_file,
        ]
    )

    assert result.exit_code == 0
    assert result.output == snapshot


@pytest.mark.integration
def test_multi_queries_where_one_of_them_is_failing(runner, test_root_path):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", f"select 1; select 2; select foo; select 4", "--format", "json"],
    )
    assert result.exit_code == 1

    assert '"1"  :   1' in result.output
    assert '"2"  :   2' in result.output
    assert "invalid identifier 'FOO'" in result.output


@pytest.mark.integration
def test_multi_input_from_stdin(runner, test_root_path):
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-i",
        ],
        input="select 1, 2, 3 union select 4, 5, 6; select 42",
    )
    assert result.exit_code == 0
    assert result.json == [
        [{"1": 1, "2": 2, "3": 3}, {"1": 4, "2": 5, "3": 6}],
        [{"42": 42}],
    ]


def _round_values(results):
    for result in results:
        for k, v in result.items():
            result[k] = round(v, 2)
    return results


def _round_values_for_multi_queries(results):
    return [_round_values(r) for r in results]


@pytest.mark.integration
@mock.patch("snowflake.cli._app.printing._get_table")
@mock.patch("snowflake.cli._app.printing.Live")
def test_queries_are_streamed_to_output(
    _, mock_get_table, runner, capsys, test_root_path
):
    log = []

    def _log(
        *args,
    ):
        log.append((time.monotonic(), *args))

    # We mock add_row by a function that logs call time, later we compare call times
    # to make sure those were separated in time
    mock_get_table().add_row = _log

    runner.invoke_with_connection(
        [
            "sql",
            "-q",
            "select 13; select system$wait(10);",
        ],
    )

    assert len(log) == 2
    (time_0, query_0), (time_1, query_1) = log

    assert query_0 == "13"
    assert time_1 - time_0 >= 10.0
    assert "waited 10 seconds" in query_1


@pytest.mark.integration
@pytest.mark.parametrize(
    "query, expected",
    (
        pytest.param(
            "select 1; -- trailing comment\n",
            [
                {"1": 1},
            ],
            id="single query",
        ),
        pytest.param(
            "select 1; --comment\n select 2; \n -- trailing comment\n",
            [
                [
                    {"1": 1},
                ],
                [
                    {"2": 2},
                ],
            ],
        ),
    ),
)
def test_trailing_comments_queries(runner, query, expected, test_root_path):
    result = runner.invoke_with_connection_json(
        ["sql", "-q", query, "--format", "JSON"]
    )
    assert result.exit_code == 0
    assert result.json == expected, result.json


@pytest.mark.integration
def test_sql_execute_query_prints_query(runner):
    result = runner.invoke_with_connection(
        ["sql", "-q", "select 1 as A; select 2 as B"]
    )

    assert result.exit_code == 0, result.output
    assert "select 1 as A" in result.output
    assert "select 2 as B" in result.output


@pytest.mark.integration_experimental
def test_sql_large_lobs_in_memory_tables(runner):
    table_name = ObjectNameProvider(
        "table_with_default"
    ).create_and_get_next_object_name()
    result = runner.invoke_with_connection(
        [
            "sql",
            "-q",
            f"create or replace table {table_name}(x int, v text default x::varchar);"
            f"select get_ddl('table', '{table_name}');"
            f"drop table {table_name};",
        ]
    )

    assert "VARCHAR(134217728)" in result.output


@pytest.mark.integration
@pytest.mark.parametrize(
    "template",
    [
        "<% ctx.env.monty_python %>",
        "&{ ctx.env.monty_python }",
    ],
)
def test_sql_with_variables_from_project(runner, project_directory, template):
    with project_directory("sql"):
        result = runner.invoke_with_connection_json(
            [
                "sql",
                "-q",
                f"select '{template}' as var",
            ]
        )
        assert result.exit_code == 0, result.output
        assert result.json == [{"VAR": "Knights of Nii"}]


@pytest.mark.integration
def test_sql_ec(runner):
    result = runner.invoke_with_connection(
        [
            "sql",
            "--enhanced-exit-codes",
            "-q",
            "select a",
        ],
    )
    assert result.exit_code == 5, result


@pytest.mark.integration
def test_inner_json_nested_object_extraction(runner):
    """Test extracting nested JSON objects as proper JSON structures."""
    nested_json = (
        '{"user": {"profile": {"name": "Alice", "settings": {"theme": "dark"}}}}'
    )
    result = runner.invoke_with_connection_json_ext(
        [
            "sql",
            "-q",
            f"SELECT GET(PARSE_JSON('{nested_json}'), 'user') as user_object",
        ]
    )

    assert result.exit_code == 0
    assert len(result.json) == 1

    # Verify that nested objects are returned as proper JSON structures
    user_obj = result.json[0]["USER_OBJECT"]

    # Check that user_object contains the complete user structure with nested profile
    assert isinstance(user_obj, dict)
    assert "profile" in user_obj

    # Verify the nested profile structure is preserved
    profile = user_obj["profile"]
    assert isinstance(profile, dict)
    assert profile["name"] == "Alice"
    assert "settings" in profile

    # Verify the deeply nested settings structure is preserved
    settings = profile["settings"]
    assert isinstance(settings, dict)
    assert settings["theme"] == "dark"


@pytest.mark.integration
def test_array_construct_and_object_construct(runner):
    """Test ARRAY_CONSTRUCT and OBJECT_CONSTRUCT functions return proper JSON structures."""
    result = runner.invoke_with_connection_json_ext(
        [
            "sql",
            "-q",
            "SELECT ARRAY_CONSTRUCT(1, 2, 3) as a, OBJECT_CONSTRUCT('foo', 'XXXX', 'bar', 42) as o",
        ]
    )

    assert result.exit_code == 0
    assert len(result.json) == 1

    # Verify that the array is returned as a proper list
    array_result = result.json[0]["A"]
    assert isinstance(array_result, list)
    assert array_result == [1, 2, 3]

    # Verify that the object is returned as a proper dictionary
    object_result = result.json[0]["O"]
    assert isinstance(object_result, dict)
    assert object_result == {"foo": "XXXX", "bar": 42}


@pytest.mark.integration
def test_nested_json_backward_compatibility(runner):
    """Test that without JSON_EXT format, JSON objects are returned as strings (backward compatibility)."""
    nested_json = (
        '{"user": {"profile": {"name": "Alice", "settings": {"theme": "dark"}}}}'
    )
    result = runner.invoke_with_connection_json(
        ["sql", "-q", f"SELECT GET(PARSE_JSON('{nested_json}'), 'user') as user_object"]
    )

    assert result.exit_code == 0
    assert len(result.json) == 1

    # Verify that nested objects are returned as strings (original behavior)
    user_obj = result.json[0]["USER_OBJECT"]

    # Should be a string, not a dict
    assert isinstance(user_obj, str)

    # The string should contain JSON content
    assert "profile" in user_obj
    assert "Alice" in user_obj
    assert "dark" in user_obj
