import pytest
from time import sleep
from typing import Set


class TableForTesting:
    def __init__(self, runner):
        self.runner = runner
        self.name = "TABLE_BY_CLI_INTEGRATION_TESTS"
        self.col = "COL1"
        result = self.runner.invoke_with_connection(
            ["sql", "-q", f"CREATE TABLE {self.name} ({self.col} VARCHAR)"]
        )
        assert result.exit_code == 0, result.output

    def add_value_query(self, value):
        return f"INSERT INTO {self.name} VALUES ('{value}')"

    def assert_contents(self, expected_contents: Set[str]):
        # as very rarely the result of the last async query is not yet visible on the server side,
        # we wait short amount of time and repeat the check before failing the test
        for time_to_wait in [0, 0.1, 0.3]:
            sleep(time_to_wait)
            result = self.runner.invoke_with_connection_json(
                ["sql", "-q", f"SELECT {self.col} FROM {self.name}"]
            )
            assert result.exit_code == 0, result.output
            contents = set()
            for row in result.json:
                contents.add(row[self.col])
            if contents == expected_contents:
                return
        assert contents == expected_contents


@pytest.mark.integration
def test_only_async_queries(runner, test_database):
    table = TableForTesting(runner)
    result = runner.invoke_with_connection(
        ["sql", "-q", f"{table.add_value_query('single async query')};>"]
    )
    assert result.exit_code == 0, result.output
    table.assert_contents({"single async query"})

    result = runner.invoke_with_connection(
        [
            "sql",
            "-q",
            f"""{table.add_value_query('async query 1')};>
            {table.add_value_query('async query 2')};>
            {table.add_value_query('async query 3')};>
            """,
        ]
    )
    assert result.exit_code == 0, result.output
    table.assert_contents(
        {
            "single async query",
            "async query 1",
            "async query 2",
            "async query 3",
        }
    )


@pytest.mark.integration
def test_mix(runner, test_database):
    table = TableForTesting(runner)

    result = runner.invoke_with_connection(
        [
            "sql",
            "-q",
            f"""{table.add_value_query('async query before')};>
            select 4;
            {table.add_value_query('async query after')};>
            """,
        ]
    )
    assert result.exit_code == 0, result.output
    table.assert_contents(
        {
            "async query before",
            "async query after",
        }
    )

    result = runner.invoke_with_connection(
        [
            "sql",
            "-q",
            f"""{table.add_value_query('async before')};>
            select 15;
            {table.add_value_query('async mid')};>
            select 6;
            {table.add_value_query('async after')};>
            """,
        ]
    )
    assert result.exit_code == 0, result.output
    table.assert_contents(
        {
            "async query before",
            "async query after",
            "async before",
            "async mid",
            "async after",
        }
    )
