import datetime

from snowflake.cli._app.printing import print_structured
from snowflake.cli.api.output.types import QueryResult


def test_print_structured_output_date(mock_cursor, capsys):
    cmd_result = QueryResult(
        cursor=mock_cursor(
            [(datetime.date.fromisoformat("2025-02-17"),)], ["CURRENT_DATE()"]
        )
    )
    print_structured(cmd_result)
    captured = capsys.readouterr()
    assert (
        captured.out
        == """[\n    {\n        "CURRENT_DATE()": "2025-02-17"\n    }\n]\n"""
    )
