from textwrap import dedent

import pytest


@pytest.mark.usefixtures("faker_app")
def test_format_enables_silent(runner):
    expected_output = dedent(
        """\
        SELECT A MOCK QUERY
        +---------------------------------------------------------------------+
        | string | number | array     | object          | date                |
        |--------+--------+-----------+-----------------+---------------------|
        | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
        | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
        +---------------------------------------------------------------------+
        """
    )

    result = runner.invoke(["Faker", "--silent"])
    assert result.output == expected_output, result.output


@pytest.mark.usefixtures("faker_app")
def test_intermediate_output_and_result(runner):
    expected_output = dedent(
        """\
        Faker. Phase UNO.
          Faker. Teeny Tiny step: UNO UNO
        SELECT A MOCK QUERY
        +---------------------------------------------------------------------+
        | string | number | array     | object          | date                |
        |--------+--------+-----------+-----------------+---------------------|
        | string | 42     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
        | string | 43     | ['array'] | {'k': 'object'} | 2022-03-21 00:00:00 |
        +---------------------------------------------------------------------+
        """
    )

    result = runner.invoke(["Faker"])
    assert result.output == expected_output, result.output


@pytest.mark.usefixtures("faker_app")
def test_json_format_disables_intermediate_output(runner):
    expected_output = [
        {
            "string": "string",
            "number": 42,
            "array": ["array"],
            "object": {"k": "object"},
            "date": "2022-03-21T00:00:00",
        },
        {
            "string": "string",
            "number": 43,
            "array": ["array"],
            "object": {"k": "object"},
            "date": "2022-03-21T00:00:00",
        },
    ]

    result = runner.invoke(["Faker", "--format", "JSON"])
    import json

    assert json.loads(result.output) == expected_output
