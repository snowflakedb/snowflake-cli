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

from textwrap import dedent

import pytest


@pytest.mark.usefixtures("faker_app")
def test_table_result_with_silent_enabled(runner):
    expected_output = dedent(
        """\
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
def test_table_result_with_silent_disabled(runner):
    expected_output = dedent(
        """\
        Enter
          Faker. Teeny Tiny step: UNO UNO
        Exit
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
