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

from __future__ import annotations

import re
from contextlib import contextmanager
from typing import Any, Optional
from unittest import mock

import pytest

from tests.testing_utils.fixtures import TEST_DIR
from tests.testing_utils.result_assertions import assert_successful_result_message


@pytest.fixture
def _mock_cortex_result(mock_ctx, mock_cursor):
    @contextmanager
    def _mock(raw_result: Any, expected_query: Optional[str] = None):
        ctx = mock_ctx(
            mock_cursor(
                columns=["CORTEX_RESULT"],
                rows=[{"CORTEX_RESULT": raw_result}],
            )
        )
        with mock.patch("snowflake.connector.connect", return_value=ctx):
            yield
            if expected_query:
                actual_query = re.sub(r"\s+", " ", ctx.get_query()).strip()
                assert expected_query == actual_query

    return _mock


def test_cortex_complete_for_prompt_with_default_model(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="Yes",
        expected_query="SELECT SNOWFLAKE.CORTEX.COMPLETE( 'snowflake-arctic', 'Is 5 more than 4? Please answer using one word without a period.' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "complete",
                "Is 5 more than 4? Please answer using one word without a period.",
            ]
        )
        assert_successful_result_message(result, expected_msg="Yes")


def test_cortex_complete_for_prompt_with_chosen_model(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="Yes",
        expected_query="SELECT SNOWFLAKE.CORTEX.COMPLETE( 'reka-flash', 'Is 5 more than 4? Please answer using one word without a period.' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "complete",
                "Is 5 more than 4? Please answer using one word without a period.",
                "--model",
                "reka-flash",
            ]
        )
        assert_successful_result_message(result, expected_msg="Yes")


def test_cortex_complete_for_file(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="""{"choices": [{"messages": "No, I'm not"}]}""",
        expected_query="""SELECT SNOWFLAKE.CORTEX.COMPLETE( 'snowflake-arctic', PARSE_JSON('[ { "role": "user", "content": "how does a \\\\"snowflake\\\\" get its \\'unique\\' pattern?" }, { "role": "system", "content": "I don\\'t know" }, { "role": "user", "content": "I thought \\\\"you\\\\" are smarter" } ] '), {} ) AS CORTEX_RESULT;""",
    ):
        result = runner.invoke(
            [
                "cortex",
                "complete",
                "--file",
                str(TEST_DIR / "test_data/cortex/conversation.json"),
            ]
        )
        assert_successful_result_message(result, expected_msg="No, I'm not")


def test_cortex_extract_answer_from_cmd_arg(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="""[{ "answer": "blue", "score": 0.81898624 }]""",
        expected_query="SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER( 'John has a car. John\\'s car is blue.', 'What\\'s the color of John\\'s car?' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "extract-answer",
                "What's the color of John's car?",
                "John has a car. John's car is blue.",
            ]
        )
        assert_successful_result_message(result, expected_msg="blue")


def test_cortex_extract_answer_from_file(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="""[{ "answer": "blue", "score": 0.81898624 }]""",
        expected_query="SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER( 'John has a car. John\\'s car is blue. ', 'What\\'s the color of John\\'s car?' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "extract-answer",
                "What's the color of John's car?",
                "--file",
                str(TEST_DIR / "test_data/cortex/short_english_text.txt"),
            ]
        )
        assert_successful_result_message(result, expected_msg="blue")


def test_cortex_sentiment_for_cmd_arg(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result=0.81898624,
        expected_query="SELECT SNOWFLAKE.CORTEX.SENTIMENT( 'John has a car. John\\'s car is blue.' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "sentiment",
                "John has a car. John's car is blue.",
            ]
        )
        assert_successful_result_message(result, expected_msg="0.81898624")


def test_cortex_sentiment_for_file(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result=0.81898624,
        expected_query="SELECT SNOWFLAKE.CORTEX.SENTIMENT( 'John has a car. John\\'s car is blue. ' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "sentiment",
                "--file",
                str(TEST_DIR / "test_data/cortex/short_english_text.txt"),
            ]
        )
        assert_successful_result_message(result, expected_msg="0.81898624")


def test_cortex_summarize_cmd_arg(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="John has a blue car.",
        expected_query="SELECT SNOWFLAKE.CORTEX.SUMMARIZE( 'John has a car. John\\'s car is blue.' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "summarize",
                "John has a car. John's car is blue.",
            ]
        )
        assert_successful_result_message(result, expected_msg="John has a blue car.")


def test_cortex_summarize_file(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="John has a blue car.",
        expected_query="SELECT SNOWFLAKE.CORTEX.SUMMARIZE( 'John has a car. John\\'s car is blue. ' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "summarize",
                "--file",
                str(TEST_DIR / "test_data/cortex/short_english_text.txt"),
            ]
        )
        assert_successful_result_message(result, expected_msg="John has a blue car.")


def test_cortex_translate_arg_cmd_from_detected_language(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="ziołowy",
        expected_query="SELECT SNOWFLAKE.CORTEX.TRANSLATE( 'herb', '', 'pl' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "translate",
                "herb",
                "--to",
                "pl",
            ]
        )
        assert_successful_result_message(result, expected_msg="ziołowy")


def test_cortex_translate_arg_cmd_from_chosen_language(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="coat of arms",
        expected_query="SELECT SNOWFLAKE.CORTEX.TRANSLATE( 'herb', 'pl', 'en' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "translate",
                "herb",
                "--from",
                "pl",
                "--to",
                "en",
            ]
        )
        assert_successful_result_message(result, expected_msg="coat of arms")


def test_cortex_translate_file(_mock_cortex_result, runner):
    with _mock_cortex_result(
        raw_result="John ma samochód. Samochód Johna jest niebieski.",
        expected_query="SELECT SNOWFLAKE.CORTEX.TRANSLATE( 'John has a car. John\\'s car is blue. ', '', 'pl' ) AS CORTEX_RESULT;",
    ):
        result = runner.invoke(
            [
                "cortex",
                "translate",
                "--file",
                str(TEST_DIR / "test_data/cortex/short_english_text.txt"),
                "--to",
                "pl",
            ]
        )
        assert_successful_result_message(
            result, expected_msg="John ma samochód. Samochód Johna jest niebieski."
        )


@mock.patch("snowflake.cli.plugins.cortex.commands.SEARCH_COMMAND_ENABLED", new=False)
def test_if_search_raises_exception_for_312(runner, os_agnostic_snapshot):

    result = runner.invoke(
        [
            "cortex",
            "search",
            "parrot",
            "--service",
            "test_service",
            "--columns",
            "region",
        ]
    )
    assert result.exit_code == 1
    assert result.output == os_agnostic_snapshot
