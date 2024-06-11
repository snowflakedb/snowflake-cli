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

import sys

import pytest

from tests_integration.conftest import TEST_DIR
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_successful_result_message,
    assert_that_result_is_successful,
)


@pytest.mark.integration
@pytest.mark.skip  # TODO: when we have Cortex activated on test account, unskip this and setup part in sql script
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Snowflake Python API currently does not support Python 3.12 and greater",
)
def test_cortex_search(runner):
    result = runner.invoke_with_connection_json(
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

    expected_result = [
        {
            "": "This parrot is no more! It has ceased to be! It`s expired and gone to meet its maker!",
            "region": "Flying Circus",
        }
    ]
    assert result.exit_code == 0
    assert result.json == expected_result


@pytest.mark.integration
def test_cortex_complete_for_prompt(runner):
    result = runner.invoke_with_connection(
        [
            "cortex",
            "complete",
            "Is 5 more than 4? Please answer using one word without a period.",
        ]
    )
    assert_successful_result_message(result, expected_msg="Yes")


@pytest.mark.integration
def test_cortex_complete_for_conversation(runner):
    result = runner.invoke_with_connection(
        [
            "cortex",
            "complete",
            "--file",
            str(TEST_DIR / "test_data/cortex/conversation.json"),
        ]
    )
    assert_successful_result_message(result, expected_msg="Yes")


@pytest.mark.integration
def test_cortex_extract_answer(runner):
    result = runner.invoke_with_connection(
        [
            "cortex",
            "extract-answer",
            "What's the color of John's car?",
            "John has a car. John's car is blue.",
        ]
    )
    assert_successful_result_message(result, expected_msg="blue")


@pytest.mark.integration
def test_cortex_sentiment(runner):
    result = runner.invoke_with_connection(
        [
            "cortex",
            "sentiment",
            "--file",
            str(TEST_DIR / "test_data/cortex/english_text.txt"),
        ]
    )
    assert_that_result_is_successful(result)
    sentiment_value = float(result.output)
    assert sentiment_value >= -1
    assert sentiment_value <= 1


@pytest.mark.integration
def test_cortex_summarize(runner):
    result = runner.invoke_with_connection(
        [
            "cortex",
            "summarize",
            "--file",
            str(TEST_DIR / "test_data/cortex/english_text.txt"),
        ]
    )
    assert_that_result_is_successful(result)
    summary_result = result.output
    assert len(summary_result) > 0
    assert "cortex" in summary_result.lower()


@pytest.mark.integration
def test_cortex_translate_from_detected_language(runner):
    result = runner.invoke_with_connection(
        [
            "cortex",
            "translate",
            "niebieski",
            "--to",
            "en",
        ]
    )
    assert_successful_result_message(result, expected_msg="blue")


@pytest.mark.integration
def test_cortex_translate_from_chosen_language(runner):
    result = runner.invoke_with_connection(
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
