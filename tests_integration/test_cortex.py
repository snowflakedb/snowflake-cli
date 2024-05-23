import pytest

from tests_integration.conftest import TEST_DIR
from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_successful_result_message,
    assert_that_result_is_successful,
)


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
