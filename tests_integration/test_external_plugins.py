import pytest

from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful,
    assert_that_result_is_successful_and_output_json_contains,
)
from tests_integration.snowflake_connector import snowflake_session


@pytest.mark.integration
def test_loading_of_installed_plugins_if_all_plugins_enabled(runner, snowflake_session):
    runner.use_config("config_with_enabled_all_external_plugins.toml")

    result_of_top_level_help = runner.invoke_with_config(["--help"])
    assert_that_result_is_successful(result_of_top_level_help)
    assert "multilingual-hello" in result_of_top_level_help.output

    result_of_multilingual_hello_help = runner.invoke_with_config(
        ["multilingual-hello", "--help"]
    )
    assert_that_result_is_successful(result_of_multilingual_hello_help)
    assert "hello-en" in result_of_multilingual_hello_help.output
    assert "hello-fr" in result_of_multilingual_hello_help.output

    result_of_multilingual_hello_fr_help = runner.invoke_with_config(
        ["multilingual-hello", "hello-fr", "--help"]
    )
    assert_that_result_is_successful(result_of_multilingual_hello_fr_help)
    assert "Says hello in French" in result_of_multilingual_hello_fr_help.output
    assert "Your name" in result_of_multilingual_hello_fr_help.output

    result_of_snowpark_help = runner.invoke_with_config(["snowpark", "--help"])
    assert_that_result_is_successful(result_of_snowpark_help)
    assert "hello" in result_of_snowpark_help.output
    assert "Says hello" in result_of_snowpark_help.output

    result_of_snowpark_hello_help = runner.invoke_with_config(
        ["snowpark", "hello", "--help"]
    )
    assert_that_result_is_successful(result_of_snowpark_hello_help)
    assert "Your name" in result_of_snowpark_hello_help.output

    result_of_snowpark_hello = runner.invoke_integration(["snowpark", "hello", "John"])
    assert_that_result_is_successful_and_output_json_contains(
        result_of_snowpark_hello, {"GREETING": "Hello John! You are in Snowpark!"}
    )

    result_of_multilingual_hello_en = runner.invoke_integration(
        ["multilingual-hello", "hello-en", "John"]
    )
    assert_that_result_is_successful_and_output_json_contains(
        result_of_multilingual_hello_en, {"GREETING": "Hello John!"}
    )

    result_of_multilingual_hello_fr = runner.invoke_integration(
        ["multilingual-hello", "hello-fr", "John"]
    )
    assert_that_result_is_successful_and_output_json_contains(
        result_of_multilingual_hello_fr, {"GREETING": "Salut John!"}
    )


@pytest.mark.integration
def test_loading_of_installed_plugins_if_only_one_plugin_is_enabled(
    runner, snowflake_session
):
    runner.use_config("config_with_enabled_only_one_external_plugin.toml")

    result_of_top_level_help = runner.invoke_with_config(["--help"])
    assert_that_result_is_successful(result_of_top_level_help)
    assert "multilingual-hello" not in result_of_top_level_help.output

    result_of_snowpark_hello = runner.invoke_integration(["snowpark", "hello", "John"])
    assert_that_result_is_successful_and_output_json_contains(
        result_of_snowpark_hello, {"GREETING": "Hello John! You are in Snowpark!"}
    )
