from pathlib import Path

import pytest

from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_is_successful,
    assert_that_result_is_successful_and_output_json_contains,
)


@pytest.fixture(scope="module")
def install_plugins():
    import subprocess

    path = Path(__file__).parent.parent / "test_external_plugins"
    subprocess.check_call(["pip", "install", path / "multilingual_hello_command_group"])
    subprocess.check_call(["pip", "install", path / "snowpark_hello_single_command"])


@pytest.mark.integration
def test_loading_of_installed_plugins_if_all_plugins_enabled(
    runner, install_plugins, caplog
):
    runner.use_config("config_with_enabled_all_external_plugins.toml")

    result_of_top_level_help = runner.invoke_with_config(["--help"])
    assert_that_result_is_successful(result_of_top_level_help)
    _assert_that_no_error_logs(caplog)
    assert "multilingual-hello" in result_of_top_level_help.output

    result_of_multilingual_hello_help = runner.invoke_with_config(
        ["multilingual-hello", "--help"]
    )
    assert_that_result_is_successful(result_of_multilingual_hello_help)
    _assert_that_no_error_logs(caplog)
    assert "hello-en" in result_of_multilingual_hello_help.output
    assert "hello-fr" in result_of_multilingual_hello_help.output

    result_of_multilingual_hello_fr_help = runner.invoke_with_config(
        ["multilingual-hello", "hello-fr", "--help"]
    )
    assert_that_result_is_successful(result_of_multilingual_hello_fr_help)
    _assert_that_no_error_logs(caplog)
    assert "Says hello in French" in result_of_multilingual_hello_fr_help.output
    assert "Your name" in result_of_multilingual_hello_fr_help.output

    result_of_snowpark_help = runner.invoke_with_config(["snowpark", "--help"])
    assert_that_result_is_successful(result_of_snowpark_help)
    _assert_that_no_error_logs(caplog)
    assert "hello" in result_of_snowpark_help.output
    assert "Says hello" in result_of_snowpark_help.output

    result_of_snowpark_hello_help = runner.invoke_with_config(
        ["snowpark", "hello", "--help"]
    )
    assert_that_result_is_successful(result_of_snowpark_hello_help)
    _assert_that_no_error_logs(caplog)
    assert "Your name" in result_of_snowpark_hello_help.output

    result_of_snowpark_hello = runner.invoke_with_connection_json(
        ["snowpark", "hello", "John"]
    )
    assert_that_result_is_successful_and_output_json_contains(
        result_of_snowpark_hello, {"GREETING": "Hello John! You are in Snowpark!"}
    )

    result_of_multilingual_hello_en = runner.invoke_with_connection_json(
        ["multilingual-hello", "hello-en", "John"]
    )
    assert_that_result_is_successful_and_output_json_contains(
        result_of_multilingual_hello_en, {"GREETING": "Hello John!"}
    )

    result_of_multilingual_hello_fr = runner.invoke_with_connection_json(
        ["multilingual-hello", "hello-fr", "John"]
    )
    assert_that_result_is_successful_and_output_json_contains(
        result_of_multilingual_hello_fr, {"GREETING": "Salut John!"}
    )


@pytest.mark.integration
def test_loading_of_installed_plugins_if_only_one_plugin_is_enabled(
    runner, install_plugins, caplog
):
    runner.use_config("config_with_enabled_only_one_external_plugin.toml")

    result_of_top_level_help = runner.invoke_with_config(["--help"])
    assert_that_result_is_successful(result_of_top_level_help)
    _assert_that_no_error_logs(caplog)
    assert "multilingual-hello" not in result_of_top_level_help.output

    result_of_snowpark_hello = runner.invoke_with_connection_json(
        ["snowpark", "hello", "John"]
    )
    assert_that_result_is_successful_and_output_json_contains(
        result_of_snowpark_hello, {"GREETING": "Hello John! You are in Snowpark!"}
    )


def _assert_that_no_error_logs(caplog):
    error_logs = [
        record.message for record in caplog.records if record.levelname == "ERROR"
    ]
    assert error_logs == []
