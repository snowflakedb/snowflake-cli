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

"""
Tests for DECFLOAT data type support and decimal precision configuration.
"""

import os
from decimal import getcontext
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.qa_only,
]


@pytest.fixture
def reset_decimal_precision():
    """Reset decimal precision before and after each test."""
    original_prec = getcontext().prec
    yield
    getcontext().prec = original_prec


def test_decfloat_values(runner, reset_decimal_precision):
    """Test DECFLOAT type with various value types: positive/negative, maximum/minimum, floating point."""

    sql = """
        SELECT 
            CAST('123456789012345678901234567890.123456789' AS DECFLOAT) AS positive_value,
            CAST('-123456789012345678901234567890.123456789' AS DECFLOAT) AS negative_value,
            CAST('3.14159265358979323846264338327950288419' AS DECFLOAT) AS floating_point,
            CAST('99999999999999999999999999999999999999e16384' AS DECFLOAT) AS maximum_value,
            CAST('-99999999999999999999999999999999999999e16384' AS DECFLOAT) AS minimum_value,
    """

    result = runner.invoke_with_connection_json(["sql", "-q", sql])
    assert result.exit_code == 0, f"Failed to select DECFLOAT values: {result.output}"

    # Verify JSON response contains expected values
    assert len(result.json) == 1
    row = result.json[0]

    # Assert exact values that Snowflake returns for DECFLOAT
    assert row["POSITIVE_VALUE"] == "1.234567890123456789012345679E+29"
    assert row["NEGATIVE_VALUE"] == "-1.234567890123456789012345679E+29"
    assert (
        row["FLOATING_POINT"] == "3.141592653589793238462643383"
    )  # value is rounded up to 28 numbers
    assert row["MAXIMUM_VALUE"] == "1.000000000000000000000000000E+16422"
    assert row["MINIMUM_VALUE"] == "-1.000000000000000000000000000E+16422"


def test_decfloat_values_precision_38(runner, reset_decimal_precision):
    """Test DECFLOAT type with precision=38 for higher precision decimal operations."""

    sql = """
        SELECT 
            CAST('123456789012345678901234567890.123456789' AS DECFLOAT) AS positive_value,
            CAST('-123456789012345678901234567890.123456789' AS DECFLOAT) AS negative_value,
            CAST('3.14159265358979323846264338327950288419' AS DECFLOAT) AS floating_point,
            CAST('99999999999999999999999999999999999999e16384' AS DECFLOAT) AS maximum_value,
            CAST('-99999999999999999999999999999999999999e16384' AS DECFLOAT) AS minimum_value
    """

    result = runner.invoke_with_connection_json(
        ["sql", "-q", sql, "--decimal-precision", "38"]
    )
    assert (
        result.exit_code == 0
    ), f"Failed to select DECFLOAT values with precision=38: {result.output}"

    assert len(result.json) == 1
    row = result.json[0]

    assert row["POSITIVE_VALUE"] == "123456789012345678901234567890.12345679"
    assert row["NEGATIVE_VALUE"] == "-123456789012345678901234567890.12345679"
    assert row["FLOATING_POINT"] == "3.1415926535897932384626433832795028842"
    assert row["MAXIMUM_VALUE"] == "9.9999999999999999999999999999999999999E+16421"
    assert row["MINIMUM_VALUE"] == "-9.9999999999999999999999999999999999999E+16421"


def test_decimal_precision_environment_variable(runner, reset_decimal_precision):
    """Test decimal precision using SNOWFLAKE_DECIMAL_PRECISION environment variable."""

    sql = """
        SELECT 
            CAST('1234.56789012345678901234567890' AS DECFLOAT) AS test_value,
            CAST('3.14159265358979323846' AS DECFLOAT) AS pi_value
    """

    original_env = os.environ.get("SNOWFLAKE_DECIMAL_PRECISION")
    os.environ["SNOWFLAKE_DECIMAL_PRECISION"] = "10"

    try:
        result = runner.invoke_with_connection_json(["sql", "-q", sql])
        assert (
            result.exit_code == 0
        ), f"Failed to execute SQL with env var precision: {result.output}"

        assert len(result.json) == 1
        row = result.json[0]

        assert row["TEST_VALUE"] == "1234.567890"
        assert row["PI_VALUE"] == "3.141592654"

    finally:
        if original_env is not None:
            os.environ["SNOWFLAKE_DECIMAL_PRECISION"] = original_env
        else:
            os.environ.pop("SNOWFLAKE_DECIMAL_PRECISION", None)


def test_decimal_precision_param_overrides_env(runner, reset_decimal_precision):
    """Test that CLI parameter takes precedence over environment variable."""

    sql = """
        SELECT 
            CAST('1234.56789012345678901234567890' AS DECFLOAT) AS test_value,
            CAST('3.14159265358979323846' AS DECFLOAT) AS pi_value
    """

    original_env = os.environ.get("SNOWFLAKE_DECIMAL_PRECISION")
    os.environ["SNOWFLAKE_DECIMAL_PRECISION"] = "25"

    try:
        result = runner.invoke_with_connection_json(
            ["sql", "-q", sql, "--decimal-precision", "5"]
        )
        assert (
            result.exit_code == 0
        ), f"Failed to execute SQL with param override: {result.output}"

        assert len(result.json) == 1
        row = result.json[0]

        assert row["TEST_VALUE"] == "1234.6"
        assert row["PI_VALUE"] == "3.1416"

    finally:
        if original_env is not None:
            os.environ["SNOWFLAKE_DECIMAL_PRECISION"] = original_env
        else:
            os.environ.pop("SNOWFLAKE_DECIMAL_PRECISION", None)


def test_decimal_precision_from_config(runner, reset_decimal_precision, temp_dir):
    """Test decimal precision reading from config.toml file."""
    sql = """
        SELECT 
            CAST('1234.56789012345678901234567890' AS DECFLOAT) AS test_value,
            CAST('3.14159265358979323846' AS DECFLOAT) AS pi_value
    """

    config_path = Path(temp_dir) / "test_config.toml"
    config_path.write_text(
        "[cli]\n"
        "decimal_precision = 15\n"
        "\n"
        "[connections.integration]\n"
        f'account = "{os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT", "")}"\n'
        f'user = "{os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_USER", "")}"\n'
    )

    result = runner.invoke_json(
        ["--config-file", str(config_path), "sql", "-q", sql, "-c", "integration"]
    )
    assert (
        result.exit_code == 0
    ), f"Failed to execute SQL with config file precision: {result.output}"

    assert len(result.json) == 1
    row = result.json[0]

    assert row["TEST_VALUE"] == "1234.56789012346"
    assert row["PI_VALUE"] == "3.14159265358979"


def test_decimal_precision_cli_overrides_config(
    runner, reset_decimal_precision, temp_dir
):
    """Test that CLI parameter takes precedence over config file setting."""
    sql = """
        SELECT 
            CAST('1234.56789012345678901234567890' AS DECFLOAT) AS test_value
    """

    config_path = Path(temp_dir) / "test_config.toml"
    config_path.write_text(
        "[cli]\n"
        "decimal_precision = 20\n"
        "\n"
        "[connections.integration]\n"
        f'account = "{os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT", "")}"\n'
        f'user = "{os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_USER", "")}"\n'
    )

    result = runner.invoke_json(
        [
            "--config-file",
            str(config_path),
            "sql",
            "-q",
            sql,
            "-c",
            "integration",
            "--decimal-precision",
            "8",
        ]
    )
    assert (
        result.exit_code == 0
    ), f"Failed to execute SQL with CLI override: {result.output}"

    assert len(result.json) == 1
    row = result.json[0]

    assert row["TEST_VALUE"] == "1234.5679"
