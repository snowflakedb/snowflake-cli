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

from snowflake.cli.api.errno import DCM_DEPLOYMENT_DOES_NOT_EXIST

from tests_common.sql_error_contract import (
    DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    output_contains_snowflake_sql_error,
    snowflake_sql_error_token,
)


def test_snowflake_sql_error_token_formats_errno_and_sqlstate():
    assert (
        snowflake_sql_error_token(
            DCM_DEPLOYMENT_DOES_NOT_EXIST, DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE
        )
        == "001569 (22023)"
    )


def test_output_contains_snowflake_sql_error_matches_server_message():
    output = (
        "001569 (22023): Cannot drop deployment DEPLOYMENT$1 because it does not exist"
    )
    assert output_contains_snowflake_sql_error(
        output,
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )


def test_output_contains_snowflake_sql_error_rejects_brittle_english_only():
    assert not output_contains_snowflake_sql_error(
        "Deployment does not exist",
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )


def test_output_contains_snowflake_sql_error_rejects_empty_and_none():
    assert not output_contains_snowflake_sql_error(
        None,
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )
    assert not output_contains_snowflake_sql_error(
        "",
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )


def test_output_contains_snowflake_sql_error_requires_zero_padded_errno():
    assert not output_contains_snowflake_sql_error(
        "1569 (22023): Cannot drop deployment",
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )


def test_output_contains_snowflake_sql_error_rejects_wrong_sqlstate():
    assert not output_contains_snowflake_sql_error(
        "001569 (42000): Cannot drop deployment",
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )


def test_output_contains_snowflake_sql_error_rejects_wrong_errno():
    assert not output_contains_snowflake_sql_error(
        "002043 (22023): object does not exist",
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )


def test_output_contains_snowflake_sql_error_rejects_errno_without_sqlstate():
    assert not output_contains_snowflake_sql_error(
        "001569: Cannot drop deployment",
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )


def test_output_contains_snowflake_sql_error_matches_whitespace_collapsed_token():
    haystack = "prefix 001569   (22023) : Cannot drop deployment"
    assert output_contains_snowflake_sql_error(
        haystack,
        errno=DCM_DEPLOYMENT_DOES_NOT_EXIST,
        sqlstate=DCM_DEPLOYMENT_DOES_NOT_EXIST_SQLSTATE,
    )
