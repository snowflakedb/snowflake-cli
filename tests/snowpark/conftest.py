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

import json
from unittest import mock

import pytest

from tests.conftest import TEST_DIR


@pytest.fixture
def mock_available_packages_sql_result(mock_ctx, mock_cursor):
    with open(
        TEST_DIR / "test_data/packages_available_in_snowflake_sql_result_rows.json"
    ) as fh:
        result_rows = json.load(fh)
    ctx = mock_ctx(
        mock_cursor(
            columns=["package_name", "version"],
            rows=result_rows,
        )
    )
    with mock.patch(
        "snowflake.cli._app.snow_connector.connect_to_snowflake", return_value=ctx
    ):
        yield


@pytest.fixture
def mock_procedure_description(mock_cursor):
    yield mock_cursor(
        rows=[
            ("signature", "(NAME VARCHAR)"),
            ("returns", "VARCHAR(16777216)"),
            ("language", "PYTHON"),
            ("null handling", "CALLED ON NULL INPUT"),
            ("volatility", "VOLATILE"),
            ("execute as", "CALLER"),
            ("body", None),
            ("imports", "[@FOO.BAR.BAZ/my_snowpark_project/app.zip]"),
            ("handler", "app.hello_procedure"),
            ("runtime_version", "3.10"),
            ("packages", "['snowflake-snowpark-python','pytest<9.0.0,>=7.0.0']"),
            ("installed_packages", "['_libgcc_mutex==0.1']"),
            ("artifact_repository", None),
            ("artifact_repository_packages", None),
        ],
        columns=[
            "signature",
            "returns",
            "language",
            "null handling",
            "volatility",
            "execute as",
            "body",
            "imports",
            "handler",
            "runtime_version",
            "packages",
            "installed_packages",
        ],
    )
