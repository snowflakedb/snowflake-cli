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

from dataclasses import dataclass
from typing import Any, Dict, List
from unittest import mock

import pytest
from snowflake.cli.api.rest_api import CannotDetermineCreateURLException, RestApi
from snowflake.connector.errors import InterfaceError

_DUMMY_SERVER_URL = "https://DUMMY_SERVER_URL"


@dataclass
class _RestApiCallMatch:
    method: str
    url: str

    def assert_matches_kwargs(self, kwargs: Dict[str, Any]):
        assert kwargs["method"] == self.method
        assert kwargs["full_url"] == self.url


@pytest.fixture()
def mock_rest_connection():
    class _MockRestConnection:
        def __init__(self):
            self.connection = None
            self.rest = None

        def setup(
            self,
            fetch_return_value=None,
            fetch_side_effects=None,
        ):
            rest = mock.MagicMock()
            rest.server_url = _DUMMY_SERVER_URL
            if fetch_side_effects is not None:
                rest.fetch.side_effect = fetch_side_effects
            if fetch_return_value is not None:
                rest.fetch.return_value = fetch_return_value
            connection = mock.MagicMock()
            connection.rest = rest

            self.connection = connection
            self.rest = rest

            return connection

        def assert_rest_fetch_calls_matches(
            self, call_matches: List[_RestApiCallMatch]
        ):
            fetch_calls = [
                call for call in self.rest.mock_calls if call.fetch is not None
            ]
            assert len(call_matches) == len(fetch_calls)
            for call_match, fetch_call in zip(call_matches, fetch_calls):
                call_match.assert_matches_kwargs(fetch_call.kwargs)

    yield _MockRestConnection()


@pytest.mark.parametrize(
    "return_value,expected",
    [([], True), (["an object"], True), ({"some": "data"}, True), ({}, False)],
)
def test_endpoint_exists(mock_rest_connection, return_value, expected):
    mock_rest_connection.setup(fetch_return_value=return_value)
    rest_api = RestApi(mock_rest_connection)
    assert rest_api.get_endpoint_exists("/dummy_url") == expected
    mock_rest_connection.assert_rest_fetch_calls_matches(
        [_RestApiCallMatch(url=f"{_DUMMY_SERVER_URL}/dummy_url", method="get")]
    )


def test_endpoint_exists_handles_404(
    mock_rest_connection,
):
    mock_rest_connection.setup(fetch_side_effects=[InterfaceError("404 Not Found")])
    rest_api = RestApi(mock_rest_connection)
    assert not rest_api.get_endpoint_exists("/dummy_url")
    mock_rest_connection.assert_rest_fetch_calls_matches(
        [_RestApiCallMatch(url=f"{_DUMMY_SERVER_URL}/dummy_url", method="get")]
    )


@pytest.mark.parametrize("number_of_fails", range(4))
def test_determine_create_url(mock_rest_connection, number_of_fails):
    fetch_side_effects = [InterfaceError("404 Not Found")] * number_of_fails + [[]]
    mock_rest_connection.setup(fetch_side_effects=fetch_side_effects)
    mock_rest_connection.connection.database = "DB"
    mock_rest_connection.connection.schema = "SCHEMA"

    a_type = "a_type"
    urls = [
        f"/api/v2/{a_type}s/",
        f"/api/v2/databases/DB/{a_type}s/",
        f"/api/v2/databases/DB/schemas/SCHEMA/{a_type}s/",
    ]

    rest = RestApi(mock_rest_connection.connection)
    # mock additional check
    rest._fetch_endpoint_exists = lambda _: True  # noqa: SLF001

    try:
        result = rest.determine_url_for_create_query(a_type)
        assert result == urls[number_of_fails]
    except CannotDetermineCreateURLException:
        assert number_of_fails == 3

    mock_rest_connection.assert_rest_fetch_calls_matches(
        [
            _RestApiCallMatch(url=f"{_DUMMY_SERVER_URL}{url}", method="get")
            for url in urls[: number_of_fails + 1]
            if url
        ]
    )
