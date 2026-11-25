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

from types import SimpleNamespace

import pytest

from tests_integration.testing_utils.assertions.test_result_assertions import (
    assert_that_result_failed_with_message_containing,
    extract_error_box_message,
)


def test_extract_error_box_message_from_structured_json():
    raw = (
        '{\n    "error": "Source path must be a relative path",\n'
        '    "type": "ArtifactError",\n    "exit_code": 1\n}\n'
    )

    assert extract_error_box_message(raw) == "Source path must be a relative path"


def test_extract_error_box_message_from_rich_panel():
    raw = (
        "╭─ Error ──────────────────────────────────────────╮\n"
        "│ Source path must be a relative path              │\n"
        "╰──────────────────────────────────────────────────╯\n"
    )

    assert extract_error_box_message(raw) == "Source path must be a relative path"


def test_assert_failed_with_message_uses_parsed_json_payload():
    result = SimpleNamespace(
        exit_code=1,
        json={
            "error": "Multiple entities of type streamlit found. Please provide entity id for the operation.",
            "type": "UsageError",
            "exit_code": 2,
        },
        output='{"error": "Multiple entities of type streamlit found. Please provide entity id for the operation."}',
    )

    assert_that_result_failed_with_message_containing(
        result, "Multiple entities of type streamlit found"
    )


def test_assert_failed_with_message_rejects_success():
    result = SimpleNamespace(exit_code=0, json=None, output="ok")

    with pytest.raises(AssertionError):
        assert_that_result_failed_with_message_containing(result, "unused")
