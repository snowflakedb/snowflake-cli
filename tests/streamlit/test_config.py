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

from pathlib import Path

import pytest

from src.snowflake.cli.api.project.definition import load_project

TEST_DATA = Path(__file__).parent.parent / "test_data" / "streamlit"
FILE_WITH_LONG_LIST = TEST_DATA / "with_list_in_source_file.yml"
ANOTHER_FILE_WITH_LONG_LIST = TEST_DATA / "another_file_with_list.yml"
FILE_WITH_SINGLE_ITEM_LIST = TEST_DATA / "with_single_item.yml"
ANOTHER_FILE_WITH_SINGLE_ITEM = TEST_DATA / "another_file_with_single_item.yml"


@pytest.mark.parametrize(
    "test_files,expected",
    [
        (
            [FILE_WITH_LONG_LIST, FILE_WITH_SINGLE_ITEM_LIST],
            Path("a_tree_cut_down_with_a_herring"),
        ),
        ([FILE_WITH_SINGLE_ITEM_LIST, FILE_WITH_LONG_LIST], Path("a_shrubbery.py")),
        (
            [FILE_WITH_SINGLE_ITEM_LIST, ANOTHER_FILE_WITH_SINGLE_ITEM],
            Path("another_shrubbery_but_a_bit_higher"),
        ),
        ([FILE_WITH_LONG_LIST, ANOTHER_FILE_WITH_LONG_LIST], Path("boing.py")),
    ],
)
def test_load_project_definition(test_files, expected):

    result = load_project(test_files).project_definition

    assert expected in result.streamlit.additional_source_files
