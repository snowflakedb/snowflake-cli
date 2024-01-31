from collections import OrderedDict
from pathlib import Path

import pytest
from strictyaml import YAML, as_document

from src.snowflake.cli.api.project.definition import load_project_definition, merge_left

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
            "a_tree_cut_down_with_a_herring",
        ),
        ([FILE_WITH_SINGLE_ITEM_LIST, FILE_WITH_LONG_LIST], "a_shrubbery.py"),
        (
            [FILE_WITH_SINGLE_ITEM_LIST, ANOTHER_FILE_WITH_SINGLE_ITEM],
            "another_shrubbery_but_a_bit_higher",
        ),
        ([FILE_WITH_LONG_LIST, ANOTHER_FILE_WITH_LONG_LIST], "boing.py"),
    ],
)
def test_load_project_definition(test_files, expected):

    result = load_project_definition(test_files)

    assert expected in result["streamlit"]["additional_source_files"]
