from pathlib import Path
from typing import Tuple

import pytest
from snowcli.cli.snowpark.common import (
    _convert_resource_details_to_dict,
    _get_snowflake_packages_delta,
    _sql_to_python_return_type_mapper,
)

from tests.testing_utils.fixtures import (
    correct_requirements_snowflake_txt,
    runner,
    temp_dir,
    test_data,
)

INIT_DEFAULT_FILES = {
    Path("example_snowpark/.gitignore"),
    Path("example_snowpark/app"),
    Path("example_snowpark/app/__init__.py"),
    Path("example_snowpark/app/common.py"),
    Path("example_snowpark/app/functions.py"),
    Path("example_snowpark/app/procedures.py"),
    Path("example_snowpark/requirements.txt"),
    Path("example_snowpark/snowflake.yml"),
}


def test_get_snowflake_packages_delta(temp_dir, correct_requirements_snowflake_txt):
    anaconda_package = test_data.requirements[-1]

    result = _get_snowflake_packages_delta(anaconda_package)

    assert result == test_data.requirements[:-1]


def test_convert_resource_details_to_dict():
    resource_details = [
        ("packages", "{'name': 'my-awesome-package','version': '1.2.3'}"),
        ("handler", "handler_function"),
    ]

    assert _convert_resource_details_to_dict(resource_details) == {
        "packages": {"name": "my-awesome-package", "version": "1.2.3"},
        "handler": "handler_function",
    }


@pytest.mark.parametrize(
    "argument",
    [
        ("NUMBER(38,0)", "int"),
        ("TIMESTAMP_NTZ(9)", "datetime"),
        ("TIMESTAMP_TZ(9)", "datetime"),
        ("VARCHAR(16777216)", "string"),
        ("FLOAT", "float"),
        ("ARRAY", "array"),
    ],
)
def test_sql_to_python_return_type_mapper(argument: Tuple[str, str]):
    assert _sql_to_python_return_type_mapper(argument[0]) == argument[1]
