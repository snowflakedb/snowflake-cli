from typing import Tuple

import pytest
from snowflake.cli.plugins.snowpark.common import (
    _convert_resource_details_to_dict,
    _snowflake_dependencies_differ,
    _sql_to_python_return_type_mapper,
)


def test_get_snowflake_packages_delta():
    for uploaded_packages, new_packages, expected in [
        ([], [], False),
        (
            ["package", "package_with_requirements>=2,<4"],
            ["package-with-requirements <4,>=2", "PACKAGE"],
            False,
        ),
        (
            ["different-requirements<1.1,>0.9"],
            ["different-requirements<1.0,>0.9"],
            True,
        ),
        (["different-package"], ["another-package"], True),
        (["package"], ["package", "added-package"], True),
        (["package", "removed-package"], ["package"], True),
    ]:
        assert expected == _snowflake_dependencies_differ(
            uploaded_packages, new_packages
        )


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
