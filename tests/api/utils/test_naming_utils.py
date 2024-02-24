import pytest
from snowflake.cli.api.utils.naming_utils import from_qualified_name


@pytest.mark.parametrize(
    "qualified_name, expected",
    [
        ("func(number, number)", ("func(number, number)", None, None)),
        ("name", ("name", None, None)),
        ("schema.name", ("name", "schema", None)),
        ("db.schema.name", ("name", "schema", "db")),
    ],
)
def test_from_fully_qualified_name(qualified_name, expected):
    assert from_qualified_name(qualified_name) == expected
