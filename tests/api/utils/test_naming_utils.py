import pytest
from snowflake.cli.api.identifiers import FQN


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
    name, schema, database = expected
    fqn = FQN.from_string(qualified_name)
    assert fqn.name == name
    assert fqn.schema == schema
    assert fqn.database == database
