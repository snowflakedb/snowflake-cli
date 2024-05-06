import pytest
from snowflake.cli.api.fqn import FQN


def test_database():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    assert fqn.database == "database_name"


def test_schema():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    assert fqn.schema == "schema_name"


def test_name():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    assert fqn.name == "object_name"


def test_identifier():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    assert fqn.identifier == "DATABASE_NAME.SCHEMA_NAME.OBJECT_NAME"


def test_url_identifier():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    assert fqn.url_identifier == "DATABASE_NAME.SCHEMA_NAME.OBJECT_NAME"


@pytest.mark.parametrize(
    "fqn_str, database, schema, name, identifier",
    [
        ("db.schema.name", "db", "schema", "name"),
        ("schema.name", None, "schema", "name"),
        ("name", None, None, "name"),
        ('"quotedNAME".schema.name', '"quotedNAME"', "schema", "name"),
    ],
)
def test_from_string(fqn_str, database, schema, name, identifier):
    fqn = FQN.from_string(fqn_str)
    assert fqn.database == database
    assert fqn.schema == schema
    assert fqn.name == name


def test_from_identifier_model():
    assert False


def test_set_database():
    assert False


def test_set_schema():
    assert False


def test_set_name():
    assert False


def test_using_connection():
    assert False


def test_using_context():
    assert False
