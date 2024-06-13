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

from unittest import mock
from unittest.mock import MagicMock

import pytest
from snowflake.cli.api.exceptions import FQNInconsistencyError, FQNNameError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit


def test_attributes():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    assert fqn.database == "database_name"
    assert fqn.schema == "schema_name"
    assert fqn.name == "object_name"


def test_identifier():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    assert fqn.identifier == "database_name.schema_name.object_name"


def test_quoted_identifier():
    fqn = FQN(name='"my object"', database="database_name", schema="schema_name")
    assert fqn.identifier == 'database_name.schema_name."my object"'


@pytest.mark.parametrize(
    "fqn, expected",
    [
        (
            FQN(name="object_name", database="database_name", schema="schema_name"),
            "DATABASE_NAME.SCHEMA_NAME.OBJECT_NAME",
        ),
        (
            FQN(name='"quoted name"', database="database_name", schema="schema_name"),
            "DATABASE_NAME.SCHEMA_NAME.quoted%20name",
        ),
    ],
)
def test_url_identifier(fqn, expected):
    assert fqn.url_identifier == expected


def test_set_database():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    fqn.set_database("foo")
    assert fqn.database == "foo"


def test_set_schema():
    fqn = FQN(name="object_name", database="database_name", schema="schema_name")
    fqn.set_schema("foo")
    assert fqn.schema == "foo"


@pytest.mark.parametrize(
    "fqn_str, identifier",
    [
        ("db.schema.name", "db.schema.name"),
        ("DB.SCHEMA.NAME", "DB.SCHEMA.NAME"),
        ("schema.name", "schema.name"),
        ("name", "name"),
        ('"name with space"', '"name with space"'),
        ('"dot.db"."dot.schema"."dot.name"', '"dot.db"."dot.schema"."dot.name"'),
        ('"dot.db".schema."dot.name"', '"dot.db".schema."dot.name"'),
        ('db.schema."dot.name"', 'db.schema."dot.name"'),
        ('"dot.db".schema."DOT.name"', '"dot.db".schema."DOT.name"'),
        # Nested quotes
        ('"abc""this is in nested quotes"""', '"abc""this is in nested quotes"""'),
        # Callables
        (
            "db.schema.function(string, int, variant)",
            "db.schema.function(string, int, variant)",
        ),
        (
            'db.schema."fun tion"(string, int, variant)',
            'db.schema."fun tion"(string, int, variant)',
        ),
    ],
)
def test_from_string(fqn_str, identifier):
    fqn = FQN.from_string(fqn_str)
    assert fqn.identifier == identifier


@pytest.mark.parametrize(
    "fqn_str",
    [
        "db.schema.name.foo",
        "schema. name",
        "name with space",
        'dot.db."dot.schema"."dot.name"',
        '"dot.db.schema."dot.name"',
    ],
)
def test_from_string_fails_if_pattern_does_not_match(fqn_str):
    with pytest.raises(FQNNameError) as err:
        FQN.from_string(fqn_str)

    assert err.value.message == f"Specified name '{fqn_str}' is not valid name."


@pytest.mark.parametrize(
    "model, expected",
    [
        (
            Streamlit(name="my_dashboard", database="my_db", schema="my_schema"),
            "my_db.my_schema.my_dashboard",
        ),
        (Streamlit(name="my_dashboard", schema="my_schema"), "my_schema.my_dashboard"),
    ],
)
def test_from_identifier_model(model, expected):
    fqn = FQN.from_identifier_model(model)
    assert fqn.identifier == expected


@pytest.mark.parametrize(
    "model",
    [
        Streamlit(name="db.schema.my_dashboard", database="my_db", schema="my_schema"),
        Streamlit(name="schema.my_dashboard", database="my_db", schema="my_schema"),
    ],
)
def test_from_identifier_model_fails_if_name_is_fqn_and_schema_or_db(model):
    with pytest.raises(FQNInconsistencyError) as err:
        FQN.from_identifier_model(model)
    assert (
        f"provided but name '{model.name}' is fully qualified name" in err.value.message
    )


def test_using_connection():
    connection = MagicMock(database="database_test", schema="test_schema")
    fqn = FQN.from_string("name").using_connection(connection)
    assert fqn.identifier == "database_test.test_schema.name"


@mock.patch("snowflake.cli.api.identifiers.cli_context")
def test_using_context(mock_ctx):
    mock_ctx.connection = MagicMock(database="database_test", schema="test_schema")
    fqn = FQN.from_string("name").using_context()
    assert fqn.identifier == "database_test.test_schema.name"
