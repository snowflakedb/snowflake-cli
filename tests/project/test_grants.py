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

import pytest
from pydantic import ValidationError
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)

GRANT = "GRANT USAGE ON STREAMLIT IDENTIFIER('my_app')"


def _entity(*grants: dict) -> StreamlitEntityModel:
    return StreamlitEntityModel(
        type="streamlit", identifier="my_app", grants=list(grants)
    )


@pytest.mark.parametrize(
    "role, expected",
    [
        # Case is passed through, never folded. Snowflake upper-cases an unquoted
        # name itself, so ANALYST and analyst are one role to it either way, and
        # rewriting the case here would only make the emitted statement harder to
        # read back against the project file.
        ("ACCOUNTADMIN", "ROLE ACCOUNTADMIN"),
        ("accountadmin", "ROLE accountadmin"),
        ("MiXeDcAsE", "ROLE MiXeDcAsE"),
        # Everything SQL accepts bare stays bare, so a project file that worked
        # before keeps emitting exactly the statement it emitted before.
        ("my_role_1", "ROLE my_role_1"),
        ("_leading_underscore", "ROLE _leading_underscore"),
        ("MY$ROLE", "ROLE MY$ROLE"),
        # Already quoted: passed through as written, not quoted a second time. A
        # quoted name is case-sensitive, so "MixedCase" has to survive intact.
        ('"my role"', 'ROLE "my role"'),
        ('"MixedCase"', 'ROLE "MixedCase"'),
        ('"quote""inside"', 'ROLE "quote""inside"'),
        # Not a bare identifier. Each of these used to reach Snowflake unquoted,
        # as a syntax error; they are quoted now.
        ("role-with-dash", 'ROLE "role-with-dash"'),
        ("role with space", 'ROLE "role with space"'),
        ("1role", 'ROLE "1role"'),
        # A dotted name is not a database role — that needs TO DATABASE ROLE,
        # which this schema cannot express — so it is one quoted account role.
        ("mydb.myrole", 'ROLE "mydb.myrole"'),
    ],
)
def test_role_is_quoted_only_where_sql_needs_it(role, expected):
    assert _entity({"privilege": "USAGE", "role": role}).get_grant_sqls() == [
        f"{GRANT} TO {expected}"
    ]


@pytest.mark.parametrize(
    "user, expected",
    [
        ("AAMADHAVAN", "USER AAMADHAVAN"),
        ("aamadhavan", "USER aamadhavan"),
        # The case that matters for UBAC: users are routinely named after an
        # email address, which is not a bare identifier.
        ("first.last@example.com", 'USER "first.last@example.com"'),
        ('"first.last@example.com"', 'USER "first.last@example.com"'),
        ("user-with-dash", 'USER "user-with-dash"'),
        # A quoted lower-case name is a different user from the unquoted one.
        ('"aamadhavan"', 'USER "aamadhavan"'),
    ],
)
def test_user_is_quoted_only_where_sql_needs_it(user, expected):
    assert _entity({"privilege": "USAGE", "user": user}).get_grant_sqls() == [
        f"{GRANT} TO {expected}"
    ]


@pytest.mark.parametrize(
    "privilege", ["USAGE", "usage", "OWNERSHIP", "IMPORTED PRIVILEGES"]
)
def test_privilege_is_emitted_as_written(privilege):
    """Unchanged behaviour: the privilege reaches the statement verbatim."""
    assert _entity({"privilege": privilege, "role": "ANALYST"}).get_grant_sqls() == [
        f"GRANT {privilege} ON STREAMLIT IDENTIFIER('my_app') TO ROLE ANALYST"
    ]


@pytest.mark.parametrize(
    "grant",
    [
        # Neither grantee.
        {"privilege": "USAGE"},
        {"privilege": "USAGE", "role": None},
        {"privilege": "USAGE", "role": "", "user": ""},
        # Both.
        {"privilege": "USAGE", "role": "ACCOUNTADMIN", "user": "AAMADHAVAN"},
        # Present but empty, which used to emit `TO ROLE ` and fail in SQL.
        {"privilege": "USAGE", "role": ""},
        {"privilege": "USAGE", "user": ""},
        # Whitespace-only is absent too, and fails the same way as empty.
        {"privilege": "USAGE", "role": " "},
        {"privilege": "USAGE", "user": " "},
        {"privilege": "USAGE", "role": "\t"},
        {"privilege": "USAGE", "role": " ", "user": " "},
    ],
)
def test_grant_requires_exactly_one_grantee(grant):
    with pytest.raises(ValidationError, match="exactly one of role or user"):
        _entity(grant)


@pytest.mark.parametrize(
    "grant, expected_grantee",
    [
        # One name given, the other written out as blank: still one grant.
        ({"privilege": "USAGE", "role": "ANALYST", "user": ""}, "ROLE ANALYST"),
        ({"privilege": "USAGE", "role": "", "user": "ALICE"}, "USER ALICE"),
        ({"privilege": "USAGE", "role": "ANALYST", "user": " "}, "ROLE ANALYST"),
        # Surrounding whitespace is trimmed rather than quoted into the name.
        ({"privilege": "USAGE", "role": "  ANALYST  "}, "ROLE ANALYST"),
    ],
)
def test_a_blank_second_grantee_is_absent(grant, expected_grantee):
    assert _entity(grant).get_grant_sqls() == [f"{GRANT} TO {expected_grantee}"]


@pytest.mark.parametrize(
    "privilege",
    [
        # `execute_query` runs through `execute_stream`, so a `;` would be a
        # second statement rather than a bad privilege name.
        "USAGE; DROP STAGE foo",
        'USAGE"x',
        "USAGE'x",
        "USAGE\nDROP STAGE foo",
        "USAGE -- comment",
        "USAGE()",
        "",
        " ",
        "IMPORTED  PRIVILEGES",
    ],
)
def test_privilege_must_be_privilege_words(privilege):
    with pytest.raises(ValidationError, match="Invalid privilege"):
        _entity({"privilege": privilege, "role": "ANALYST"})


@pytest.mark.parametrize("grants", [None, []])
def test_no_grants_emits_no_statements(grants):
    entity = StreamlitEntityModel(type="streamlit", identifier="my_app", grants=grants)

    assert entity.get_grant_sqls() == []


def test_grants_are_emitted_in_order():
    assert _entity(
        {"privilege": "USAGE", "role": "ANALYST"},
        {"privilege": "USAGE", "user": "AAMADHAVAN"},
    ).get_grant_sqls() == [
        f"{GRANT} TO ROLE ANALYST",
        f"{GRANT} TO USER AAMADHAVAN",
    ]


def test_grant_can_carry_the_grant_option():
    """So a share recorded in `grants:` keeps view-and-share on the next deploy."""
    assert _entity(
        {"privilege": "USAGE", "user": "AAMADHAVAN", "with_grant_option": True}
    ).get_grant_sqls() == [
        "GRANT USAGE ON STREAMLIT IDENTIFIER('my_app') TO USER AAMADHAVAN"
        " WITH GRANT OPTION"
    ]
