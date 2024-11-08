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

import codecs
import os
import re
from typing import List, Optional
from urllib.parse import quote

IDENTIFIER = r'((?:"[^"]*(?:""[^"]*)*")|(?:[A-Za-z_][\w$]{0,254}))'
IDENTIFIER_NO_LENGTH = r'((?:"[^"]*(?:""[^"]*)*")|(?:[A-Za-z_][\w$]*))'
DB_SCHEMA_AND_NAME = f"{IDENTIFIER}[.]{IDENTIFIER}[.]{IDENTIFIER}"
SCHEMA_AND_NAME = f"{IDENTIFIER}[.]{IDENTIFIER}"
GLOB_REGEX = r"^[a-zA-Z0-9_\-./*?**\p{L}\p{N}]+$"
RELATIVE_PATH = r"^[^/][\p{L}\p{N}_\-.][^/]*$"

SINGLE_QUOTED_STRING_LITERAL_REGEX = r"'((?:\\.|''|[^'\n])+?)'"

# See https://docs.snowflake.com/en/sql-reference/identifiers-syntax for identifier syntax
UNQUOTED_IDENTIFIER_REGEX = r"([a-zA-Z_])([a-zA-Z0-9_$]{0,254})"
QUOTED_IDENTIFIER_REGEX = r'"((""|[^"]){0,255})"'
VALID_IDENTIFIER_REGEX = f"(?:{UNQUOTED_IDENTIFIER_REGEX}|{QUOTED_IDENTIFIER_REGEX})"

# An env var that is used to suffix the names of some account-level resources
TEST_RESOURCE_SUFFIX_VAR = "SNOWFLAKE_CLI_TEST_RESOURCE_SUFFIX"


def encode_uri_component(s: str) -> str:
    """
    Implementation of JavaScript's encodeURIComponent.
    """
    return quote(s, safe="!~*'()")


def sanitize_identifier(input_: str):
    """
    Removes characters that cannot be used in an unquoted identifier.
    If the identifier does not start with a letter or underscore, prefix it with an underscore.
    Limits the identifier to 255 characters.
    """
    value = re.sub(r"[^a-zA-Z0-9_$]", "", f"{input_}")

    # if it does not start with a letter or underscore, prefix it with an underscore
    if not value or not re.match(r"[a-zA-Z_]", value[0]):
        value = f"_{value}"

    # limit it to 255 characters
    return value[:255]


def is_valid_unquoted_identifier(identifier: str) -> bool:
    """
    Determines whether the provided identifier is a valid Snowflake unquoted identifier.
    """
    return re.fullmatch(UNQUOTED_IDENTIFIER_REGEX, identifier) is not None


def is_valid_quoted_identifier(identifier: str) -> bool:
    """
    Determines whether the provided identifier is a valid Snowflake quoted identifier.
    """
    return re.fullmatch(QUOTED_IDENTIFIER_REGEX, identifier) is not None


def is_valid_identifier(identifier: str) -> bool:
    """
    Determines whether the provided identifier is a valid Snowflake quoted or unquoted identifier.
    """
    return is_valid_unquoted_identifier(identifier) or is_valid_quoted_identifier(
        identifier
    )


def is_valid_object_name(name: str, max_depth=2, allow_quoted=True) -> bool:
    """
    Determines whether the given identifier is a valid object name in the form <name>, <schema>.<name>, or <database>.<schema>.<name>.
    Max_depth determines how many valid identifiers are allowed. For example, account level objects would have a max depth of 0
    because they cannot be qualified by a database or schema, just the single identifier.
    """
    if max_depth < 0:
        raise ValueError("max_depth must be non-negative")
    identifier_pattern = (
        VALID_IDENTIFIER_REGEX if allow_quoted else UNQUOTED_IDENTIFIER_REGEX
    )
    pattern = rf"{identifier_pattern}(?:\.{identifier_pattern}){{0,{max_depth}}}"
    return re.fullmatch(pattern, name) is not None


def to_quoted_identifier(input_value: str) -> str:
    """
    Turn the input into a valid quoted identifier.
    If it is already a valid quoted identifier,
    return it as is.
    """
    if is_valid_quoted_identifier(input_value):
        return input_value

    return '"' + input_value.replace('"', '""') + '"'


def to_identifier(name: str) -> str:
    """
    Converts a name to a valid Snowflake identifier. If the name is already a valid
    Snowflake identifier, then it is returned unmodified.
    """
    if is_valid_identifier(name):
        return name

    return to_quoted_identifier(name)


def identifier_to_str(identifier: str) -> str:
    if is_valid_quoted_identifier(identifier):
        unquoted_id = identifier[1:-1]
        return unquoted_id.replace('""', '"')

    return identifier


def append_to_identifier(identifier: str, suffix: str) -> str:
    """
    Appends a suffix to a valid identifier.
    """
    if is_valid_unquoted_identifier(identifier):
        return to_identifier(f"{identifier}{suffix}")
    else:
        # the identifier is quoted, so insert the suffix within the quotes
        unquoted = identifier[1:-1]
        return f'"{unquoted}{suffix}"'


def unquote_identifier(identifier: str) -> str:
    """
    Returns a version of this identifier that can be used inside of a
    string for a LIKE clause, or to match an identifier passed back as
    a value from a SQL statement.
    """
    if match := re.fullmatch(QUOTED_IDENTIFIER_REGEX, identifier):
        return match.group(1).replace('""', '"')
    # unquoted identifiers are internally represented as uppercase
    return identifier.upper()


def identifier_for_url(identifier: str) -> str:
    """
    Returns a version of this identifier that can be used as part of a URL.
    """
    return encode_uri_component(unquote_identifier(identifier))


def is_valid_string_literal(literal: str) -> bool:
    """
    Determines if a literal is a valid single quoted string literal
    """
    return re.fullmatch(SINGLE_QUOTED_STRING_LITERAL_REGEX, literal) is not None


def to_string_literal(raw_value: str) -> str:
    """
    Converts the raw string value to a correctly escaped, single-quoted string literal.
    """
    # encode escape sequences
    escaped = str(codecs.encode(raw_value, "unicode-escape"), "utf-8")

    # escape single quotes
    escaped = re.sub(r"^'|(?<!')'", r"\'", escaped)

    return f"'{escaped}'"


def extract_schema(qualified_name: str):
    """
    Extracts the schema from either a two-part or three-part qualified name
    (i.e. schema.object or database.schema.object). If qualified_name is not
    qualified with a schema, returns None.
    """
    if match := re.fullmatch(DB_SCHEMA_AND_NAME, qualified_name):
        return match.group(2)
    elif match := re.fullmatch(SCHEMA_AND_NAME, qualified_name):
        return match.group(1)
    return None


def first_set_env(*keys: str):
    for k in keys:
        v = os.getenv(k)
        if v:
            return v

    return None


def get_env_username() -> Optional[str]:
    return first_set_env("USER", "USERNAME", "LOGNAME")


def concat_identifiers(identifiers: list[str]) -> str:
    """
    Concatenate multiple identifiers.
    If any of them is quoted identifier or contains unsafe characters, quote the result.
    Otherwise, the resulting identifier will be unquoted.
    """
    quotes_found = False
    stringified_identifiers: List[str] = []

    for identifier in identifiers:
        if is_valid_quoted_identifier(identifier):
            quotes_found = True
        stringified_identifiers.append(identifier_to_str(identifier))

    concatenated_ids_str = "".join(stringified_identifiers)
    if quotes_found:
        return to_quoted_identifier(concatenated_ids_str)

    return to_identifier(concatenated_ids_str)


SUPPORTED_VERSIONS = [1]


def validate_version(version: str):
    if version in SUPPORTED_VERSIONS:
        raise ValueError(
            f"Project definition version {version} is not supported by this version of Snowflake CLI. Supported versions: {SUPPORTED_VERSIONS}"
        )


def escape_like_pattern(pattern: str, escape_sequence: str = r"\\") -> str:
    """
    When used with LIKE in Snowflake, '%' and '_' are wildcard characters and must be escaped to be used literally.
    The escape character is '\\' when used in SHOW LIKE and must be specified when used with string matching using the
    following syntax: <subject> LIKE <pattern> [ ESCAPE <escape> ].
    """
    pattern = pattern.replace("%", rf"{escape_sequence}%").replace(
        "_", rf"{escape_sequence}_"
    )
    return pattern


def identifier_to_show_like_pattern(identifier: str) -> str:
    """
    Takes an identifier and converts it into a pattern to be used with SHOW ... LIKE ... to get all rows with name
    matching this identifier
    """
    return f"'{escape_like_pattern(unquote_identifier(identifier))}'"


def append_test_resource_suffix(identifier: str) -> str:
    """
    Append a suffix that should be added to specified account-level resources.

    This is an internal concern that is currently only used in tests
    to isolate concurrent runs and to add the test name to resources.
    """
    suffix = os.environ.get(TEST_RESOURCE_SUFFIX_VAR, "")
    if identifier_to_str(identifier).endswith(identifier_to_str(suffix)):
        # If the suffix has already been added, don't add it again
        return identifier
    if is_valid_quoted_identifier(identifier) or is_valid_quoted_identifier(suffix):
        # If either identifier is already quoted, use concat_identifier
        # to add the suffix inside the quotes
        return concat_identifiers([identifier, suffix])
    # Otherwise just append the string, don't add quotes
    # in case the user doesn't want them
    return f"{identifier}{suffix}"
