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

import pytest
from snowflake.cli.api.identifiers import FQN


@pytest.mark.parametrize(
    "qualified_name, expected",
    [
        ("func(number, number)", ("func", None, None)),
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
    if fqn.signature:
        assert fqn.signature == "(number, number)"
