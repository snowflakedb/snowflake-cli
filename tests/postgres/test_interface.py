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

from snowflake.cli._plugins.postgres.interface import (
    POSTGRES_SPEC,
    ConnectionTestExitCode,
)
from snowflake.cli.api.plugins.command.interface import ParamKind
from snowflake.cli.api.plugins.command.testing import assert_interface_well_formed

# Exit codes 1-5 are used by snowflake.cli.api.exceptions.
_RESERVED_EXIT_CODES = {1, 2, 3, 4, 5}


def test_interface_is_well_formed():
    assert_interface_well_formed(POSTGRES_SPEC)


def test_connection_test_exit_codes_are_unique_and_unreserved():
    codes = [int(code) for code in ConnectionTestExitCode]
    assert len(codes) == len(set(codes))
    assert not _RESERVED_EXIT_CODES & set(codes)


def test_connection_test_accepts_optional_url_argument_and_part_options():
    command = POSTGRES_SPEC.subgroups[0].commands[0]
    params = {p.name: p for p in command.params}
    assert params["url"].kind is ParamKind.ARGUMENT
    assert params["url"].default is None
    for name in ("host", "port", "database", "username", "password"):
        assert params[name].kind is ParamKind.OPTION
        assert params[name].default is None
