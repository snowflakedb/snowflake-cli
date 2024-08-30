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

from snowflake.cli.api.cli_global_context import (
    fork_cli_context,
    get_cli_context,
    get_cli_context_manager,
)


def test_forked_context():
    assert get_cli_context().connection_context.connection_name is None

    with fork_cli_context(
        connection_overrides={"connection_name": "outer"},
        env={"abc": "123", "initial": "value"},
    ) as outer:
        assert outer._manager == get_cli_context_manager()  # noqa: SLF001
        assert get_cli_context().connection_context.connection_name == "outer"
        assert get_cli_context_manager().project_env_overrides_args == {
            "abc": "123",
            "initial": "value",
        }

        with fork_cli_context(
            connection_overrides={"connection_name": "inner"},
            env={"abc": "456", "another": "one"},
        ) as inner:
            assert inner._manager == get_cli_context_manager()  # noqa: SLF001
            assert get_cli_context().connection_context.connection_name == "inner"
            assert get_cli_context_manager().project_env_overrides_args == {
                "abc": "456",
                "initial": "value",
                "another": "one",
            }

        assert get_cli_context().connection_context.connection_name == "outer"
        assert get_cli_context_manager().project_env_overrides_args == {
            "abc": "123",
            "initial": "value",
        }

    assert get_cli_context().connection_context.connection_name is None
