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

from dataclasses import fields

from snowflake.cli.api.cli_global_context import (
    _CONNECTION_CACHE,
    _CliGlobalContextManager,
    fork_cli_context,
    get_cli_context,
    get_cli_context_manager,
)
from snowflake.cli.api.metrics import CLIMetricsSpan
from snowflake.cli.api.output.formats import OutputFormat


def test_reset_global_context_mgr():
    """
    Ensures that the reset() method is working properly.
    """
    mgr = _CliGlobalContextManager()
    mgr.experimental = True
    mgr.silent = True
    mgr.output_format = OutputFormat.JSON
    mgr.connection_context.database = "blahblah"
    mgr.connection_context.password = "****"
    mgr.override_project_definition = "project definition"
    assert mgr.connection_cache == _CONNECTION_CACHE
    mgr.connection_cache = None
    mgr.reset()
    assert mgr.connection_cache == _CONNECTION_CACHE
    assert mgr.override_project_definition is None

    initial_mgr = _CliGlobalContextManager()
    for f in fields(mgr):
        assert getattr(mgr, f.name) == getattr(
            initial_mgr, f.name
        ), f"{f.name} was not reset properly"


# FIXME: needs asyncio test
def test_forked_context():
    assert get_cli_context().connection_context.connection_name is None

    with fork_cli_context(
        connection_overrides={"connection_name": "outer"},
        project_env_overrides={"abc": "123", "initial": "value"},
    ) as outer:
        assert outer._manager == get_cli_context_manager()  # noqa: SLF001
        assert get_cli_context().connection_context.connection_name == "outer"
        assert get_cli_context_manager().project_env_overrides_args == {
            "abc": "123",
            "initial": "value",
        }

        with fork_cli_context(
            connection_overrides={"connection_name": "inner"},
            project_env_overrides={"abc": "456", "another": "one"},
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


def test_forked_metrics_spans():
    outer_metrics = get_cli_context_manager().metrics

    with outer_metrics.span("outer_span"):
        with fork_cli_context() as inner:
            inner_metrics = inner.metrics
            with inner_metrics.span("inner_span"):
                pass

    assert outer_metrics != inner_metrics
    assert outer_metrics.completed_spans != inner_metrics.completed_spans

    assert len(outer_metrics.completed_spans) == len(inner_metrics.completed_spans) == 1

    assert outer_metrics.completed_spans[0][CLIMetricsSpan.NAME_KEY] == "outer_span"

    assert inner_metrics.completed_spans[0][CLIMetricsSpan.NAME_KEY] == "inner_span"
