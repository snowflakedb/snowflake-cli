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

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

from snowflake.cli.api.cli_global_context import get_cli_context

SQL_CLIENT_QUERY_SPAN = "sql.client_query"


@dataclass
class ClientQuerySpanGate:
    record: bool = False


@contextmanager
def sql_client_query_span(
    *, defer_if_recorded: bool = False
) -> Iterator[ClientQuerySpanGate]:
    """Time compile+execute+fetch+render. Caller sets gate.record when cnt > 0."""
    gate = ClientQuerySpanGate()
    with get_cli_context().metrics.span(SQL_CLIENT_QUERY_SPAN) as span:
        error: BaseException | None = None
        try:
            yield gate
        except BaseException as err:
            error = err
            raise
        finally:
            if not gate.record:
                span.discard()
            elif defer_if_recorded and error is None:
                span.defer()
