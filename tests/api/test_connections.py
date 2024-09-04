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
from snowflake.cli.api.connections import ConnectionContext


@pytest.mark.parametrize(
    "args, expected",
    [
        (
            {"connection_name": "myconn"},
            "ConnectionContext(_connection_name='myconn', _enable_diag=False, _temporary_connection=False)",
        ),
        (
            {"temporary_connection": True, "account": "myacct", "user": "myuser"},
            "ConnectionContext(_account='myacct', _user='myuser', _enable_diag=False, _temporary_connection=True)",
        ),
    ],
)
def test_connection_context_repr(args: dict, expected: str):
    ctx = ConnectionContext()
    for k, v in args.items():
        getattr(ctx, f"set_{k}")(v)
    assert repr(ctx) == expected
