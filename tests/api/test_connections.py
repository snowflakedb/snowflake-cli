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
    "args",
    [
        {},
        {"connection_name": "myconn"},
        {"temporary_connection": True, "account": "myacct", "user": "myuser"},
    ],
)
def test_stable_connection_context_repr(args: dict, snapshot):
    ctx = ConnectionContext()
    ctx.update(**args)
    ctx.validate_and_complete()
    assert repr(ctx) == snapshot
