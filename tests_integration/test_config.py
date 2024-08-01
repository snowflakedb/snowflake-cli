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
from snowflake.connector.compat import IS_WINDOWS

from tests_integration.conftest import WORLD_READABLE_CONFIG


@pytest.mark.integration
def test_config_file_permissions_warning(runner, recwarn):
    runner.use_config(WORLD_READABLE_CONFIG)

    result = runner.invoke_with_config(["connection", "list"])
    assert result.exit_code == 0, result.output

    is_warning = any(
        "Bad owner or permissions" in str(warning.message) for warning in recwarn
    )
    if IS_WINDOWS:
        assert not is_warning, "Permissions warning found in warnings list (Windows OS)"
    else:
        assert (
            is_warning
        ), "Permissions warning not found in warnings list (OS other than Windows)"
