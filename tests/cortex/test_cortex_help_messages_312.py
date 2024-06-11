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

import sys

import pytest

if sys.version_info < (3, 12):
    pytest.skip(
        """As Snowflake Python API does not support Python version 3.12 and greater, 
    Cortex Search command should be hidden, resulting in different help messages""",
        allow_module_level=True,
    )


def test_cortex_help_messages_for_311_and_less(runner, snapshot):
    result = runner.invoke(["cortex", "--help"])
    assert result.exit_code == 0
    assert result.output == snapshot


def test_cortex_help_messages_for_311_and_less_no_help_flag(runner, snapshot):
    result = runner.invoke(["cortex"])
    assert result.exit_code == 0
    assert result.output == snapshot
