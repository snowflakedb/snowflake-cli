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


@pytest.mark.integration
def test_use_session_token(runner, snowflake_session):
    session_token = snowflake_session.rest.token
    master_token = snowflake_session.rest.master_token

    result_of_setting_variable = runner.invoke(
        [
            "sql",
            "-q",
            "set a = 42",
            "-x",
            "--account",
            snowflake_session.account,
            "--session-token",
            session_token,
            "--master-token",
            master_token,
        ]
    )
    assert result_of_setting_variable.exit_code == 0
    result_of_getting_variable = runner.invoke_json(
        [
            "sql",
            "-q",
            "select $a as dummy",
            "-x",
            "--account",
            snowflake_session.account,
            "--session-token",
            session_token,
            "--master-token",
            master_token,
            "--format",
            "json",
        ]
    )
    assert result_of_getting_variable.exit_code == 0
    assert result_of_getting_variable.json == [{"DUMMY": 42}]
