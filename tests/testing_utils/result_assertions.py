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
import re

from click.testing import Result


def assert_successful_result_message(result: Result, expected_msg: str):
    assert result.exit_code == 0, result.output
    assert result.output == expected_msg + "\n"


def assert_that_result_is_usage_error(
    result: Result, expected_error_message: str
) -> None:
    result_output = re.sub(r"\s*\|\|\s*", " ", result.output.replace("\n", ""))
    assert result.exit_code == 2, result.output
    assert expected_error_message in result_output, result.output
    assert isinstance(result.exception, SystemExit)
    assert "traceback" not in result.output.lower()
