# Copyright (c) 2025 Snowflake Inc.
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
from snowflake.cli._plugins.dbt.utils import _extract_dbt_args


class TestDBTUtilsFunction:
    @pytest.mark.parametrize(
        "input_args,expected,sensitive_patterns",
        [
            pytest.param([], [], [], id="empty_args"),
            pytest.param(
                ["-f", "--debug"],
                ["-f", "--debug"],
                [],
                id="safe_boolean_flags",
            ),
            pytest.param(
                ["--select", "sensitive_model"],
                ["--select"],
                ["sensitive_model"],
                id="model_names_masked",
            ),
            pytest.param(
                ["--vars", "'{api_key: secret}'"],
                ["--vars"],
                ["secret", "api_key"],
                id="variables_masked",
            ),
            pytest.param(
                ["--format=JSON"],
                ["--format"],
                ["JSON"],
                id="compound_args_handled",
            ),
            pytest.param(
                ["generate", "--profiles-dir", "/secret/path"],
                ["--profiles-dir", "generate"],
                ["secret", "path"],
                id="subcommand_with_sensitive_path",
            ),
            pytest.param(
                ["--select", "pii.customers", "--vars", "'{password: abc123}'", "-f"],
                ["-f", "--select", "--vars"],
                ["pii", "customers", "abc123", "password"],
                id="complex_sensitive_data_masked",
            ),
        ],
    )
    def test_extract_dbt_args(self, input_args, expected, sensitive_patterns):
        result = _extract_dbt_args(list(input_args))

        assert sorted(result) == sorted(expected)

        result_str = str(result)
        for pattern in sensitive_patterns:
            assert (
                pattern not in result_str
            ), f"Sensitive '{pattern}' leaked in result: {result}"
