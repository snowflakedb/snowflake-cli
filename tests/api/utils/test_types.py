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
from snowflake.cli.api.utils.types import try_cast_to_bool, try_cast_to_int


class TestTryCastToBool:
    def test_bool_true_returns_true(self):
        assert try_cast_to_bool(True) is True

    def test_bool_false_returns_false(self):
        assert try_cast_to_bool(False) is False

    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "tRuE"])
    def test_string_true_variations(self, value):
        assert try_cast_to_bool(value) is True

    @pytest.mark.parametrize("value", ["false", "False", "FALSE", "fAlSe"])
    def test_string_false_variations(self, value):
        assert try_cast_to_bool(value) is False

    def test_string_1_returns_true(self):
        assert try_cast_to_bool("1") is True

    def test_string_0_returns_false(self):
        assert try_cast_to_bool("0") is False

    def test_int_1_returns_true(self):
        assert try_cast_to_bool(1) is True

    def test_int_0_returns_false(self):
        assert try_cast_to_bool(0) is False

    @pytest.mark.parametrize("value", ["yes", "no", "on", "off", "2", "-1", ""])
    def test_invalid_string_raises_value_error(self, value):
        with pytest.raises(ValueError, match="Could not cast .* to bool value"):
            try_cast_to_bool(value)

    def test_invalid_type_raises_value_error(self):
        with pytest.raises(ValueError, match="Could not cast .* to bool value"):
            try_cast_to_bool([1, 2, 3])


class TestTryCastToInt:
    def test_int_returns_same_value(self):
        assert try_cast_to_int(42) == 42

    def test_negative_int_returns_same_value(self):
        assert try_cast_to_int(-5) == -5

    def test_zero_returns_zero(self):
        assert try_cast_to_int(0) == 0

    def test_string_number_returns_int(self):
        assert try_cast_to_int("42") == 42

    def test_string_negative_number_returns_int(self):
        assert try_cast_to_int("-5") == -5

    def test_string_zero_returns_zero(self):
        assert try_cast_to_int("0") == 0

    def test_string_with_whitespace_returns_int(self):
        assert try_cast_to_int("  42  ") == 42

    def test_bool_true_converts_to_1(self):
        result = try_cast_to_int(True)
        assert result == 1
        assert type(result) is int

    def test_bool_false_converts_to_0(self):
        result = try_cast_to_int(False)
        assert result == 0
        assert type(result) is int

    def test_float_converts_to_int(self):
        assert try_cast_to_int(3.7) == 3

    def test_string_float_raises_value_error(self):
        with pytest.raises(ValueError, match="Could not cast '3.14' to int value"):
            try_cast_to_int("3.14")

    def test_empty_string_raises_value_error(self):
        with pytest.raises(
            ValueError, match="Could not cast empty string to int value"
        ):
            try_cast_to_int("")

    def test_whitespace_only_string_raises_value_error(self):
        with pytest.raises(
            ValueError, match="Could not cast empty string to int value"
        ):
            try_cast_to_int("   ")

    def test_invalid_string_raises_value_error(self):
        with pytest.raises(ValueError, match="Could not cast 'abc' to int value"):
            try_cast_to_int("abc")

    def test_none_raises_value_error(self):
        with pytest.raises(ValueError, match="Could not cast 'None' to int value"):
            try_cast_to_int(None)

    def test_list_raises_value_error(self):
        with pytest.raises(
            ValueError, match=r"Could not cast '\[1, 2, 3\]' to int value"
        ):
            try_cast_to_int([1, 2, 3])
