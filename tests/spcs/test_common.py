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

from unittest.mock import Mock

import pytest
from click import ClickException
from snowflake.cli.api.exceptions import ObjectAlreadyExistsError, ObjectType
from snowflake.cli.plugins.spcs.common import (
    handle_object_already_exists,
    validate_and_set_instances,
)
from snowflake.connector.errors import ProgrammingError


@pytest.mark.parametrize(
    "min_instances, max_instances, expected_max",
    [
        (2, None, 2),  # max_instances is None, set max_instances to min_instances
        (
            5,
            10,
            10,
        ),  # max_instances is valid non-None value, return max_instances unchanged
    ],
)
def test_validate_and_set_instances(min_instances, max_instances, expected_max):
    assert expected_max == validate_and_set_instances(
        min_instances, max_instances, "name"
    )


@pytest.mark.parametrize(
    "min_instances, max_instances, expected_msg",
    [
        (0, 1, "min_name must be positive"),  # non-positive min_instances
        (-1, 1, "min_name must be positive"),  # negative min_instances
        (
            2,
            1,
            "max_name must be greater or equal to min_name",
        ),  # min_instances > max_instances
    ],
)
def test_validate_and_set_instances_invalid(min_instances, max_instances, expected_msg):
    with pytest.raises(ClickException) as exc:
        validate_and_set_instances(min_instances, max_instances, "name")
    assert expected_msg in exc.value.message


SPCS_OBJECT_EXISTS_ERROR = ProgrammingError(
    msg="Object 'TEST_OBJECT' already exists.", errno=2002
)


def test_handle_object_exists_error():
    mock_type = Mock(spec=ObjectType)
    test_name = "TEST_OBJECT"
    with pytest.raises(ObjectAlreadyExistsError):
        handle_object_already_exists(SPCS_OBJECT_EXISTS_ERROR, mock_type, test_name)


def test_handle_object_exists_error_other_error():
    # For any errors other than 'Object 'XYZ' already exists.', simply pass the error through
    other_error = ProgrammingError(msg="Object does not already exist.", errno=0)
    with pytest.raises(ProgrammingError) as e:
        handle_object_already_exists(other_error, Mock(spec=ObjectType), "TEST_OBJECT")
    assert other_error == e.value
