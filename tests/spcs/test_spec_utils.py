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

from textwrap import dedent
from typing import Any, Dict

import pytest
from snowflake.cli._plugins.spcs.services import spec_utils

SPEC_CONTENT = dedent(
    """
    spec:
        containers:
        - name: cloudbeaver
          image: /spcs_demos_db/cloudbeaver:23.2.1
        endpoints:
        - name: cloudbeaver
          port: 80
          public: true

    """
)

SPEC_DICT = {
    "spec": {
        "containers": [
            {"name": "cloudbeaver", "image": "/spcs_demos_db/cloudbeaver:23.2.1"},
        ],
        "endpoints": [{"name": "cloudbeaver", "port": 80, "public": True}],
    }
}


@pytest.mark.parametrize(
    "base,override,expected",
    [
        # Test basic property overrides
        (
            {
                "key1": "str_value",
                "key2": 1,
                "key3": 1.5,
            },
            {
                "key2": 10,
                "key3": 20,
            },
            {
                "key1": "str_value",
                "key2": 10,
                "key3": 20,
            },
        ),
        # Test nested dictionary merging
        (
            {
                "dict_prop": {
                    "key1": "str_value",
                    "key2": 1,
                    "key3": 1.5,
                    "key4": {
                        "nested": "value",
                    },
                },
            },
            {
                "key1": "distinct from dict_prop subkey",
                "dict_prop": {
                    "key1": "overridden key1",
                    "key4": {
                        "nested": "overridden nested",
                        "nested2": "new value",
                    },
                    "key5": "new key",
                },
            },
            {
                "key1": "distinct from dict_prop subkey",
                "dict_prop": {
                    "key1": "overridden key1",
                    "key2": 1,
                    "key3": 1.5,
                    "key4": {
                        "nested": "overridden nested",
                        "nested2": "new value",
                    },
                    "key5": "new key",
                },
            },
        ),
        # Test list merging
        # - Lists of primitives should be overwritten
        # - Lists of dictionaries should be matched based on a match key, default "name"
        # - Lists of mixed types should be treated as lists of primitives
        # TODO: Are we sure about primitive overwrite behavior? Should we "merge" (i.e. concat) instead?
        (
            {
                "plain_list": [1, 2, 3],
                "dict_list": [
                    {
                        "name": "match_me",
                        "key": "value",
                        "key2": "some other property",
                    },
                ],
                "mixed_list": [1, "str", {"key": "value"}],
            },
            {
                "plain_list": [4, 5, 6],
                "dict_list": [
                    {
                        "name": "new entry",
                        "key": "some property value",
                    },
                    {
                        "name": "match_me",
                        "key": "value override",
                        "key3": "some new property",
                    },
                ],
                "mixed_list": ["new str", {"key": "override"}],
            },
            {
                "plain_list": [4, 5, 6],
                "dict_list": [
                    {
                        "name": "match_me",
                        "key": "value override",
                        "key2": "some other property",
                        "key3": "some new property",
                    },
                    {
                        "name": "new entry",
                        "key": "some property value",
                    },
                ],
                "mixed_list": ["new str", {"key": "override"}],
            },
        ),
        # TODO: Test complex spec merge
        # TODO: Test squash props in override
    ],
)
def test_merge_dicts(
    base: Dict[str, Any], override: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    actual = spec_utils.merge_dicts(base, override)
    assert expected == actual
