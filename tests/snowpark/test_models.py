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
from snowflake.cli.plugins.snowpark.models import Requirement, get_package_name


@pytest.mark.parametrize(
    "line,name,extras",
    [
        ("ipython ; extra == 'docs'", "ipython", ["docs"]),
        ("foo", "foo", []),
        ("pytest ; extra == 'tests'", "pytest", ["tests"]),
    ],
)
def test_requirement_is_parsed_correctly(line, name, extras):
    result = Requirement.parse_line(line)

    assert result.name == name
    assert result.extras == extras


@pytest.mark.parametrize(
    "line,name",
    [
        (
            "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests",
            "dummy-pkg-for-tests",
        ),
        (
            "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests@foo",
            "dummy-pkg-for-tests",
        ),
        (
            "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests@0123456789abcdef0123456789abcdef01234567",
            "dummy-pkg-for-tests",
        ),
        ("foo.zip", "foo"),
        ("package", "package"),
        ("package.zip", "package"),
        ("git+https://github.com/snowflakedb/snowflake-cli/", "snowflake-cli"),
        (
            "git+https://github.com/snowflakedb/snowflake-cli.git/@snow-123456-fix",
            "snowflake-cli",
        ),
    ],
)
def test_get_package_name(line, name):
    assert get_package_name(line) == name
