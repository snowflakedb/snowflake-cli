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
from snowflake.cli._plugins.snowpark.models import (
    Requirement,
    WheelMetadata,
    get_package_name,
)


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


def test_wheel_metadata_parsing(test_root_path):
    from snowflake.cli._plugins.snowpark.zipper import zip_dir
    from snowflake.cli.api.secure_path import SecurePath

    with SecurePath.temporary_directory() as tmpdir:
        wheel_path = tmpdir / "Zendesk-1.1.1-py3-none-any.whl"

        # prepare .whl package
        package_dir = tmpdir / "ZendeskWhl"
        package_dir.mkdir()
        package_src = (
            SecurePath(test_root_path) / "test_data" / "local_packages" / ".packages"
        )
        for srcdir in ["zendesk", "Zendesk-1.1.1.dist-info"]:
            (package_src / srcdir).copy(package_dir.path)
        zip_dir(source=package_dir.path, dest_zip=wheel_path.path)

        # check metadata
        meta = WheelMetadata.from_wheel(wheel_path.path)
        assert meta.name == "zendesk"
        assert meta.wheel_path == wheel_path.path
        assert meta.dependencies == ["httplib2", "simplejson"]


def test_raise_error_when_artifact_contains_asterix(
    runner, project_directory, alter_snowflake_yml, os_agnostic_snapshot
):
    with project_directory("glob_patterns") as tmp_dir:
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml", "entities.hello_procedure.artifacts", ["src/*"]
        )

        result = runner.invoke(["snowpark", "build"])

        assert result.exit_code == 1
        assert result.output == os_agnostic_snapshot


def test_error_is_raised_when_packages_are_specified_with_no_repository(
    runner, project_directory, alter_snowflake_yml, os_agnostic_snapshot
):
    with project_directory("snowpark_functions_v2") as tmp_dir:
        alter_snowflake_yml(
            tmp_dir / "snowflake.yml",
            "entities.func1.artifact_repository_packages",
            ["package"],
        )

        result = runner.invoke(["snowpark", "build"])

        assert result.exit_code == 1
        assert result.output == os_agnostic_snapshot
