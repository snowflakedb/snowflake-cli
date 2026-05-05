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

import zipfile

import pytest
from snowflake.cli._plugins.snowpark.models import (
    Requirement,
    RequirementWithWheel,
    UnsafeWheelEntryError,
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


def _make_wheel(wheel_path, members):
    """Write a .whl file with the given members (name -> bytes).
    Paths are written verbatim with ZIP_STORED so traversal sequences
    like '../../evil' are preserved."""
    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_STORED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)


def test_extract_files_is_a_noop_when_wheel_path_is_none(tmp_path):
    destination = tmp_path / "dest"
    destination.mkdir()

    RequirementWithWheel(
        requirement=Requirement.parse_line("foo"),
        wheel_path=None,
    ).extract_files(destination)

    assert list(destination.iterdir()) == []


def test_extract_files_extracts_safe_wheel(tmp_path):
    wheel_path = tmp_path / "safe-1.0-py3-none-any.whl"
    _make_wheel(
        wheel_path,
        {
            "safe/__init__.py": b"",
            "safe/module.py": b"x = 1\n",
        },
    )
    destination = tmp_path / "dest"
    destination.mkdir()

    RequirementWithWheel(
        requirement=Requirement.parse_line("safe"),
        wheel_path=wheel_path,
    ).extract_files(destination)

    assert (destination / "safe" / "__init__.py").exists()
    assert (destination / "safe" / "module.py").read_text() == "x = 1\n"


@pytest.mark.parametrize(
    "malicious_member",
    [
        "../evil.py",
        "../../evil.py",
        "subdir/../../evil.py",
    ],
)
def test_extract_files_rejects_zip_slip_relative_paths(tmp_path, malicious_member):
    wheel_path = tmp_path / "evil-1.0-py3-none-any.whl"
    _make_wheel(
        wheel_path,
        {
            "evil/__init__.py": b"",
            malicious_member: b"pwned\n",
        },
    )
    destination = tmp_path / "dest"
    destination.mkdir()

    with pytest.raises(UnsafeWheelEntryError) as exc_info:
        RequirementWithWheel(
            requirement=Requirement.parse_line("evil"),
            wheel_path=wheel_path,
        ).extract_files(destination)

    assert malicious_member in str(exc_info.value)
    # Ensure nothing leaked outside the destination.
    assert not (tmp_path / "evil.py").exists()


def test_extract_files_rejects_absolute_path_entry(tmp_path):
    wheel_path = tmp_path / "evil-1.0-py3-none-any.whl"
    outside = tmp_path / "outside.txt"
    _make_wheel(
        wheel_path,
        {
            "evil/__init__.py": b"",
            str(outside): b"pwned\n",
        },
    )
    destination = tmp_path / "dest"
    destination.mkdir()

    with pytest.raises(UnsafeWheelEntryError):
        RequirementWithWheel(
            requirement=Requirement.parse_line("evil"),
            wheel_path=wheel_path,
        ).extract_files(destination)

    assert not outside.exists()


def test_extract_files_aborts_before_writing_any_file(tmp_path):
    """The validation pass must run to completion before any file is written,
    so a wheel that mixes safe and unsafe entries leaves the destination clean."""
    wheel_path = tmp_path / "mixed-1.0-py3-none-any.whl"
    _make_wheel(
        wheel_path,
        {
            "mixed/safe.py": b"ok\n",
            "../evil.py": b"pwned\n",
        },
    )
    destination = tmp_path / "dest"
    destination.mkdir()

    with pytest.raises(UnsafeWheelEntryError):
        RequirementWithWheel(
            requirement=Requirement.parse_line("mixed"),
            wheel_path=wheel_path,
        ).extract_files(destination)

    assert list(destination.iterdir()) == []


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
