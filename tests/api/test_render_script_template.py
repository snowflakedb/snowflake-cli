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

from pathlib import Path

import pytest
from click import ClickException
from snowflake.cli.api.entities.utils import (
    render_script_template,
    render_script_templates,
)


@pytest.fixture
def project_with_scripts(tmp_path):
    """A project tree with an inner SQL script and a sibling secret file."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "scripts").mkdir()
    (project_root / "scripts" / "post.sql").write_text("SELECT 1;")
    (project_root / "scripts" / "nested").mkdir()
    (project_root / "scripts" / "nested" / "deep.sql").write_text("SELECT 2;")

    secret = tmp_path / "secret.sql"
    secret.write_text("LEAKED")

    return project_root, secret


def test_renders_relative_script_inside_project(project_with_scripts):
    project_root, _ = project_with_scripts
    assert render_script_template(project_root, {}, "scripts/post.sql") == "SELECT 1;"


def test_renders_nested_relative_script(project_with_scripts):
    project_root, _ = project_with_scripts
    assert (
        render_script_template(project_root, {}, "scripts/nested/deep.sql")
        == "SELECT 2;"
    )


def test_rejects_absolute_path_outside_project(project_with_scripts):
    project_root, secret = project_with_scripts
    with pytest.raises(ClickException) as err:
        render_script_template(project_root, {}, str(secret))
    assert "outside the project root" in err.value.message
    assert "absolute path" in err.value.message


def test_rejects_dotdot_escape_from_project(project_with_scripts):
    project_root, _ = project_with_scripts
    with pytest.raises(ClickException) as err:
        render_script_template(project_root, {}, "../secret.sql")
    assert "outside the project root" in err.value.message


def test_rejects_nested_dotdot_escape_from_project(project_with_scripts):
    project_root, _ = project_with_scripts
    with pytest.raises(ClickException) as err:
        render_script_template(project_root, {}, "scripts/../../secret.sql")
    assert "outside the project root" in err.value.message


def test_allow_absolute_paths_bypass_for_trusted_callers(project_with_scripts):
    project_root, secret = project_with_scripts
    # Trusted internal callers (e.g. in-memory v1->v2 project definition
    # conversion that writes tempfiles) can opt in to absolute paths.
    assert (
        render_script_template(project_root, {}, str(secret), allow_absolute_paths=True)
        == "LEAKED"
    )


def test_render_script_templates_applies_containment_to_each(project_with_scripts):
    project_root, _ = project_with_scripts
    with pytest.raises(ClickException):
        render_script_templates(project_root, {}, ["scripts/post.sql", "../secret.sql"])


def test_render_script_templates_allow_absolute_paths_flag(project_with_scripts):
    project_root, secret = project_with_scripts
    results = render_script_templates(
        project_root,
        {},
        ["scripts/post.sql", str(secret)],
        allow_absolute_paths=True,
    )
    assert results == ["SELECT 1;", "LEAKED"]


def test_rejects_symlink_inside_project_pointing_outside(tmp_path):
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "scripts").mkdir()
    secret = tmp_path / "secret.sql"
    secret.write_text("LEAKED")
    (project_root / "scripts" / "evil.sql").symlink_to(secret)

    with pytest.raises(ClickException) as err:
        render_script_template(project_root, {}, "scripts/evil.sql")
    assert "outside the project root" in err.value.message


def test_project_root_symlink_resolution(tmp_path):
    real_root = tmp_path / "real_project"
    real_root.mkdir()
    (real_root / "scripts").mkdir()
    (real_root / "scripts" / "post.sql").write_text("SELECT 42;")

    link_root = tmp_path / "link_project"
    link_root.symlink_to(real_root, target_is_directory=True)

    assert (
        render_script_template(Path(link_root), {}, "scripts/post.sql") == "SELECT 42;"
    )
