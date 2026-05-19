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
from click import ClickException
from snowflake.cli.api.entities.utils import (
    render_script_template,
    render_script_templates,
)
from snowflake.cli.api.project.schemas.entities.common import PostDeployHook
from snowflake.cli.api.project.schemas.updatable_model import context


@pytest.fixture
def project_with_scripts(tmp_path):
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


def test_rejects_dotdot_escape_from_project(project_with_scripts):
    project_root, _ = project_with_scripts

    with pytest.raises(ClickException) as err:
        render_script_template(project_root, {}, "../secret.sql")

    assert "outside the project root" in err.value.message


def test_rejects_rooted_windows_path(project_with_scripts):
    project_root, _ = project_with_scripts

    with pytest.raises(ClickException) as err:
        render_script_template(project_root, {}, r"\secret.sql")

    assert "outside the project root" in err.value.message


def test_allow_generated_tempfile_bypass_for_trusted_callers(project_with_scripts):
    project_root, secret = project_with_scripts

    assert (
        render_script_template(
            project_root,
            {},
            str(secret),
            allow_generated_sql_script_path=True,
        )
        == "LEAKED"
    )


def test_render_script_templates_applies_containment_to_each(project_with_scripts):
    project_root, _ = project_with_scripts
    hooks = [
        PostDeployHook(sql_script="scripts/post.sql"),
        PostDeployHook.model_construct(sql_script="../secret.sql"),
    ]

    with pytest.raises(ClickException):
        render_script_templates(project_root, {}, hooks)


def test_render_script_templates_allow_generated_tempfile_hook(project_with_scripts):
    project_root, secret = project_with_scripts
    hooks = [PostDeployHook(sql_script="scripts/post.sql")]
    with context({"allow_generated_sql_script_path": True}):
        generated_hook = PostDeployHook(sql_script=str(secret))
    generated_hook._display_path = "scripts/generated.sql"  # noqa: SLF001
    hooks.append(generated_hook)

    assert render_script_templates(project_root, {}, hooks) == ["SELECT 1;", "LEAKED"]


def test_project_root_symlink_resolution(tmp_path):
    real_root = tmp_path / "real_project"
    real_root.mkdir()
    (real_root / "scripts").mkdir()
    (real_root / "scripts" / "post.sql").write_text("SELECT 42;")

    link_root = tmp_path / "link_project"
    link_root.symlink_to(real_root, target_is_directory=True)

    assert render_script_template(link_root, {}, "scripts/post.sql") == "SELECT 42;"
