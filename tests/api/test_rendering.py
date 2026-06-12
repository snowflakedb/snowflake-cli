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

import os
from unittest import mock

import pytest
from click import ClickException
from jinja2 import UndefinedError
from snowflake.cli.api.rendering.project_definition_templates import (
    has_client_side_templates,
)
from snowflake.cli.api.rendering.sql_templates import (
    SQLTemplateSyntaxConfig,
    has_sql_templates,
    snowflake_sql_jinja_render,
)
from snowflake.cli.api.utils.models import ProjectEnvironment


@pytest.fixture
def cli_context():
    with mock.patch(
        "snowflake.cli.api.rendering.sql_templates.get_cli_context"
    ) as cli_context:
        cli_context().template_context = {
            "ctx": {"env": ProjectEnvironment(default_env={}, override_env={})}
        }
        yield cli_context()


def test_rendering_with_data(cli_context):
    assert (
        snowflake_sql_jinja_render(
            "&{ foo }",
            data={"foo": "bar"},
            template_syntax_config=SQLTemplateSyntaxConfig(),
        )
        == "bar"
    )


@pytest.mark.parametrize(
    "text, output",
    [
        # Green path
        ("&{ foo }", "bar"),
        # Using $ as sf variable and basic jinja for server side
        ("${{ foo }}", "${{ foo }}"),
        ("$&{ foo }{{ var }}", "$bar{{ var }}"),
        ("${{ &{ foo } }}", "${{ bar }}"),
        # Using $ as sf variable and client side rendering
        ("$&{ foo }", "$bar"),
    ],
)
def test_rendering(text, output, cli_context):
    assert (
        snowflake_sql_jinja_render(
            text, data={"foo": "bar"}, template_syntax_config=SQLTemplateSyntaxConfig()
        )
        == output
    )


@pytest.mark.parametrize(
    "text",
    [
        """
    {% for item in navigation %}
        <li><a href="{{ item.href }}">{{ item.caption }}</a></li>
    {% endfor %}
    """,
        """{% if loop.index is divisibleby 3 %}""",
        """
    {% if True %}
        yay
    {% endif %}
    """,
    ],
)
def test_that_common_logic_block_are_ignored(text, cli_context):
    assert (
        snowflake_sql_jinja_render(
            text, template_syntax_config=SQLTemplateSyntaxConfig()
        )
        == text
    )


def test_that_common_comments_are_respected(cli_context):
    # Make sure comment are ignored
    assert (
        snowflake_sql_jinja_render(
            "{# note a comment &{ foo } #}",
            template_syntax_config=SQLTemplateSyntaxConfig(),
        )
        == ""
    )
    # Make sure comment's work together with templates
    assert (
        snowflake_sql_jinja_render(
            "{# note a comment #}&{ foo }",
            data={"foo": "bar"},
            template_syntax_config=SQLTemplateSyntaxConfig(),
        )
        == "bar"
    )


@pytest.mark.parametrize(
    "text",
    [
        "&{ctx.env.__class__}",
        "&{ctx.env.get}",
        "&{foo}",
    ],
)
def test_that_undefined_variables_raise_error(text, cli_context):
    with pytest.raises(UndefinedError):
        snowflake_sql_jinja_render(
            text, template_syntax_config=SQLTemplateSyntaxConfig()
        )


@pytest.mark.parametrize(
    "key_word",
    [
        "ctx",
        "fn",
    ],
)
def test_reserved_keywords_raise_error(key_word, cli_context):
    with pytest.raises(ClickException) as err:
        snowflake_sql_jinja_render(
            "select 1;",
            data={key_word: "some_value"},
            template_syntax_config=SQLTemplateSyntaxConfig(),
        )
    assert (
        err.value.message
        == f"{key_word} in user defined data. The `{key_word}` variable is reserved for CLI usage."
    )


@mock.patch.dict(os.environ, {"TEST_ENV_VAR": "foo"})
def test_contex_can_access_environment_variable(cli_context):
    assert snowflake_sql_jinja_render(
        "&{ ctx.env.TEST_ENV_VAR }", template_syntax_config=SQLTemplateSyntaxConfig()
    ) == os.environ.get("TEST_ENV_VAR")


def test_has_sql_templates():
    assert has_sql_templates("abc <% %> abc")
    assert has_sql_templates("abc <% abc")
    assert has_sql_templates("abc &{ foo } abc")
    assert has_sql_templates("abc &{ abc")
    assert not has_sql_templates("SELECT 1")
    assert not has_sql_templates("<test>")
    assert not has_sql_templates("{<est}")
    assert not has_sql_templates("")


def test_has_client_side_templates():
    assert has_client_side_templates("abc <% %> abc")
    assert has_client_side_templates("abc <% abc")
    assert not has_client_side_templates("abc &{ foo } abc")
    assert not has_client_side_templates("abc &{ abc")
    assert not has_client_side_templates("SELECT 1")
    assert not has_client_side_templates("<test>")
    assert not has_client_side_templates("{<est}")
    assert not has_client_side_templates("")


# --- read_file_content / procedure_from_js_file containment tests -----------


@pytest.fixture
def jinja_cli_context():
    """Patches get_cli_context wherever jinja.py imports it, for filter tests."""
    with mock.patch(
        "snowflake.cli.api.cli_global_context.get_cli_context"
    ) as ctx, mock.patch(
        "snowflake.cli.api.rendering.sql_templates.get_cli_context"
    ) as sql_ctx:
        sql_ctx().template_context = {
            "ctx": {"env": ProjectEnvironment(default_env={}, override_env={})}
        }
        yield ctx()


def _render(content: str) -> str:
    return snowflake_sql_jinja_render(
        content,
        template_syntax_config=SQLTemplateSyntaxConfig(enable_jinja_syntax=True),
    )


def test_read_file_content_allows_file_inside_project_root(tmp_path, jinja_cli_context):
    jinja_cli_context.project_root = tmp_path
    target = tmp_path / "inside.txt"
    target.write_text("hello from project")
    assert (
        _render(f"{{{{ '{target.as_posix()}' | read_file_content }}}}")
        == "hello from project"
    )


def test_read_file_content_allows_relative_path_inside_project_root(
    tmp_path, jinja_cli_context, monkeypatch
):
    jinja_cli_context.project_root = tmp_path
    target = tmp_path / "relative.txt"
    target.write_text("relative content")
    # CWD deliberately differs from project root — relative path must be
    # anchored to project_root, not CWD.
    monkeypatch.chdir("/")
    assert _render("{{ 'relative.txt' | read_file_content }}") == "relative content"


def test_read_file_content_rejects_path_outside_project_root(
    tmp_path, jinja_cli_context
):
    project_root = tmp_path / "project"
    project_root.mkdir()
    jinja_cli_context.project_root = project_root

    outside = tmp_path / "outside.secret"
    outside.write_text("SECRET")

    with pytest.raises(ClickException) as err:
        _render(f"{{{{ '{outside.as_posix()}' | read_file_content }}}}")
    assert "outside the project root" in err.value.message
    assert "read_file_content" in err.value.message


def test_read_file_content_rejects_parent_traversal(tmp_path, jinja_cli_context):
    project_root = tmp_path / "project"
    project_root.mkdir()
    jinja_cli_context.project_root = project_root

    outside = tmp_path / "outside.secret"
    outside.write_text("SECRET")

    traversal = project_root / ".." / "outside.secret"
    with pytest.raises(ClickException) as err:
        _render(f"{{{{ '{traversal.as_posix()}' | read_file_content }}}}")
    assert "outside the project root" in err.value.message


def test_procedure_from_js_file_rejects_path_outside_project_root(
    tmp_path, jinja_cli_context
):
    project_root = tmp_path / "project"
    project_root.mkdir()
    jinja_cli_context.project_root = project_root

    outside_js = tmp_path / "evil.js"
    outside_js.write_text("return 42;")

    with pytest.raises(ClickException) as err:
        _render(f"{{{{ '{outside_js.as_posix()}' | procedure_from_js_file }}}}")
    assert "outside the project root" in err.value.message
    assert "procedure_from_js_file" in err.value.message


def test_procedure_from_js_file_allows_file_inside_project_root(
    tmp_path, jinja_cli_context
):
    jinja_cli_context.project_root = tmp_path
    js = tmp_path / "proc.js"
    js.write_text("return arguments[0];")
    rendered = _render(f"{{{{ '{js.as_posix()}' | procedure_from_js_file }}}}")
    assert "return arguments[0];" in rendered
    assert "module.exports = exports;" in rendered


def test_read_file_content_enforces_default_size_limit(tmp_path, jinja_cli_context):
    """A file over DEFAULT_SIZE_LIMIT_MB (128 MB) must be rejected rather than
    read unbounded (the UNLIMITED bypass is removed)."""
    from snowflake.cli.api.exceptions import FileTooLargeError

    jinja_cli_context.project_root = tmp_path
    target = tmp_path / "big.txt"
    target.write_text("x")

    with mock.patch(
        "snowflake.cli.api.secure_path.SecurePath._assert_file_size_limit"
    ) as assert_size:
        assert_size.side_effect = FileTooLargeError(target, 128)
        with pytest.raises(FileTooLargeError):
            _render(f"{{{{ '{target.as_posix()}' | read_file_content }}}}")
