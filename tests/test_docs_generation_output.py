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
from textwrap import dedent
from typing import Optional
from unittest import mock

import pytest
import typer
from click import Command
from pydantic.json_schema import GenerateJsonSchema, model_json_schema
from snowflake.cli._app.dev.docs.commands_docs_generator import (
    _additional_section,
    _command_page_markdown,
    _page_examples_fallback,
    _page_usage_notes_fallback,
    _split_docstring,
    collapse_whitespace,
    mdx_escape,
)
from snowflake.cli.api.commands.command_docs import (
    DOCS_ATTRIBUTE,
    Code,
    CommandDocs,
    Example,
    PlainText,
    Ref,
    RelatedLink,
)
from snowflake.cli.api.commands.command_docs_rendering import (
    render_paragraph_mdx,
    render_usage_mdx,
)
from snowflake.cli.api.project.schemas.project_definition import DefinitionV11
from typer.main import get_command


@mock.patch(
    "snowflake.cli._app.dev.docs.project_definition_generate_json_schema.ProjectDefinitionGenerateJsonSchema.generate"
)
def test_definition_file_format_generated_from_json(
    mock_generate, runner, temporary_directory
):
    property1 = {
        "path": "propertyA",
        "title": "Title of property A",
        "indents": 0,
        "item_index": 0,
        "required": False,
        "name": "propertyA",
        "add_types": False,
        "types": "",
    }

    property2 = {
        "path": "propertyA.propertyB",
        "title": "Title of property B",
        "description": "Description of property B",
        "indents": 1,
        "item_index": 1,
        "required": True,
        "name": "propertyB",
        "add_types": True,
        "types": "string",
    }
    result = {
        "result": [
            {
                "properties": [property1, property2],
                "title": "SECTION_TITLE",
                "name": "section_demo",
            }
        ]
    }

    mock_generate.return_value = result

    runner.invoke(["--docs"])
    project_definition_path = (
        Path(temporary_directory)
        / "gen_docs"
        / "project_definition"
        / "definition_section_demo.mdx"
    )

    assert project_definition_path.read_text() == dedent(
        """\
## SECTION_TITLE

### Project definition structure

```

  propertyA:
    - propertyB: <string>
```

### Project definition properties

The following table describes the project definition properties.

<Table>
  <thead>
    <tr>
      <th>Property</th>
      <th>Definition</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>

**propertyA**

*Optional*

      </td>
      <td>

Title of property A

      </td>
    </tr>
    <tr>
      <td>

**propertyA.propertyB**

*Required*, *string*

      </td>
      <td>

Title of property B

Description of property B

      </td>
    </tr>
  </tbody>
</Table>
"""
    )


def test_files_generated_for_each_optional_project_definition_property(
    runner, temporary_directory
):
    runner.invoke(["--docs"])
    project_definition_path = (
        Path(temporary_directory) / "gen_docs" / "project_definition"
    )
    errors = []

    model_json = model_json_schema(DefinitionV11, schema_generator=GenerateJsonSchema)
    for property_name in model_json["properties"]:
        if property_name in model_json["required"]:
            continue
        if not (project_definition_path / f"definition_{property_name}.mdx").exists():
            errors.append(f"Section `{property_name}` was not properly generated")

    assert len(errors) == 0, "\n".join(errors)


def test_all_commands_have_generated_files(
    runner, temporary_directory, get_click_context
):
    runner.invoke(["--docs"])

    commands_path = Path(temporary_directory) / "gen_docs" / "commands"

    errors = []

    def _check(command: Command, directory_path: Path, command_path=None):
        if command_path is None:
            command_path = []
        if getattr(command, "hidden", False):
            return
        if hasattr(command, "commands"):
            for command_name, command_info in command.commands.items():
                new_directory_path = (
                    directory_path / command.name
                    if command.name != "default"
                    else directory_path
                )
                _check(command_info, new_directory_path, [*command_path, command_name])
        else:
            if not (directory_path / f"usage-{command.name}.mdx").exists():
                errors.append(
                    f"Command `{' '.join(command_path)}` documentation was not properly generated"
                )

    app = get_click_context().command
    assert (
        len(app.commands) >= 1
    )  # confirm that test is actually checking some commands
    _check(get_click_context().command, commands_path)

    assert len(errors) == 0, "\n".join(errors)


def test_flags_have_default_values(runner, temporary_directory, snapshot):
    runner.invoke(["--docs"])

    # cortex complete checks:
    # "Default: False" case
    # "--diag-log-path" flag, with tempdir path as default value
    example_generated_file = (
        Path(temporary_directory)
        / "gen_docs"
        / "commands"
        / "cortex"
        / "usage-complete.mdx"
    )
    assert example_generated_file.exists()
    assert example_generated_file.read_text() == snapshot


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ""),
        ("", ""),
        ("plain text", "plain text"),
        ("<system_temporary_directory>", "&lt;system_temporary_directory&gt;"),
        ("before <value> after", "before &lt;value&gt; after"),
    ],
)
def test_mdx_escape(value, expected):
    assert mdx_escape(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ""),
        ("", ""),
        ("   \n  ", ""),
        ("plain text", "plain text"),
        ("first line\nsecond line", "first line second line"),
        ("\n    indented.\n    continued.\n    ", "indented. continued."),
    ],
)
def test_collapse_whitespace(value, expected):
    assert collapse_whitespace(value) == expected


def test_option_metavar_angle_brackets_are_escaped():
    app = typer.Typer(add_completion=False)

    @app.command("list")
    def list_(
        in_scope: tuple[str, str] = typer.Option(
            (None, None),
            "--in",
            help="Specifies the scope of this command.",
        ),
    ):
        """Lists objects."""

    page = _command_page_markdown(get_command(app), ["plugin", "list"])
    assert "<em>&lt;TEXT TEXT&gt;...</em>" in page
    assert "<em><TEXT TEXT>...</em>" not in page


def test_multiline_argument_help_renders_as_a_single_line():
    app = typer.Typer(add_completion=False)

    @app.command("describe")
    def describe(
        identifier: Optional[str] = typer.Argument(
            None,
            help="""
                Identifier of the object. Example: MY_DB.MY_SCHEMA.MY_OBJECT.
                Optional if `--target` is defined in the manifest.
            """,
        ),
    ):
        """Describes an object."""

    page = _command_page_markdown(get_command(app), ["plugin", "describe"])
    arguments = page[page.index("## Arguments") : page.index("## Options")]
    help_line = next(
        line
        for line in arguments.splitlines()
        if line.startswith("Identifier of the object.")
    )
    assert "  " not in help_line
    assert help_line.endswith("is defined in the manifest.")


def test_command_help_angle_brackets_are_escaped():
    app = typer.Typer(add_completion=False)

    @app.command("demo")
    def demo():
        """Runs the <object> command."""

    overview = _command_page_markdown(get_command(app), ["plugin", "demo"]).split(
        "## Syntax"
    )[0]
    assert "Runs the &lt;object&gt; command." in overview
    assert "Runs the <object> command." not in overview


def test_render_usage_mdx_empty_versus_plain_text():
    assert render_usage_mdx(()) == ""
    assert render_usage_mdx((PlainText(parts=()),)) == ""
    assert (
        render_usage_mdx(
            (
                PlainText(parts=("Use <name>.",)),
                PlainText(parts=("Second paragraph.",)),
            )
        )
        == "Use &lt;name&gt;.\n\nSecond paragraph."
    )
    assert (
        render_usage_mdx((PlainText(parts=("Use ", "<name>", ".")),))
        == "Use &lt;name&gt;."
    )


def test_render_usage_mdx_rejects_unknown_blocks():
    with pytest.raises(TypeError, match="Unsupported usage-note block"):
        render_usage_mdx(("not a block",))  # type: ignore[arg-type]


def test_render_usage_mdx_code_span_keeps_angle_brackets_unescaped():
    assert (
        render_usage_mdx(
            (
                PlainText(
                    parts=("Pass ", Code(value='-D "<key>=<value>"'), " to set it.")
                ),
            )
        )
        == 'Pass `-D "<key>=<value>"` to set it.'
    )


def test_render_usage_mdx_reference():
    assert (
        render_usage_mdx((PlainText(parts=("Create a ", Ref(name="dcm-object"), ".")),))
        == "Create a %dcm-object%."
    )


def test_render_paragraph_mdx_empty_versus_mixed_spans():
    assert render_paragraph_mdx(PlainText(parts=())) == ""
    assert (
        render_paragraph_mdx(
            PlainText(
                parts=(
                    "Preview ",
                    Code(value="MY_TABLE"),
                    " in a ",
                    Ref(name="dcm-object"),
                    ".",
                )
            )
        )
        == "Preview `MY_TABLE` in a %dcm-object%."
    )


def test_additional_section_empty_versus_missing():
    assert _additional_section({}, "Usage notes") is None
    assert _additional_section({"additional_sections": []}, "Usage notes") is None
    assert _additional_section(_split_docstring("Help only."), "Usage notes") is None

    params = _split_docstring("Help.\n\n## Usage notes\n\nThe notes.")
    assert _additional_section(params, "Usage notes") == "The notes."
    assert _additional_section(params, "Examples") is None

    empty_section = _split_docstring("Help.\n\n## Usage notes\n\n")
    assert _additional_section(empty_section, "Usage notes") is None


def test_page_usage_notes_fallback_empty_versus_missing():
    params = _split_docstring("Help.\n\n## Usage notes\n\nFrom the <docstring>.")
    assert _page_usage_notes_fallback(CommandDocs(), params) == "From the <docstring>."
    assert _page_usage_notes_fallback(CommandDocs(), {}) is None
    assert (
        _page_usage_notes_fallback(
            CommandDocs(usage_notes=(PlainText(parts=("From CommandDocs.",)),)), params
        )
        is None
    )
    assert _page_usage_notes_fallback(CommandDocs(usage_notes=()), params) is None


def test_page_examples_fallback_empty_versus_missing():
    params = _split_docstring("Help.\n\n## Examples\n\nsnow git setup <repo>")
    assert _page_examples_fallback(CommandDocs(), params) == "snow git setup <repo>"
    assert _page_examples_fallback(CommandDocs(), {}) is None
    assert (
        _page_examples_fallback(
            CommandDocs(examples=(Example(command="snow plugin demo"),)), params
        )
        is None
    )
    assert _page_examples_fallback(CommandDocs(examples=()), params) is None


def _demo_click_command() -> Command:
    app = typer.Typer(add_completion=False)

    @app.command("demo")
    def demo(
        object_name: str = typer.Argument(help="Name of the object."),
        force: bool = typer.Option(
            False, "--force", help="Skip the confirmation prompt."
        ),
    ):
        """Executes the demo object."""

    return get_command(app)


def test_render_command_page_with_structured_docs(snapshot):
    docs = CommandDocs(
        related=(
            RelatedLink(
                href="/developer-guide/snowflake-cli/data-pipelines/dcm-projects",
            ),
            RelatedLink(
                href="/user-guide/dcm-projects/dcm-projects-overview",
                title="DCM <projects> overview",
            ),
        ),
        usage_notes=(
            PlainText(parts=("Use `--force` to skip the prompt.",)),
            PlainText(parts=("The <object_name> object must already exist.",)),
        ),
        examples=(
            Example(
                command="snow plugin demo MY_OBJECT",
                description=PlainText(
                    parts=("The following example executes the <object_name> object:",)
                ),
                output="Object <MY_OBJECT> executed.",
            ),
            Example(
                command="snow plugin demo OTHER --force",
            ),
        ),
    )

    command = _demo_click_command()
    setattr(command.callback, DOCS_ATTRIBUTE, docs)
    rendered = _command_page_markdown(command, ["plugin", "demo"])
    assert rendered.endswith("\n")
    assert "The &lt;object_name&gt; object must already exist." in rendered
    assert "Object <MY_OBJECT> executed." in rendered
    assert rendered == snapshot


def test_docs_pages_empty_extras_for_command_without_docs(runner, temporary_directory):
    result = runner.invoke(["--docs-pages"])
    assert result.exit_code == 0, result.output

    page_path = (
        Path(temporary_directory) / "gen_docs" / "pages" / "cortex" / "sentiment.mdx"
    )
    assert page_path.exists()
    content = page_path.read_text()
    assert content.startswith("---\n")
    assert "title: snow cortex sentiment" in content
    assert "description: ''" in content
    assert (
        "import Help from 'INCLUDE/snowcli/parameter-descriptions/help.mdx'" in content
    )
    assert "# snow cortex sentiment" in content
    assert "<Help />" in content
    assert "## Syntax" in content
    assert "## Arguments" in content
    assert "## Options" in content
    assert "## Usage notes" not in content
    assert "## Examples" not in content
    assert "<RelatedTopics>" not in content
    assert page_path.read_bytes().endswith(b"\n")


def test_render_command_page_falls_back_to_docstring_sections():
    app = typer.Typer(add_completion=False)

    @app.command("setup")
    def setup():
        """Sets up a git repository object.

        ## Usage notes

        You will be prompted for a <url>.

        ## Examples

        snow git setup my_repo
        """

    rendered = _command_page_markdown(get_command(app), ["git", "setup"])
    assert "## Usage notes" in rendered
    assert "You will be prompted for a &lt;url&gt;." in rendered
    assert "## Examples" in rendered
    assert "snow git setup my_repo" in rendered
    assert "## Usage notes\n\nNone" not in rendered
    assert "## Examples\n\nNone" not in rendered
    assert rendered.endswith("\n")
    assert not rendered.endswith("\n\n")


def test_render_command_page_omits_usage_and_examples_when_missing():
    rendered = _command_page_markdown(_demo_click_command(), ["plugin", "demo"])
    assert "## Usage notes" not in rendered
    assert "## Examples" not in rendered
    assert "<RelatedTopics>" not in rendered


def test_empty_command_docs_usage_notes_do_not_fall_back_to_docstring():
    app = typer.Typer(add_completion=False)

    @app.command("setup")
    def setup():
        """Sets up a git repository object.

        ## Usage notes

        From the docstring.
        """

    command = get_command(app)
    setattr(command.callback, DOCS_ATTRIBUTE, CommandDocs(usage_notes=()))
    rendered = _command_page_markdown(command, ["git", "setup"])
    assert "## Usage notes" not in rendered
    assert "From the docstring." not in rendered

    setattr(command.callback, DOCS_ATTRIBUTE, CommandDocs())
    rendered = _command_page_markdown(command, ["git", "setup"])
    assert "From the docstring." in rendered


def test_empty_command_docs_examples_do_not_fall_back_to_docstring():
    app = typer.Typer(add_completion=False)

    @app.command("setup")
    def setup():
        """Sets up a git repository object.

        ## Examples

        snow git setup my_repo
        """

    command = get_command(app)
    setattr(command.callback, DOCS_ATTRIBUTE, CommandDocs(examples=()))
    rendered = _command_page_markdown(command, ["git", "setup"])
    assert "## Examples" not in rendered
    assert "snow git setup my_repo" not in rendered

    setattr(command.callback, DOCS_ATTRIBUTE, CommandDocs())
    rendered = _command_page_markdown(command, ["git", "setup"])
    assert "snow git setup my_repo" in rendered


def test_command_docs_usage_notes_win_over_docstring():
    app = typer.Typer(add_completion=False)

    @app.command("setup")
    def setup():
        """Sets up a git repository object.

        ## Usage notes

        From the docstring.
        """

    command = get_command(app)
    setattr(
        command.callback,
        DOCS_ATTRIBUTE,
        CommandDocs(usage_notes=(PlainText(parts=("From CommandDocs.",)),)),
    )
    rendered = _command_page_markdown(command, ["git", "setup"])
    assert "From CommandDocs." in rendered
    assert "From the docstring." not in rendered


def test_docs_pages_include_cortex_complete_command_docs(
    runner, temporary_directory, snapshot
):
    result = runner.invoke(["--docs-pages"])
    assert result.exit_code == 0, result.output

    page_path = (
        Path(temporary_directory) / "gen_docs" / "pages" / "cortex" / "complete.mdx"
    )
    assert page_path.exists()
    content = page_path.read_text()
    assert "/developer-guide/snowflake-cli/index" in content
    assert "In the simplest use case, the prompt is a single string." in content
    assert "snow cortex complete" in content
    assert content == snapshot


def test_docs_pages_keep_git_setup_docstring_usage_notes(runner, temporary_directory):
    result = runner.invoke(["--docs-pages"])
    assert result.exit_code == 0, result.output

    page_path = Path(temporary_directory) / "gen_docs" / "pages" / "git" / "setup.mdx"
    content = page_path.read_text()
    assert "## Usage notes" in content
    assert "You will be prompted for:" in content
    assert "## Usage notes\n\nNone" not in content


def test_docs_pages_generated_for_each_command(
    runner, temporary_directory, get_click_context
):
    result = runner.invoke(["--docs-pages"])
    assert result.exit_code == 0, result.output

    pages_path = Path(temporary_directory) / "gen_docs" / "pages"
    errors = []

    def _check(command: Command, directory_path: Path, command_path=None):
        if command_path is None:
            command_path = []
        if getattr(command, "hidden", False):
            return
        if hasattr(command, "commands"):
            for command_name, command_info in command.commands.items():
                new_directory_path = (
                    directory_path / command.name
                    if command.name != "default"
                    else directory_path
                )
                _check(command_info, new_directory_path, [*command_path, command_name])
        else:
            if not (directory_path / f"{command.name}.mdx").exists():
                errors.append(
                    f"Command `{' '.join(command_path)}` page was not properly generated"
                )

    app = get_click_context().command
    assert len(app.commands) >= 1
    _check(get_click_context().command, pages_path)

    assert len(errors) == 0, "\n".join(errors)
