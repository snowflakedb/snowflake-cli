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

from __future__ import annotations

from io import StringIO

import pytest
from rich.console import Console
from rich.text import Text
from snowflake.cli.api.commands.command_docs import (
    Code,
    CommandDocs,
    Example,
    PlainText,
    Ref,
    RelatedLink,
)
from snowflake.cli.api.commands.command_docs_rendering import (
    STYLE_INLINE_CODE,
    render_usage_help,
)
from snowflake.cli.api.commands.docs_help import (
    DOCS_BASE_URL,
    _absolute_url,
    _docs_panels,
    _example_lines,
    _fold_at_slash,
    _panel,
    _related_list,
    _RelatedTopic,
)

_LONG_URL = (
    "https://docs.snowflake.com/en/developer-guide/snowflake-cli/"
    "command-reference/overview"
)


def _render(renderable, width: int = 80) -> str:
    buf = StringIO()
    Console(file=buf, width=width, height=40, color_system=None).print(renderable)
    return buf.getvalue()


def test_absolute_url_empty_versus_relative_versus_absolute():
    assert _absolute_url("") == f"{DOCS_BASE_URL}/"
    assert _absolute_url("developer-guide/index") == (
        f"{DOCS_BASE_URL}/developer-guide/index"
    )
    assert _absolute_url("/developer-guide/index") == (
        f"{DOCS_BASE_URL}/developer-guide/index"
    )
    assert _absolute_url("https://example.com/x") == "https://example.com/x"
    assert _absolute_url("http://example.com/x") == "http://example.com/x"


def test_fold_at_slash_empty_versus_fits():
    assert _fold_at_slash("", 10) == [""]
    assert _fold_at_slash("https://example.com/foo", 80) == ["https://example.com/foo"]
    assert _fold_at_slash(_LONG_URL, 0) == [_LONG_URL]


def test_fold_at_slash_keeps_path_segments_whole():
    lines = _fold_at_slash(_LONG_URL, 64)

    assert len(lines) > 1, "this URL is meant to need folding"
    assert "".join(lines) == _LONG_URL
    assert all(len(line) <= 64 for line in lines)
    assert all(line.endswith("/") for line in lines[:-1])
    for segment in _LONG_URL.split("/"):
        assert any(segment in line for line in lines)


def test_fold_at_slash_cuts_a_segment_longer_than_the_width():
    lines = _fold_at_slash("https://example.com/" + "a" * 30, 20)

    assert "".join(lines) == "https://example.com/" + "a" * 30
    assert all(len(line) <= 20 for line in lines)


def test_render_usage_help_empty_versus_text():
    assert _render(render_usage_help(())).strip() == ""
    assert _render(render_usage_help((PlainText(parts=()),))).strip() == ""
    rendered = _render(render_usage_help((PlainText(parts=("One sentence.",)),)))
    assert rendered.strip() == "One sentence."


def test_render_usage_help_joins_string_parts_and_keeps_angle_brackets():
    rendered = _render(render_usage_help((PlainText(parts=("Use ", "<name>", ".")),)))
    assert rendered.strip() == "Use <name>."


def test_render_usage_help_styles_a_code_span():
    blocks = (PlainText(parts=("Pass ", Code(value="--target"), " to pick a target.")),)
    (paragraph,) = render_usage_help(blocks).renderables

    assert paragraph.plain == "Pass --target to pick a target."
    assert [(span.start, span.end, span.style) for span in paragraph.spans] == [
        (5, 13, STYLE_INLINE_CODE)
    ]


def test_code_rejects_backticks_in_value():
    with pytest.raises(ValueError, match="must not contain backticks"):
        Code(value="`rm -rf`")


def test_render_usage_help_expands_known_reference():
    rendered = _render(
        render_usage_help(
            (PlainText(parts=("Create a ", Ref(name="dcm-object"), ".")),)
        )
    )

    assert rendered.strip() == "Create a DCM project."


def test_ref_accepts_known_name():
    assert Ref(name="dcm-object").name == "dcm-object"


def test_ref_rejects_unknown_name():
    with pytest.raises(ValueError, match="unknown prod-docs reference"):
        Ref(name="unknown")


def test_render_usage_help_keeps_newlines_and_separates_plain_text_blocks():
    rendered = _render(
        render_usage_help(
            (
                PlainText(parts=("First line,\nsecond line.",)),
                PlainText(parts=("New paragraph.\nStill it.",)),
            )
        )
    )
    assert rendered == "First line,\nsecond line.\n\nNew paragraph.\nStill it.\n"


def test_render_usage_help_rejects_unknown_blocks():
    with pytest.raises(TypeError, match="Unsupported usage-note block"):
        render_usage_help(("not a block",))  # type: ignore[arg-type]


def test_panel_keeps_the_title():
    panel = _panel("Usage notes", Text("hi"))

    assert panel.title == "Usage notes"
    assert "hi" in _render(panel)


def test_example_lines_empty_versus_optional_fields():
    assert list(_example_lines(())) == []

    command_only = list(_example_lines((Example(command="snow foo"),)))
    assert [line.plain for line in command_only] == ["snow foo"]

    full = list(
        _example_lines(
            (
                Example(
                    command="snow a",
                    description=PlainText(parts=("First",)),
                    output="out",
                ),
                Example(command="snow b"),
            )
        )
    )
    assert [line.plain for line in full] == [
        "First",
        "snow a",
        "Output: out",
        "",
        "snow b",
    ]


def test_example_description_renders_spans():
    (description, _) = _example_lines(
        (
            Example(
                command="snow dcm describe MY_PROJECT",
                description=PlainText(
                    parts=(
                        "Describe ",
                        Code(value="MY_PROJECT"),
                        " as a ",
                        Ref(name="dcm-object"),
                        ".",
                    )
                ),
            ),
        )
    )

    assert description.plain == "Describe MY_PROJECT as a DCM project."
    assert [span.style for span in description.spans] == [STYLE_INLINE_CODE]


def test_related_topic_untitled_is_the_url():
    rendered = _render(_RelatedTopic(RelatedLink(href="/foo")))

    assert f"{DOCS_BASE_URL}/foo" in rendered
    assert rendered.count("\n") == 1


def test_related_topic_puts_the_title_above_the_url():
    rendered = _render(
        _RelatedTopic(RelatedLink(href="/foo", title="Foo docs")), width=40
    )
    lines = [line.strip() for line in rendered.splitlines() if line.strip()]

    assert lines[0].startswith("Foo docs")
    assert any(f"{DOCS_BASE_URL}/foo" in line for line in lines)


def test_related_list_empty_versus_one_link():
    empty = _render(_related_list(()))
    assert empty.strip() == ""

    rendered = _render(
        _related_list((RelatedLink(href="/foo", title="Foo docs"),)), width=80
    )
    assert "•" in rendered
    assert "Foo docs" in rendered
    assert f"{DOCS_BASE_URL}/foo" in rendered


def test_docs_panels_empty_versus_each_section():
    assert list(_docs_panels(CommandDocs())) == []
    assert list(_docs_panels(CommandDocs(usage_notes=()))) == []

    usage = list(
        _docs_panels(
            CommandDocs(usage_notes=(PlainText(parts=("Only on Tuesdays.",)),))
        )
    )
    examples = list(_docs_panels(CommandDocs(examples=(Example(command="snow demo"),))))
    related = list(_docs_panels(CommandDocs(related=(RelatedLink(href="/foo"),))))

    assert [panel.title for panel in usage] == ["Usage notes"]
    assert [panel.title for panel in examples] == ["Examples"]
    assert [panel.title for panel in related] == ["Related topics"]
    assert "Only on Tuesdays." in _render(usage[0])
    assert "snow demo" in _render(examples[0])
    assert f"{DOCS_BASE_URL}/foo" in _render(related[0])
