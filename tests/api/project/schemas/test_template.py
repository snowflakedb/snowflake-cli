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

from unittest import mock

from snowflake.cli.api.project.schemas.template import TemplateVariable


@mock.patch(
    "snowflake.cli.api.project.schemas.template.typer.prompt", return_value="blue"
)
def test_prompt_user_for_value_labels_and_sanitizes_prompt(mock_prompt):
    variable = TemplateVariable(name="color", prompt="\x1b[31mEnter a color\x1b[0m")

    assert variable.prompt_user_for_value(no_interactive=False) == "blue"

    shown = mock_prompt.call_args.args[0]
    assert shown == "From template: Enter a color"
    assert "\x1b" not in shown
    mock_prompt.assert_called_once_with(
        "From template: Enter a color", default=None, type=str
    )


@mock.patch(
    "snowflake.cli.api.project.schemas.template.typer.prompt", return_value="app"
)
def test_prompt_user_for_value_labels_variable_name_when_prompt_omitted(mock_prompt):
    variable = TemplateVariable(name="project_name")

    assert variable.prompt_user_for_value(no_interactive=False) == "app"

    mock_prompt.assert_called_once_with(
        "From template: project_name", default=None, type=str
    )


@mock.patch(
    "snowflake.cli.api.project.schemas.template.typer.prompt", return_value="ok"
)
def test_prompt_user_for_value_control_only_prompt_keeps_template_label(mock_prompt):
    variable = TemplateVariable(name="color", prompt="\x1b[2J\x1b[H")

    assert variable.prompt_user_for_value(no_interactive=False) == "ok"

    mock_prompt.assert_called_once_with("From template", default=None, type=str)


@mock.patch(
    "snowflake.cli.api.project.schemas.template.typer.prompt", return_value="west"
)
def test_prompt_user_for_value_sanitizes_string_default(mock_prompt):
    variable = TemplateVariable(name="region", default="\x1b[31mus-west\x1b[0m")

    assert variable.prompt_user_for_value(no_interactive=False) == "west"

    mock_prompt.assert_called_once_with(
        "From template: region", default="us-west", type=str
    )


def test_prompt_user_for_value_no_interactive_uses_sanitized_string_default():
    variable = TemplateVariable(name="region", default="\x1b[31mus-west\x1b[0m")

    assert variable.prompt_user_for_value(no_interactive=True) == "us-west"


@mock.patch(
    "snowflake.cli.api.project.schemas.template.typer.prompt", return_value="blue"
)
def test_prompt_user_for_value_strips_carriage_return_from_prompt(mock_prompt):
    variable = TemplateVariable(name="color", prompt="\rEnter a color")

    assert variable.prompt_user_for_value(no_interactive=False) == "blue"

    shown = mock_prompt.call_args.args[0]
    assert shown == "From template: Enter a color"
    assert "\r" not in shown
    mock_prompt.assert_called_once_with(
        "From template: Enter a color", default=None, type=str
    )


@mock.patch(
    "snowflake.cli.api.project.schemas.template.typer.prompt", return_value="ok"
)
def test_prompt_user_for_value_cr_only_prompt_keeps_template_label(mock_prompt):
    variable = TemplateVariable(name="color", prompt="\r")

    assert variable.prompt_user_for_value(no_interactive=False) == "ok"

    mock_prompt.assert_called_once_with("From template", default=None, type=str)


@mock.patch(
    "snowflake.cli.api.project.schemas.template.typer.prompt", return_value="west"
)
def test_prompt_user_for_value_strips_carriage_return_from_string_default(mock_prompt):
    variable = TemplateVariable(name="region", default="us-\rwest")

    assert variable.prompt_user_for_value(no_interactive=False) == "west"

    mock_prompt.assert_called_once_with(
        "From template: region", default="us-west", type=str
    )


def test_prompt_user_for_value_no_interactive_strips_cr_from_string_default():
    variable = TemplateVariable(name="region", default="us-\rwest")

    assert variable.prompt_user_for_value(no_interactive=True) == "us-west"
