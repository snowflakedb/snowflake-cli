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
from unittest import mock

from click import Command
from pydantic.json_schema import GenerateJsonSchema, model_json_schema
from snowflake.cli.api.project.schemas.project_definition import DefinitionV11
from snowflake.cli.app.cli_app import app_context_holder


@mock.patch(
    "snowflake.cli.app.dev.docs.project_definition_generate_json_schema.ProjectDefinitionGenerateJsonSchema.generate"
)
def test_definition_file_format_generated_from_json(mock_generate, runner, temp_dir):
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
        Path(temp_dir)
        / "gen_docs"
        / "project_definition"
        / "definition_section_demo.txt"
    )

    assert project_definition_path.read_text() == dedent(
        """\
SECTION_TITLE

Project definition structure
===============================================================================

.. code-block::
    
    
  propertyA: 
    - propertyB: <string>


Project definition properties
===============================================================================
The following table describes the project definition properties.
    
.. list-table:: Project definition properties
  :widths: 30 70
  :header-rows: 1
    
  * - Property
    - Definition

  * - **propertyA**

      *Optional*

    - Title of property A
 
  * - **propertyA.propertyB**

      *Required*, *string*

    - Title of property B
    
      Description of property B

"""
    )


def test_files_generated_for_each_optional_project_definition_property(
    runner, temp_dir
):
    runner.invoke(["--docs"])
    project_definition_path = Path(temp_dir) / "gen_docs" / "project_definition"
    errors = []

    model_json = model_json_schema(DefinitionV11, schema_generator=GenerateJsonSchema)
    for property_name in model_json["properties"]:
        if property_name in model_json["required"]:
            continue
        if not (project_definition_path / f"definition_{property_name}.txt").exists():
            errors.append(f"Section `{property_name}` was not properly generated")

    assert len(errors) == 0, "\n".join(errors)


def test_all_commands_have_generated_files(runner, temp_dir):
    runner.invoke(["--docs"])

    # invoke help command to populate app context (plugins registration)
    runner.invoke(["--help"])

    ctx = app_context_holder.app_context

    commands_path = Path(temp_dir) / "gen_docs" / "commands"

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
            if not (directory_path / f"usage-{command.name}.txt").exists():
                errors.append(
                    f"Command `{' '.join(command_path)}` documentation was not properly generated"
                )

    _check(ctx.command, commands_path)

    assert len(errors) == 0, "\n".join(errors)
