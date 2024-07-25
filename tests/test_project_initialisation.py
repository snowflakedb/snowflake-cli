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
from tempfile import TemporaryDirectory
from unittest import mock

from snowflake.cli.api.commands.project_initialisation import add_init_command
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.secure_path import SecurePath
from typer.testing import CliRunner


@mock.patch.object(SecurePath, "copy")
def test_adds_init_command(mock_copy):
    app = SnowTyperFactory()
    runner = CliRunner()

    with TemporaryDirectory() as tmp_templates:
        template_path = Path(tmp_templates) / "my_template"
        template_path.mkdir()

        test_file = template_path / "file.txt"
        test_file.touch()

        with mock.patch(
            "snowflake.cli.api.commands.project_initialisation.TEMPLATES_PATH",
            Path(tmp_templates),
        ):
            add_init_command(app, "my_project_type", template="my_template")
            result = runner.invoke(app.create_instance(), ["my_dir"])
        assert result.exit_code == 0
        assert (
            result.output
            == "DeprecationWarning: The command 'init' is deprecated.\nInitialized the new project in my_dir/\n"
        )

    mock_copy.assert_called_once_with("my_dir", dirs_exist_ok=True)
