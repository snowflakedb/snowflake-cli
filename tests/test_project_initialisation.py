from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from snowcli.cli.common.project_initialisation import add_init_command
from typer import Typer
from typer.testing import CliRunner


@mock.patch("shutil.copytree")
@mock.patch("importlib.util.find_spec")
def test_adds_init_command(mock_find_spec, mock_copy):
    app = Typer()
    runner = CliRunner()

    with TemporaryDirectory() as tmp_templates:
        template_path = Path(tmp_templates) / "my_template"
        template_path.mkdir()

        mock_find_spec.return_value.origin = template_path

        test_file = template_path / "file.txt"
        test_file.touch()

        add_init_command(app, "my_project_type", template="my_template")
        result = runner.invoke(app, ["my_dir"])
        assert result.exit_code == 0
        assert result.output == "Initialized the new project in my_dir/\n"

    mock_find_spec.assert_called_once_with("templates")
    mock_copy.assert_called_once_with(template_path, "my_dir", dirs_exist_ok=True)
