from zipfile import ZipFile

import snowcli.cli.snowpark_shared as shared
import typer
from requirements.requirement import Requirement
from snowcli.cli.snowpark.commands import _replace_handler_in_zip
from snowcli.utils import SplitRequirements

from tests.testing_utils.fixtures import *


@mock.patch("tests.snowpark.test_snowpark_shared.shared.utils.parse_anaconda_packages")
@mock.patch("tests.snowpark.test_snowpark_shared.shared.utils.install_packages")
def test_snowpark_package(
    mock_install, mock_parse, temp_dir, correct_requirements_txt, dot_packages_directory
):

    mock_parse.return_value = SplitRequirements(
        [], [Requirement.parse("totally-awesome-package")]
    )

    mock_install.return_value = (
        True,
        SplitRequirements([Requirement.parse("another-package-in-anaconda")], []),
    )
    Path("app.py").touch()

    shared.snowpark_package("yes", False, "yes")

    zip_path = os.path.join(temp_dir, "app.zip")
    assert os.path.isfile(zip_path)
    assert ZipFile(zip_path).namelist() == [
        "app.py",
        os.path.join(
            ".packages", "totally-awesome-package", "totally-awesome-module.py"
        ),
    ]


@mock.patch(
    "tests.snowpark.test_snowpark_shared.shared.utils.generate_snowpark_coverage_wrapper"
)
def test_replace_handler_in_zip_with_wrong_handler(mock_wrapper, temp_dir, app_zip):
    with pytest.raises(typer.Abort):
        _replace_handler_in_zip(
            proc_name="hello",
            proc_signature="()",
            handler="app.hello.world",
            zip_file_path=app_zip,
            coverage_reports_stage="@example",
            coverage_reports_stage_path="test_db.public.example",
        )

    mock_wrapper.assert_not_called()
