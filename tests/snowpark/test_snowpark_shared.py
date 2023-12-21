from zipfile import ZipFile

import snowcli.cli.snowpark_shared as shared
from requirements.requirement import Requirement
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
    app_root = Path("app")
    app_root.mkdir()
    app_root.joinpath(Path("app.py")).touch()

    shared.snowpark_package(app_root, Path("app.zip"), "yes", False, "yes")

    zip_path = os.path.join(temp_dir, "app.zip")
    assert os.path.isfile(zip_path)
    assert ZipFile(zip_path).namelist() == [
        "app.py",
        os.path.join("totally-awesome-package", "totally-awesome-module.py"),
    ]
