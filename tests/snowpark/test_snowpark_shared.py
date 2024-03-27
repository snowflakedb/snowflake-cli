import os
from pathlib import Path
from unittest import mock
from zipfile import ZipFile

import snowflake.cli.plugins.snowpark.snowpark_shared as shared
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.models import Requirement, SplitRequirements
from snowflake.cli.plugins.snowpark.snowpark_package_paths import SnowparkPackagePaths


@mock.patch(
    "snowflake.cli.plugins.snowpark.package.anaconda.AnacondaChannel.parse_anaconda_packages"
)
@mock.patch("snowflake.cli.plugins.snowpark.package_utils.download_packages")
def test_snowpark_package(
    mock_download,
    mock_parse,
    temp_dir,
    correct_requirements_txt,
    dot_packages_directory,
):

    mock_parse.return_value = SplitRequirements(
        [], [Requirement.parse("totally-awesome-package")]
    )

    mock_download.return_value = (
        True,
        SplitRequirements([Requirement.parse("another-package-in-anaconda")], []),
    )
    app_root = Path("app")
    app_root.mkdir()
    app_root.joinpath(Path("app.py")).touch()

    shared.snowpark_package(
        SnowparkPackagePaths(
            source=SecurePath(app_root),
            artifact_file=SecurePath("app.zip"),
        ),
        pypi_download="yes",
        check_anaconda_for_pypi_deps=False,
        package_native_libraries="yes",
    )

    zip_path = os.path.join(temp_dir, "app.zip")
    assert os.path.isfile(zip_path)
    assert ZipFile(zip_path).namelist() == [
        "app.py",
        os.path.join("totally-awesome-package", "totally-awesome-module.py"),
    ]
