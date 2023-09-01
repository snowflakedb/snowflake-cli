from requirements.requirement import Requirement
from zipfile import ZipFile

import typer

import snowcli.cli.snowpark_shared as shared
from snowcli.cli.snowpark.procedure.commands import _replace_handler_in_zip
from snowcli.utils import SplitRequirements
from tests.testing_utils.fixtures import *


@mock.patch("tests.snowpark.test_snowpark_shared.shared.utils.parse_anaconda_packages")
@mock.patch("tests.snowpark.test_snowpark_shared.shared.utils.install_packages")
def test_snowpark_package(
    mock_install, mock_parse, temp_dir, correct_requirements_txt, caplog
):

    mock_parse.return_value = SplitRequirements(
        [], [Requirement.parse("totally-awesome-package")]
    )

    mock_install.return_value = (True, None)

    result = shared.snowpark_package("yes", False, "yes")

    zip_path = os.path.join(temp_dir, "app.zip")
    assert os.path.isfile(zip_path)

    with ZipFile(zip_path) as zip:
        assert "requirements.other.txt" in zip.namelist()

        with zip.open("requirements.other.txt") as req_file:
            reqs = req_file.readlines()
            assert b"totally-awesome-package\n" in reqs


@mock.patch("tests.snowpark.test_snowpark_shared.shared.utils.parse_anaconda_packages")
@mock.patch("tests.snowpark.test_snowpark_shared.shared.utils.install_packages")
def test_snowpark_package_with_packages_dir(
    mock_install, mock_parse, temp_dir, correct_requirements_txt, dot_packages_directory
):

    mock_parse.return_value = SplitRequirements(
        [], [Requirement.parse("totally-awesome-package")]
    )
    mock_install.return_value = (
        True,
        SplitRequirements([Requirement.parse("another-package-in-anaconda")], []),
    )

    result = shared.snowpark_package("yes", False, "yes")

    zip_path = os.path.join(temp_dir, "app.zip")
    assert os.path.isfile(zip_path)

    with ZipFile(zip_path) as zipfile:
        assert "requirements.snowflake.txt" in zipfile.namelist()
        assert (
            os.path.join(
                ".packages", "totally-awesome-package", "totally-awesome-module.py"
            )
            in zipfile.namelist()
        )


def test_replace_handler_in_zip(temp_dir, app_zip):
    result = _replace_handler_in_zip(
        proc_name="hello",
        proc_signature="()",
        handler="app.hello",
        temp_dir=temp_dir,
        zip_file_path=app_zip,
        coverage_reports_stage="@example",
        coverage_reports_stage_path="test_db.public.example",
    )
    assert os.path.isfile(app_zip)
    assert result == "snowpark_coverage.measure_coverage"

    with ZipFile(app_zip, "r") as zip:
        assert "snowpark_coverage.py" in zip.namelist()
        with zip.open("snowpark_coverage.py") as coverage:
            coverage_file = coverage.readlines()
            assert b"        return app.hello(*args,**kwargs)\n" in coverage_file
            assert b"    import app\n" in coverage_file


@mock.patch(
    "tests.snowpark.test_snowpark_shared.shared.utils.generate_snowpark_coverage_wrapper"
)
def test_replace_handler_in_zip_with_wrong_handler(mock_wrapper, temp_dir, app_zip):
    with pytest.raises(typer.Abort):
        result = _replace_handler_in_zip(
            proc_name="hello",
            proc_signature="()",
            handler="app.hello.world",
            temp_dir=temp_dir,
            zip_file_path=app_zip,
            coverage_reports_stage="@example",
            coverage_reports_stage_path="test_db.public.example",
        )

    mock_wrapper.assert_not_called()
