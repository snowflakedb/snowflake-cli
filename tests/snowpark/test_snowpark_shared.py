import logging
from pathlib import Path
from requirements.requirement import Requirement
from unittest import mock
from unittest.mock import MagicMock, ANY
from zipfile import ZipFile

import pytest
import typer


import snowcli.cli.snowpark_shared as shared
import tests.snowpark.test_snowpark_shared
from snowcli.utils import SplitRequirements
from tests.testing_utils.fixtures import *


@mock.patch("tests.snowpark.test_snowpark_shared.shared.utils.parse_anaconda_packages")
def test_snowpark_package(mock_parse, temp_dir, correct_requirements_txt, caplog):

    mock_parse.return_value = SplitRequirements(
        [], [Requirement.parse("totally-awesome-package")]
    )
    with caplog.at_level(logging.INFO):
        result = shared.snowpark_package("yes", False, "yes")
    q = tests.snowpark.test_snowpark_shared.shared.utils.parse_anaconda_packages()
    assert caplog.text

    zip_path = os.path.join(temp_dir, "app.zip")
    assert os.path.isfile(zip_path)

    with ZipFile(zip_path) as zip:
        assert "requirements.other.txt" in zip.namelist()

        with zip.open("requirements.other.txt") as req_file:
            reqs = req_file.readlines()
            assert b"totally-awesome-package\n" in reqs


def test_snowpark_update_function_with_coverage_wrapper(caplog):

    with caplog.at_level(logging.ERROR):
        with pytest.raises(typer.Abort):
            shared.snowpark_update(
                type="function",
                environment="dev",
                name="hello",
                file=Path("app.zip"),
                handler="app.hello",
                input_parameters="()",
                return_type="str",
                replace=False,
                install_coverage_wrapper=True,
            )

    assert (
        "You cannot install a code coverage wrapper on a function, only a procedure."
        in caplog.text
    )


def test_replace_handler_in_zip(temp_dir, app_zip):
    result = shared.replace_handler_in_zip(
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


def test_replace_handler_in_zip_with_wrong_handler(caplog, temp_dir, app_zip):
    with caplog.at_level(logging.ERROR):
        with pytest.raises(typer.Abort):
            result = shared.replace_handler_in_zip(
                proc_name="hello",
                proc_signature="()",
                handler="app.hello.world",
                temp_dir=temp_dir,
                zip_file_path=app_zip,
                coverage_reports_stage="@example",
                coverage_reports_stage_path="test_db.public.example",
            )

    assert (
        "To install a code coverage wrapper, your handler must be in the format <module>.<function>"
        in caplog.text
    )
