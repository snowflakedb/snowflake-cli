from zipfile import ZipFile

from snowcli.cli.snowpark.commands import _replace_handler_in_zip
from snowcli.zipper import add_file_to_existing_zip, zip_dir

from tests.testing_utils.fixtures import *


def test_zip_current_dir(temp_dir):
    zip_name = Path("zip_name.zip")
    files = [
        Path(".DS_Store"),
        Path(".git/config"),
        Path(".gitignore"),
        Path(".packages/bin/py.test"),
        Path(".packages/snowcli/snowcli.py"),
        Path(".packages/snowcli/snowcli.dist-info/METADATA.py"),
        Path(".venv/bin"),
        Path("__pycache__"),
        Path("app.py"),
        Path("app.pyc"),
        Path("app.zip"),
        Path("additional_module.py"),
        Path("requirements.txt"),
        Path("requirements.snowflake.txt"),
        Path("requirements.other.txt"),
        Path("snowflake.yml"),
        Path("utils/__pycache__"),
        Path("utils/utils.py"),
        Path("utils/utils.pyc"),
        Path("utils/utils.zip"),
    ]
    directories = [
        Path(".git"),
        Path(".packages/bin"),
        Path(".packages/snowcli"),
        Path(".packages/snowcli/snowcli.dist-info"),
        Path(".venv"),
        Path("utils"),
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    for file in files:
        file.touch()

    zip_dir(source=Path(temp_dir), dest_zip=zip_name)

    zip_file = ZipFile(zip_name)
    assert set(zip_file.namelist()) == {
        "app.py",
        "additional_module.py",
        ".packages/bin/py.test",
        ".packages/snowcli/snowcli.py",
        ".packages/snowcli/snowcli.dist-info/METADATA.py",
        "utils/utils.py",
    }


def test_replace_handler_in_zip(temp_dir, app_zip):
    result = _replace_handler_in_zip(
        proc_name="hello",
        proc_signature="()",
        handler="app.hello",
        zip_file_path=str(app_zip),
        coverage_reports_stage="@example",
        coverage_reports_stage_path="test_db.public.example",
    )
    assert os.path.isfile(app_zip)
    assert result == "snowpark_coverage.measure_coverage"

    with ZipFile(str(app_zip), "r") as zip:
        assert "snowpark_coverage.py" in zip.namelist()
        with zip.open("snowpark_coverage.py") as coverage:
            coverage_file = coverage.readlines()
            assert b"        return app.hello(*args,**kwargs)\n" in coverage_file
            assert b"    import app\n" in coverage_file


def test_add_file_to_existing_zip(
    app_zip: str, correct_requirements_snowflake_txt: str
):
    add_file_to_existing_zip(app_zip, correct_requirements_snowflake_txt)
    zip_file = ZipFile(app_zip)

    assert os.path.basename(correct_requirements_snowflake_txt) in zip_file.namelist()
