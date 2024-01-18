from zipfile import ZipFile

from snowflake.cli.plugins.snowpark.zipper import add_file_to_existing_zip, zip_dir

from tests.testing_utils.fixtures import *


def test_zip_current_dir(temp_dir):
    zip_name = Path("zip_name.zip")
    files = [
        Path(".DS_Store"),
        Path(".git/config"),
        Path(".gitignore"),
        Path("bin/py.test"),
        Path("snowcli/snowcli.py"),
        Path("snowcli/snowcli.dist-info/METADATA.py"),
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
        Path("bin"),
        Path("snowcli"),
        Path("snowcli/snowcli.dist-info"),
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
        "bin/py.test",
        "snowcli/snowcli.py",
        "snowcli/snowcli.dist-info/METADATA.py",
        "utils/utils.py",
    }


def test_add_file_to_existing_zip(
    app_zip: str, correct_requirements_snowflake_txt: str
):
    add_file_to_existing_zip(app_zip, correct_requirements_snowflake_txt)
    zip_file = ZipFile(app_zip)

    assert os.path.basename(correct_requirements_snowflake_txt) in zip_file.namelist()
