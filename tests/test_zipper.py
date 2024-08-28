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

import os
from pathlib import Path
from zipfile import ZipFile

from snowflake.cli._plugins.snowpark.zipper import add_file_to_existing_zip, zip_dir


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
        Path("snowcli/.git"),
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
        "bin/",
        "bin/py.test",
        "snowcli/",
        "snowcli/snowcli.py",
        "snowcli/snowcli.dist-info/",
        "snowcli/snowcli.dist-info/METADATA.py",
        "utils/",
        "utils/utils.py",
    }


def test_add_file_to_existing_zip(
    app_zip: str, correct_requirements_snowflake_txt: str
):
    add_file_to_existing_zip(app_zip, correct_requirements_snowflake_txt)
    zip_file = ZipFile(app_zip)

    assert os.path.basename(correct_requirements_snowflake_txt) in zip_file.namelist()
