import logging
import os
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest
import snowflake.cli.plugins.snowpark.models
import snowflake.cli.plugins.snowpark.package.utils
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils import path_utils
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.models import Requirement, YesNoAsk
from snowflake.cli.plugins.snowpark.package.anaconda import AnacondaChannel

from tests.test_data import test_data


def test_prepare_app_zip(
    temp_dir,
    app_zip: str,
    temp_directory_for_app_zip: str,
):
    result = snowflake.cli.plugins.snowpark.package.utils.prepare_app_zip(
        SecurePath(app_zip), SecurePath(temp_directory_for_app_zip)
    )
    assert str(result.path) == os.path.join(
        temp_directory_for_app_zip, Path(app_zip).name
    )


def test_prepare_app_zip_if_exception_is_raised_if_no_source(
    temp_directory_for_app_zip,
):
    with pytest.raises(FileNotFoundError) as expected_error:
        snowflake.cli.plugins.snowpark.package.utils.prepare_app_zip(
            SecurePath("/non/existent/path"), SecurePath(temp_directory_for_app_zip)
        )

    assert expected_error.value.errno == 2
    assert expected_error.type == FileNotFoundError


def test_prepare_app_zip_if_exception_is_raised_if_no_dst(app_zip):
    with pytest.raises(FileNotFoundError) as expected_error:
        snowflake.cli.plugins.snowpark.package.utils.prepare_app_zip(
            SecurePath(app_zip), SecurePath("/non/existent/path")
        )

    assert expected_error.value.errno == 2
    assert expected_error.type == FileNotFoundError


def test_parse_requirements_with_correct_file(
    correct_requirements_snowflake_txt: str, temp_dir
):
    result = package_utils.parse_requirements(
        SecurePath(correct_requirements_snowflake_txt)
    )

    assert len(result) == len(test_data.requirements)


def test_parse_requirements_with_nonexistent_file(temp_dir):
    path = os.path.join(temp_dir, "non_existent.file")
    result = package_utils.parse_requirements(SecurePath(path))

    assert result == []


@pytest.mark.parametrize(
    "contents, expected",
    [
        (
            """pytest==1.0.0\nDjango==3.2.1\nawesome_lib==3.3.3""",
            ["pytest==1.0.0", "Django==3.2.1", "awesome_lib==3.3.3"],
        ),
        ("""toml # some-comment""", ["toml"]),
        ("", []),
        ("""some-package==1.2.3#incorrect_comment""", ["some-package==1.2.3"]),
        ("""#only comment here""", []),
        (
            """pytest==1.0\n# comment\nawesome_lib==3.3.3""",
            ["pytest==1.0", "awesome_lib==3.3.3"],
        ),
    ],
)
def test_get_packages(contents, expected, correct_requirements_snowflake_txt):
    with patch.object(
        snowflake.cli.api.secure_path.SecurePath, "open", mock_open(read_data=contents)
    ) as mock_file:
        mock_file.return_value.__iter__.return_value = contents.splitlines()
        result = package_utils.get_snowflake_packages()
    mock_file.assert_called_with("r", read_file_limit_mb=128, encoding="utf-8")
    assert result == expected


def test_parse_requirements(correct_requirements_txt: str):
    result = package_utils.parse_requirements(SecurePath(correct_requirements_txt))

    assert len(result) == 3
    assert result[0].name == "Django"
    assert result[0].specifier is True
    assert result[0].specs == [("==", "3.2.1")]
    assert result[1].name == "awesome_lib"
    assert result[1].specifier is True
    assert result[1].specs == [("==", "3.3.3")]
    assert result[2].name == "pytest"
    assert result[2].specifier is True
    assert result[2].specs == [("==", "1.0.0")]


def test_deduplicate_and_sort_reqs():
    packages = [
        Requirement.parse("d"),
        Requirement.parse("b==0.9.3"),
        Requirement.parse("a==0.9.5"),
        Requirement.parse("a==0.9.3"),
        Requirement.parse("c>=0.9.5"),
    ]
    sorted_packages = package_utils.deduplicate_and_sort_reqs(packages)
    assert len(sorted_packages) == 4
    assert sorted_packages[0].name == "a"
    assert sorted_packages[0].specifier is True
    assert sorted_packages[0].specs == [("==", "0.9.5")]


@patch("platform.system")
@pytest.mark.parametrize(
    "argument, expected",
    [
        ("C:\\Something\\Something Else", "C:\\Something\\Something Else"),
        (
            "/var/folders/k8/3sdqh3nn4gg7lpr5fz0fjlqw0000gn/T/tmpja15jymq",
            "/var/folders/k8/3sdqh3nn4gg7lpr5fz0fjlqw0000gn/T/tmpja15jymq",
        ),
    ],
)
def test_path_resolver(mock_system, argument, expected):
    mock_system.response_value = "Windows"

    assert path_utils.path_resolver(argument) == expected


@patch("snowflake.cli.plugins.snowpark.package_utils.Venv")
def test_pip_fail_message(mock_installer, correct_requirements_txt, caplog):
    mock_installer.return_value.__enter__.return_value.pip_wheel.return_value = 42

    with caplog.at_level(logging.INFO, "snowflake.cli.plugins.snowpark.package_utils"):
        package_utils.download_packages(
            anaconda=AnacondaChannel({}),
            requirements_file=SecurePath(correct_requirements_txt),
            packages_dir=SecurePath(".packages"),
            ignore_anaconda=False,
            allow_shared_libraries=YesNoAsk.YES,
        )

    assert "pip failed with return code 42" in caplog.text
