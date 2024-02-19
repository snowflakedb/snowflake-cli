import json
import logging
from distutils.dir_util import copy_tree
from pathlib import PosixPath
from unittest.mock import MagicMock, mock_open, patch

import snowflake.cli.plugins.snowpark.models
import snowflake.cli.plugins.snowpark.package.utils
import typer
from snowflake.cli.plugins.snowpark.models import Requirement
from snowflake.cli.api.utils import path_utils
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.models import PypiOption
from snowflake.cli.plugins.streamlit import streamlit_utils

from tests.testing_utils.fixtures import *


def test_prepare_app_zip(
    temp_dir,
    app_zip: str,
    temp_directory_for_app_zip: str,
):
    result = snowflake.cli.plugins.snowpark.package.utils.prepare_app_zip(
        Path(app_zip), temp_directory_for_app_zip
    )
    assert result == os.path.join(temp_directory_for_app_zip, Path(app_zip).name)


def test_prepare_app_zip_if_exception_is_raised_if_no_source(
    temp_directory_for_app_zip,
):
    with pytest.raises(FileNotFoundError) as expected_error:
        snowflake.cli.plugins.snowpark.package.utils.prepare_app_zip(
            Path("/non/existent/path"), temp_directory_for_app_zip
        )

    assert expected_error.value.errno == 2
    assert expected_error.type == FileNotFoundError


def test_prepare_app_zip_if_exception_is_raised_if_no_dst(app_zip):
    with pytest.raises(FileNotFoundError) as expected_error:
        snowflake.cli.plugins.snowpark.package.utils.prepare_app_zip(
            Path(app_zip), "/non/existent/path"
        )

    assert expected_error.value.errno == 2
    assert expected_error.type == FileNotFoundError


def test_parse_requierements_with_correct_file(
    correct_requirements_snowflake_txt: str, temp_dir
):
    result = package_utils.parse_requirements(correct_requirements_snowflake_txt)

    assert len(result) == len(test_data.requirements)


def test_parse_requirements_with_nonexistent_file(temp_dir):
    path = os.path.join(temp_dir, "non_existent.file")
    result = package_utils.parse_requirements(path)

    assert result == []


@patch("snowflake.cli.plugins.snowpark.package_utils.requests")
def test_anaconda_packages(mock_requests):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = test_data.anaconda_response
    mock_requests.get.return_value = mock_response

    anaconda_packages = package_utils.parse_anaconda_packages(test_data.packages)
    assert (
        Requirement.parse_line("snowflake-connector-python")
        in anaconda_packages.snowflake
    )
    assert (
        Requirement.parse_line("my-totally-awesome-package") in anaconda_packages.other
    )


@patch("snowflake.cli.plugins.snowpark.package_utils.requests")
def test_anaconda_packages_streamlit(mock_requests):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = test_data.anaconda_response
    mock_requests.get.return_value = mock_response

    test_data.packages.append(Requirement.parse_line("streamlit"))
    anaconda_packages = package_utils.parse_anaconda_packages(test_data.packages)

    assert Requirement.parse_line("streamlit") not in anaconda_packages.other


@patch("snowflake.cli.plugins.snowpark.package_utils.requests")
def test_anaconda_packages_with_incorrect_response(mock_requests):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.json.return_value = {}
    mock_requests.get.return_value = mock_response

    with pytest.raises(typer.Abort):
        result = package_utils.parse_anaconda_packages(test_data.packages)


def test_generate_streamlit_environment_file_with_no_requirements(temp_dir):
    result = streamlit_utils.generate_streamlit_environment_file(
        [],
    )
    assert result is None


def test_generate_streamlit_file(correct_requirements_snowflake_txt: str, temp_dir):
    result = streamlit_utils.generate_streamlit_environment_file(
        [], correct_requirements_snowflake_txt
    )

    assert result == PosixPath("environment.yml")
    assert os.path.isfile(os.path.join(temp_dir, "environment.yml"))


def test_generate_streamlit_environment_file_with_excluded_dependencies(
    correct_requirements_snowflake_txt: str, temp_dir
):
    result = streamlit_utils.generate_streamlit_environment_file(
        test_data.excluded_anaconda_deps, correct_requirements_snowflake_txt
    )

    env_file = os.path.join(temp_dir, "environment.yml")
    assert result == PosixPath("environment.yml")
    assert os.path.isfile(env_file)
    with open(env_file, "r") as f:
        for dep in test_data.excluded_anaconda_deps:
            assert dep not in f.read()


def test_generate_streamlit_package_wrapper():
    result = streamlit_utils.generate_streamlit_package_wrapper(
        "example_stage", "example_module", False
    )

    assert result.exists()
    with open(result, "r") as f:
        assert 'importlib.reload(sys.modules["example_module"])' in f.read()
    os.remove(result)


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
    with patch("builtins.open", mock_open(read_data=contents)) as mock_file:
        mock_file.return_value.__iter__.return_value = contents.splitlines()
        result = package_utils.get_snowflake_packages()
    mock_file.assert_called_with("requirements.snowflake.txt", encoding="utf-8")
    assert result == expected


def test_parse_requirements(correct_requirements_txt: str):
    result = package_utils.parse_requirements(correct_requirements_txt)

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


@mock.patch("requests.get")
def test_parse_anaconda_packages(mock_get):
    mock_response = mock.Mock()
    mock_response.status_code = 200
    # load the contents of the local json file under test_data/anaconda_channel_data.json
    mock_response.json.return_value = json.loads(
        Path(
            os.path.join(Path(__file__).parent, "test_data/anaconda_channel_data.json")
        ).read_text(encoding="utf-8")
    )
    mock_get.return_value = mock_response

    packages = [
        Requirement.parse("pandas==1.0.0"),
        Requirement.parse("FuelSDK>=0.9.3"),
        Requirement.parse("Pamela==1.0.1"),
    ]
    split_requirements = package_utils.parse_anaconda_packages(packages=packages)
    assert len(split_requirements.snowflake) == 1
    assert len(split_requirements.other) == 2
    assert split_requirements.snowflake[0].name == "pandas"
    assert split_requirements.snowflake[0].specifier is True
    assert split_requirements.snowflake[0].specs == [("==", "1.0.0")]
    assert split_requirements.other[0].name == "FuelSDK"
    assert split_requirements.other[0].specifier is True
    assert split_requirements.other[0].specs == [(">=", "0.9.3")]
    assert split_requirements.other[1].name == "Pamela"
    assert split_requirements.other[1].specs == [("==", "1.0.1")]


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
    mock_installer.return_value.__enter__.return_value.pip_install.return_value = 42

    with caplog.at_level(logging.INFO, "snowflake.cli.plugins.snowpark.package_utils"):
        result = package_utils.install_packages(
            correct_requirements_txt, True, PypiOption.YES
        )

    assert "pip failed with return code 42" in caplog.text


@pytest.mark.parametrize(
    "argument, expected",
    [
        (Requirement.parse_line("anaconda-clean"), True),
        (Requirement.parse_line("anaconda-clean==1.1.1"), True),
        (Requirement.parse_line("anaconda-clean==1.1.0"), True),
        (Requirement.parse_line("anaconda-clean==1.1.2"), False),
        (Requirement.parse_line("anaconda-clean>=1.1.1"), True),
        (Requirement.parse_line("some-other-package"), False),
    ],
)
def test_check_if_package_is_avaiable_in_conda(argument, expected):
    assert (
        package_utils.check_if_package_is_avaiable_in_conda(
            argument, test_data.anaconda_response["packages"]
        )
        == expected
    )


@pytest.mark.parametrize(
    "name,expected",
    [
        ("package", "package"),
        ("package.zip", "package"),
        ("git+https://github.com/Snowflake-Labs/snowflake-cli/", "snowflake-cli"),
        (
            "git+https://github.com/Snowflake-Labs/snowflake-cli.git/@snow-123456-fix",
            "snowflake-cli",
        ),
    ],
)
def test_get_package_name(name: str, expected: str):
    assert snowflake.cli.plugins.snowpark.models.get_package_name(name) == expected
