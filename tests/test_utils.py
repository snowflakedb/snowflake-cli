from typing import Tuple
from unittest import mock
import json

from distutils.dir_util import copy_tree
from pathlib import Path, PosixPath
from requirements.requirement import Requirement
import typer
from unittest.mock import MagicMock, patch, mock_open
from zipfile import ZipFile

from snowcli import utils
from tests.testing_utils.fixtures import *

SUBDIR = "subdir"


@pytest.mark.parametrize("argument", utils.YesNoAskOptions)
def test_yes_no_ask_callback_with_correct_argument(argument: str):
    result = utils.yes_no_ask_callback(argument)

    assert result == argument


@pytest.mark.parametrize(
    "argument",
    ["Yes", "No", "Ask", "yse", 42, "and_now_for_something_completely_different"],
)
def test_yes_no_ask_callback_with_incorrect_argument(argument):
    with pytest.raises(typer.BadParameter) as e_info:
        utils.yes_no_ask_callback(argument)

    assert (
        e_info.value.message
        == f"Valid values: ['yes', 'no', 'ask']. You provided: {argument}"
    )


def test_prepare_app_zip(
    temp_dir,
    app_zip: str,
    temp_directory_for_app_zip: str,
):
    result = utils.prepare_app_zip(app_zip, temp_directory_for_app_zip)
    assert result == os.path.join(temp_directory_for_app_zip, Path(app_zip).name)


def test_prepare_app_zip_if_exception_is_raised_if_no_source(
    temp_directory_for_app_zip,
):
    with pytest.raises(FileNotFoundError) as expected_error:
        utils.prepare_app_zip("/non/existent/path", temp_directory_for_app_zip)

    assert expected_error.value.errno == 2
    assert expected_error.type == FileNotFoundError


def test_prepare_app_zip_if_exception_is_raised_if_no_dst(app_zip):
    with pytest.raises(FileNotFoundError) as expected_error:
        utils.prepare_app_zip(app_zip, "/non/existent/path")

    assert expected_error.value.errno == 2
    assert expected_error.type == FileNotFoundError


def test_parse_requierements_with_correct_file(
    correct_requirements_snowflake_txt: str, temp_dir
):
    result = utils.parse_requirements(correct_requirements_snowflake_txt)

    assert len(result) == len(test_data.requirements)


def test_parse_requirements_with_nonexistent_file(temp_dir):
    path = os.path.join(temp_dir, "non_existent.file")
    result = utils.parse_requirements(path)

    assert result == []


@patch("tests.test_utils.utils.requests")
def test_anaconda_packages(mock_requests):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = test_data.anaconda_response
    mock_requests.get.return_value = mock_response

    anaconda_packages = utils.parse_anaconda_packages(test_data.packages)
    assert (
        Requirement.parse_line("snowflake-connector-python")
        in anaconda_packages.snowflake
    )
    assert (
        Requirement.parse_line("my-totally-awesome-package") in anaconda_packages.other
    )


@patch("tests.test_utils.utils.requests")
def test_anaconda_packages_streamlit(mock_requests):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = test_data.anaconda_response
    mock_requests.get.return_value = mock_response

    test_data.packages.append(Requirement.parse_line("streamlit"))
    anaconda_packages = utils.parse_anaconda_packages(test_data.packages)

    assert Requirement.parse_line("streamlit") not in anaconda_packages.other


@patch("tests.test_utils.utils.requests")
def test_anaconda_packages_with_incorrect_response(mock_requests):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.json.return_value = {}
    mock_requests.get.return_value = mock_response

    with pytest.raises(typer.Abort):
        result = utils.parse_anaconda_packages(test_data.packages)


def test_generate_streamlit_environment_file_with_no_requirements(temp_dir):
    result = utils.generate_streamlit_environment_file(
        [],
    )
    assert result is None


def test_generate_streamlit_file(correct_requirements_snowflake_txt: str, temp_dir):
    result = utils.generate_streamlit_environment_file(
        [], correct_requirements_snowflake_txt
    )

    assert result == PosixPath("environment.yml")
    assert os.path.isfile(os.path.join(temp_dir, "environment.yml"))


def test_generate_streamlit_environment_file_with_excluded_dependencies(
    correct_requirements_snowflake_txt: str, temp_dir
):

    result = utils.generate_streamlit_environment_file(
        test_data.excluded_anaconda_deps, correct_requirements_snowflake_txt
    )

    env_file = os.path.join(temp_dir, "environment.yml")
    assert result == PosixPath("environment.yml")
    assert os.path.isfile(env_file)
    with open(env_file, "r") as f:
        for dep in test_data.excluded_anaconda_deps:
            assert dep not in f.read()


def test_generate_streamlit_package_wrapper():
    result = utils.generate_streamlit_package_wrapper(
        "example_stage", "example_module", False
    )

    assert result.exists()
    with open(result, "r") as f:
        assert 'importlib.reload(sys.modules["example_module"])' in f.read()
    os.remove(result)


def test_get_package_name_from_metadata_using_correct_data(
    correct_metadata_file: str, tmp_path
):
    result = utils.get_package_name_from_metadata(correct_metadata_file)
    assert result == Requirement.parse_line("my-awesome-package==0.0.1")


def test_generate_snowpark_coverage_wrapper(temp_dir):
    path = os.path.join(temp_dir, "coverage.py")
    utils.generate_snowpark_coverage_wrapper(
        target_file=path,
        proc_name="process",
        proc_signature="signature",
        handler_module="awesomeModule",
        handler_function="even_better_function",
        coverage_reports_stage="example_stage",
        coverage_reports_stage_path="nyan-cat.jpg",
    )

    assert os.path.isfile(path)
    with open(path) as coverage_file:
        assert (
            "return awesomeModule.even_better_function(*args,**kwargs)"
            in coverage_file.read()
        )


def test_add_file_to_existing_zip(
    app_zip: str, correct_requirements_snowflake_txt: str
):
    utils.add_file_to_existing_zip(app_zip, correct_requirements_snowflake_txt)
    zip_file = ZipFile(app_zip)

    assert os.path.basename(correct_requirements_snowflake_txt) in zip_file.namelist()


def test_recursive_zip_packages(
    temp_dir,
    txt_file_in_a_subdir: str,
    temp_file_in_other_directory: str,
):
    zip_file_path = os.path.join(temp_dir, "packed.zip")

    utils.recursive_zip_packages_dir(temp_dir, zip_file_path)

    zip_file = ZipFile(zip_file_path)

    assert os.path.isfile(zip_file_path)
    assert os.getenv("SNOWCLI_INCLUDE_PATHS") is None
    assert str(Path(txt_file_in_a_subdir).relative_to(temp_dir)) in zip_file.namelist()
    assert Path(temp_file_in_other_directory).name not in zip_file.namelist()
    assert zip_file_path not in zip_file.namelist()


def test_recursive_zip_packages_with_env_variable(
    temp_dir,
    txt_file_in_a_subdir: str,
    other_directory: str,
    temp_file_in_other_directory: str,
    include_paths_env_variable: str,
):
    zip_file_path = os.path.join(temp_dir, "packed.zip")

    utils.recursive_zip_packages_dir(temp_dir, zip_file_path)
    zip_file = ZipFile(zip_file_path)

    assert os.path.isfile(zip_file_path)
    assert str(Path(txt_file_in_a_subdir).relative_to(temp_dir)) in zip_file.namelist()
    assert str(Path(temp_file_in_other_directory).name) in zip_file.namelist()


def test_standard_zip_dir(temp_dir, txt_file_in_a_subdir: str):
    zip_file_path = os.path.join(temp_dir, "packed.zip")
    utils.standard_zip_dir(zip_file_path)
    zip_file = ZipFile(zip_file_path)

    assert os.path.isfile(zip_file_path)
    assert (
        os.path.join(SUBDIR, os.path.basename(txt_file_in_a_subdir))
        not in zip_file.namelist()
    )


def test_standard_zip_dir_with_env_variable(
    temp_dir,
    txt_file_in_a_subdir: str,
    include_paths_env_variable,
    other_directory: str,
    temp_file_in_other_directory: str,
):
    zip_file_path = os.path.join(temp_dir, "packed.zip")
    utils.standard_zip_dir(zip_file_path)
    zip_file = ZipFile(zip_file_path)

    assert os.path.isfile(zip_file_path)
    assert (
        os.path.join("subdir", os.path.basename(txt_file_in_a_subdir))
        not in zip_file.namelist()
    )
    assert Path(temp_file_in_other_directory).name in zip_file.namelist()


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
        result = utils.get_snowflake_packages()
    mock_file.assert_called_with("requirements.snowflake.txt", encoding="utf-8")
    assert result == expected


def test_get_snowflake_packages_delta(temp_dir, correct_requirements_snowflake_txt):
    anaconda_package = test_data.requirements[-1]

    result = utils.get_snowflake_packages_delta(anaconda_package)

    assert result == test_data.requirements[:-1]


def test_convert_resource_details_to_dict():
    resource_details = [
        ("packages", "{'name': 'my-awesome-package','version': '1.2.3'}"),
        ("handler", "handler_function"),
    ]

    assert utils.convert_resource_details_to_dict(resource_details) == {
        "packages": {"name": "my-awesome-package", "version": "1.2.3"},
        "handler": "handler_function",
    }


def test_parse_requirements(correct_requirements_txt: str):
    result = utils.parse_requirements(correct_requirements_txt)

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
    ]
    split_requirements = utils.parse_anaconda_packages(packages=packages)
    assert len(split_requirements.snowflake) == 1
    assert len(split_requirements.other) == 1
    assert split_requirements.snowflake[0].name == "pandas"
    assert split_requirements.snowflake[0].specifier is True
    assert split_requirements.snowflake[0].specs == [("==", "1.0.0")]
    assert split_requirements.other[0].name == "FuelSDK"
    assert split_requirements.other[0].specifier is True
    assert split_requirements.other[0].specs == [(">=", "0.9.3")]


def test_get_downloaded_packages(test_root_path, temp_dir):
    # In this test, we parse some real package metadata downloaded by pip
    # only the dist-info directories are available, we don't need the actual files
    copy_tree(
        os.path.join(test_root_path, "test_data", "local_packages"),
        temp_dir,
    )
    requirements_with_files = utils.get_downloaded_packages()
    assert len(requirements_with_files) == 2
    assert "httplib2" in requirements_with_files
    httplib_req = requirements_with_files["httplib2"]
    assert httplib_req.requirement.name == "httplib2"
    assert httplib_req.requirement.specifier is True
    assert httplib_req.requirement.specs == [("==", "0.22.0")]
    # there are 19 files listed in the RECORD file, but we only get the
    # first part of the path. All 19 files fall under these two directories
    assert sorted(httplib_req.files) == ["httplib2", "httplib2-0.22.0.dist-info"]
    assert "Zendesk" in requirements_with_files
    zendesk_req = requirements_with_files["Zendesk"]
    assert zendesk_req.requirement.name == "Zendesk"
    assert zendesk_req.requirement.specifier is True
    assert zendesk_req.requirement.specs == [("==", "1.1.1")]
    assert sorted(zendesk_req.files) == ["Zendesk-1.1.1.dist-info", "zendesk"]


def test_deduplicate_and_sort_reqs():
    packages = [
        Requirement.parse("d"),
        Requirement.parse("b==0.9.3"),
        Requirement.parse("a==0.9.5"),
        Requirement.parse("a==0.9.3"),
        Requirement.parse("c>=0.9.5"),
    ]
    sorted_packages = utils.deduplicate_and_sort_reqs(packages)
    assert len(sorted_packages) == 4
    assert sorted_packages[0].name == "a"
    assert sorted_packages[0].specifier is True
    assert sorted_packages[0].specs == [("==", "0.9.5")]


@pytest.mark.parametrize(
    "argument",
    [
        ("NUMBER(38,0)", "int"),
        ("TIMESTAMP_NTZ(9)", "datetime"),
        ("TIMESTAMP_TZ(9)", "datetime"),
        ("VARCHAR(16777216)", "string"),
        ("FLOAT", "float"),
        ("ARRAY", "array"),
    ],
)
def test_sql_to_python_return_type_mapper(argument: Tuple[str, str]):
    assert utils.sql_to_python_return_type_mapper(argument[0]) == argument[1]


@mock.patch("platform.system")
@pytest.mark.parametrize(
    "argument, expected",
    [
        ("C:\Something\Something Else", "C:\Something\Something Else"),
        (
            "/var/folders/k8/3sdqh3nn4gg7lpr5fz0fjlqw0000gn/T/tmpja15jymq",
            "/var/folders/k8/3sdqh3nn4gg7lpr5fz0fjlqw0000gn/T/tmpja15jymq",
        ),
    ],
)
def test_path_resolver(mock_system, argument, expected):
    mock_system.response_value = "Windows"

    assert utils.path_resolver(argument) == expected
