from pathlib import Path
from unittest import mock
import tempfile
import json
import os
from requirements.requirement import Requirement
from snowcli import utils


def test_parse_requirements():
    with tempfile.NamedTemporaryFile() as tmp:
        # write a requirements.txt file
        with open(tmp.name, "w", encoding="utf-8") as f:
            f.write("pandas==1.0.0\nFuelSDK>=0.9.3")
        result = utils.parse_requirements(tmp.name)

    assert len(result) == 2
    assert result[0].name == "FuelSDK"
    assert result[0].specifier is True
    assert result[0].specs == [(">=", "0.9.3")]
    assert result[1].name == "pandas"
    assert result[1].specifier is True
    assert result[1].specs == [("==", "1.0.0")]


# mock the next call to requests.get
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

    packages = [Requirement.parse("pandas==1.0.0"), Requirement.parse("FuelSDK>=0.9.3")]
    split_requirements = utils.parse_anaconda_packages(packages=packages)
    assert len(split_requirements.snowflake) == 1
    assert len(split_requirements.other) == 1
    assert split_requirements.snowflake[0].name == "pandas"
    assert split_requirements.snowflake[0].specifier is True
    assert split_requirements.snowflake[0].specs == [("==", "1.0.0")]
    assert split_requirements.other[0].name == "FuelSDK"
    assert split_requirements.other[0].specifier is True
    assert split_requirements.other[0].specs == [(">=", "0.9.3")]


def test_get_downloaded_packages():
    # In this test, we parse some real package metadata downloaded by pip
    # only the dist-info directories are available, we don't need the actual files
    os.chdir(Path(os.path.join(Path(__file__).parent, "test_data", "local_packages")))
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


def test_generate_streamlit_environment_file():
    test_requirements = Path(
        os.path.join(
            Path(__file__).parent, "test_data", "test_streamlit_requirements.txt"
        )
    )
    environment_file = utils.generate_streamlit_environment_file(
        excluded_anaconda_deps=["pandas"], requirements_file=test_requirements
    )
    # read in the generated environment.yml file
    assert environment_file is not None
    file_result = environment_file.read_text(encoding="utf-8")
    # delete file_result
    environment_file.unlink()
    # pandas is excluded explicitly, snowflake-connector-python is excluded automatically
    assert (
        file_result
        == """name: sf_env
channels:
- snowflake
dependencies:
- pydantic"""
    )


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
