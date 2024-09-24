from pathlib import Path

from tests.nativeapp.factories import PdfV11Factory, ProjectV11Factory


def test_factory(temp_dir):
    pdf_res = PdfV11Factory(native_app__name="myapp_")
    assert pdf_res.yml["native_app"]["name"] == "myapp_"


def test_name_with_space(temp_dir):
    pdf_res = PdfV11Factory(native_app__name="name with space")
    assert pdf_res.yml["native_app"]["name"] == "name with space"


def test_minimal(temp_dir):
    pdf_res = PdfV11Factory(native_app__name="myapp")
    assert pdf_res.yml["definition_version"] == "1.1"
    assert pdf_res.yml["native_app"]["name"] == "myapp"
    assert pdf_res.yml["native_app"]["artifacts"] == []
    assert "package" not in pdf_res.yml["native_app"]
    assert "application" not in pdf_res.yml["native_app"]


# NOTE: when an object (dict) is passed in for package, the PackageFactory is no longer used.
def test_package_obj(temp_dir):
    pdf_res = PdfV11Factory(
        native_app__name="myapp",
        native_app__artifacts=["README.md", "setup.sql"],
        native_app__package={"name": "myapp_pkg", "role": "package_role"},
    )
    assert pdf_res.yml["native_app"]["package"]["role"] == "package_role"
    assert pdf_res.yml["native_app"]["name"] == "myapp"
    assert pdf_res.yml["native_app"]["artifacts"] == ["README.md", "setup.sql"]


def test_any_key(temp_dir):
    pdf_res = PdfV11Factory(
        native_app__package__non_existent_key="some_value",
    )
    assert pdf_res.yml["native_app"]["package"]["non_existent_key"] == "some_value"


def test_artifacts_str(temp_dir):
    pdf_res = PdfV11Factory(
        native_app__artifacts=["some_value"],
    )
    assert pdf_res.yml["native_app"]["artifacts"] == ["some_value"]


def test_artifacts_mapping(temp_dir):
    pdf_res = PdfV11Factory(
        native_app__artifacts=[{"src": "some_src", "dest": "some_dest"}],
    )
    assert pdf_res.yml["native_app"]["artifacts"] == [
        {"src": "some_src", "dest": "some_dest"}
    ]


def test_project_factory(temp_dir):
    pdf_res = ProjectV11Factory(
        pdf__native_app__artifacts=["README.md", "setup.sql"],
        files=[
            {"filename": "README.md", "contents": ""},
            {"filename": "setup.sql", "contents": "select 1;"},
            {"filename": "app/some_file.py", "contents": ""},
        ],
    )
    assert pdf_res.pdf.yml["native_app"]["artifacts"] == ["README.md", "setup.sql"]
    assert Path(Path(temp_dir) / "snowflake.yml").exists()
    assert Path(Path(temp_dir) / "setup.sql").exists()
    assert Path(Path(temp_dir) / "README.md").exists()
    assert Path(Path(temp_dir) / "app/some_file.py").exists()
