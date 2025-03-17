from pathlib import Path

from tests.nativeapp.factories import PdfV10Factory, ProjectV10Factory
from tests.nativeapp.utils import use_integration_project


def test_pdf_factory(temporary_directory):
    pdf_res = PdfV10Factory(native_app__name="myapp_")
    assert pdf_res.yml["native_app"]["name"] == "myapp_"


def test_name_with_space(temporary_directory):
    pdf_res = PdfV10Factory(native_app__name="name with space")
    assert pdf_res.yml["native_app"]["name"] == "name with space"


def test_minimal(temporary_directory):
    pdf_res = PdfV10Factory(native_app__name="myapp")
    assert pdf_res.yml["definition_version"] == "1"
    assert pdf_res.yml["native_app"]["name"] == "myapp"
    assert pdf_res.yml["native_app"]["artifacts"] == []
    assert "package" not in pdf_res.yml["native_app"]
    assert "application" not in pdf_res.yml["native_app"]


# NOTE: when an object (dict) is passed in for package, the PackageFactory is no longer used.
def test_package_obj(temporary_directory):
    pdf_res = PdfV10Factory(
        native_app__name="myapp",
        native_app__artifacts=["README.md", "setup.sql"],
        native_app__package={"name": "myapp_pkg", "role": "package_role"},
    )
    assert pdf_res.yml["native_app"]["package"]["role"] == "package_role"
    assert pdf_res.yml["native_app"]["name"] == "myapp"
    assert pdf_res.yml["native_app"]["artifacts"] == ["README.md", "setup.sql"]


def test_any_key(temporary_directory):
    pdf_res = PdfV10Factory(
        native_app__package__non_existent_key="some_value",
    )
    assert pdf_res.yml["native_app"]["package"]["non_existent_key"] == "some_value"


def test_artifacts_str(temporary_directory):
    pdf_res = PdfV10Factory(
        native_app__artifacts=["some_value"],
    )
    assert pdf_res.yml["native_app"]["artifacts"] == ["some_value"]


def test_artifacts_mapping(temporary_directory):
    pdf_res = PdfV10Factory(
        native_app__artifacts=[{"src": "some_src", "dest": "some_dest"}],
    )
    assert pdf_res.yml["native_app"]["artifacts"] == [
        {"src": "some_src", "dest": "some_dest"}
    ]


def test_project_factory_create(temporary_directory):
    pdf_res = ProjectV10Factory(
        pdf__native_app__artifacts=["README.md", "setup.sql"],
        files={
            "README.md": "",
            "setup.sql": "select 1;",
            "app/some_file.py": "contents",
        },
    )
    assert pdf_res.pdf.yml["native_app"]["artifacts"] == ["README.md", "setup.sql"]
    assert (Path(temporary_directory) / "snowflake.yml").exists()
    assert (Path(temporary_directory) / "setup.sql").exists()
    assert (Path(temporary_directory) / "README.md").exists()
    assert (Path(temporary_directory) / "app/some_file.py").exists()


def test_pdf_and_local_yml(temporary_directory):
    ProjectV10Factory(
        pdf__native_app__artifacts=["README.md", "setup.sql"],
        files={
            "README.md": "",
            "setup.sql": "select 1;",
        },
    )
    PdfV10Factory.with_filename("snowflake.local.yml")(
        native_app__name="myapp",
    )

    assert (Path(temporary_directory) / "snowflake.yml").exists()
    assert (Path(temporary_directory) / "snowflake.local.yml").exists()


# PoC for sample project in test_data/projects/integration
def test_integration_factory(temporary_directory):
    use_integration_project()
    assert (Path(temporary_directory) / "snowflake.yml").exists()
    assert (Path(temporary_directory) / "package" / "001-shared.sql").exists()
    assert (Path(temporary_directory) / "package" / "002-shared.sql").exists()
    assert (Path(temporary_directory) / "app" / "setup.sql").exists()
    assert (Path(temporary_directory) / "app" / "README.md").exists()
    assert (Path(temporary_directory) / "app" / "manifest.yml").exists()
