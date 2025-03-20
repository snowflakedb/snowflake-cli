from pathlib import Path

from snowflake.cli.api.project.definition_manager import DefinitionManager

from tests.nativeapp.factories import PdfV11Factory, ProjectV11Factory


def test_factory(temporary_directory):
    pdf_res = PdfV11Factory(native_app__name="myapp_")
    assert pdf_res.yml["native_app"]["name"] == "myapp_"


def test_name_with_space(temporary_directory):
    pdf_res = PdfV11Factory(native_app__name="name with space")
    assert pdf_res.yml["native_app"]["name"] == "name with space"


def test_minimal(temporary_directory):
    pdf_res = PdfV11Factory(native_app__name="myapp")
    assert pdf_res.yml["definition_version"] == "1.1"
    assert pdf_res.yml["native_app"]["name"] == "myapp"
    assert pdf_res.yml["native_app"]["artifacts"] == []
    assert "package" not in pdf_res.yml["native_app"]
    assert "application" not in pdf_res.yml["native_app"]


# NOTE: when an object (dict) is passed in for package, the PackageFactory is no longer used.
def test_package_obj(temporary_directory):
    pdf_res = PdfV11Factory(
        native_app__name="myapp",
        native_app__artifacts=["README.md", "setup.sql"],
        native_app__package={"name": "myapp_pkg", "role": "package_role"},
    )
    assert pdf_res.yml["native_app"]["package"]["role"] == "package_role"
    assert pdf_res.yml["native_app"]["name"] == "myapp"
    assert pdf_res.yml["native_app"]["artifacts"] == ["README.md", "setup.sql"]


def test_any_key(temporary_directory):
    pdf_res = PdfV11Factory(
        native_app__package__non_existent_key="some_value",
    )
    assert pdf_res.yml["native_app"]["package"]["non_existent_key"] == "some_value"


def test_artifacts_str(temporary_directory):
    pdf_res = PdfV11Factory(
        native_app__artifacts=["some_value"],
    )
    assert pdf_res.yml["native_app"]["artifacts"] == ["some_value"]


def test_artifacts_mapping(temporary_directory):
    pdf_res = PdfV11Factory(
        native_app__artifacts=[{"src": "some_src", "dest": "some_dest"}],
    )
    assert pdf_res.yml["native_app"]["artifacts"] == [
        {"src": "some_src", "dest": "some_dest"}
    ]


def test_project_factory(temporary_directory):
    pdf_res = ProjectV11Factory(
        pdf__native_app__artifacts=["README.md", "setup.sql"],
        files={
            "README.md": "",
            "setup.sql": "select 1;",
            "app/some_file.py": "",
        },
    )
    assert pdf_res.pdf.yml["native_app"]["artifacts"] == ["README.md", "setup.sql"]
    assert (Path(temporary_directory) / "snowflake.yml").exists()
    assert (Path(temporary_directory) / "setup.sql").exists()
    assert (Path(temporary_directory) / "README.md").exists()
    assert (Path(temporary_directory) / "app/some_file.py").exists()


def test_templates(temporary_directory):
    PdfV11Factory(
        native_app__name="myapp_<% ctx.env.FOO %>",
        env__FOO="bar",
    )
    assert (Path(temporary_directory) / "snowflake.yml").exists()
    dm = DefinitionManager(temporary_directory)
    assert dm.project_definition.native_app.name == "myapp_bar"
    assert dm.project_definition.env["FOO"] == "bar"
