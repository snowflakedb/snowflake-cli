from tests.nativeapp.factories import PdfV10Factory


def test_my_factory(temp_dir):
    pdf_dict = PdfV10Factory(temp_dir=temp_dir, native_app__name="myapp_booboo")
    assert pdf_dict["native_app"]["name"] == "myapp_booboo"


def test_my_factory_merge_def(temp_dir):
    pdf_dict = PdfV10Factory(
        temp_dir=temp_dir, merge_project_definition={"native_app": {"name": "myapp"}}
    )
    assert pdf_dict["native_app"]["name"] == "myapp"


def test_my_factory_minimal(temp_dir):
    pdf_dict = PdfV10Factory(
        temp_dir=temp_dir, merge_project_definition={"native_app": {"name": "myapp"}}
    )
    assert pdf_dict["definition_version"] == "1"
    assert pdf_dict["native_app"]["name"] == "myapp"
    assert pdf_dict["native_app"]["artifacts"] == []
    assert "package" not in pdf_dict["native_app"]
    assert "application" not in pdf_dict["native_app"]


# NOTE: when an object (dict) is passed in for package, the packagefactory is no longer used.
def test_my_factory_package_obj(temp_dir):
    pdf_dict = PdfV10Factory(
        temp_dir=temp_dir,
        merge_project_definition={"native_app": {"name": "myapp"}},
        native_app__artifacts=["README.md", "setup.sql"],
        native_app__package={"name": "myapp_pkg", "role": "package_role"},
    )
    assert pdf_dict["native_app"]["package"]["role"] == "package_role"
    assert pdf_dict["native_app"]["name"] == "myapp"
    assert pdf_dict["native_app"]["artifacts"] == ["README.md", "setup.sql"]


def test_my_factory_any_key(temp_dir):
    pdf_dict = PdfV10Factory(
        temp_dir=temp_dir,
        native_app__package__non_existent_key="some_value",
    )
    assert pdf_dict["native_app"]["package"]["non_existent_key"] == "some_value"
