from tests.nativeapp.factories import PdfV10Factory


def test_factory(temp_dir):
    pdf_dict, _ = PdfV10Factory(native_app__name="myapp_")
    assert pdf_dict["native_app"]["name"] == "myapp_"


def test_name_with_space(temp_dir):
    pdf_dict, _ = PdfV10Factory(native_app__name="name with space")
    assert pdf_dict["native_app"]["name"] == "name with space"


def test_merge_def(temp_dir):
    pdf_dict, _ = PdfV10Factory(
        merge_project_definition={"native_app": {"name": "myapp"}}
    )
    assert pdf_dict["native_app"]["name"] == "myapp"


def test_minimal(temp_dir):
    pdf_dict, _ = PdfV10Factory(
        merge_project_definition={"native_app": {"name": "myapp"}}
    )
    assert pdf_dict["definition_version"] == "1"
    assert pdf_dict["native_app"]["name"] == "myapp"
    assert pdf_dict["native_app"]["artifacts"] == []
    assert "package" not in pdf_dict["native_app"]
    assert "application" not in pdf_dict["native_app"]


# NOTE: when an object (dict) is passed in for package, the PackageFactory is no longer used.
def test_package_obj(temp_dir):
    pdf_dict, _ = PdfV10Factory(
        merge_project_definition={"native_app": {"name": "myapp"}},
        native_app__artifacts=["README.md", "setup.sql"],
        native_app__package={"name": "myapp_pkg", "role": "package_role"},
    )
    assert pdf_dict["native_app"]["package"]["role"] == "package_role"
    assert pdf_dict["native_app"]["name"] == "myapp"
    assert pdf_dict["native_app"]["artifacts"] == ["README.md", "setup.sql"]


def test_any_key(temp_dir):
    pdf_dict, _ = PdfV10Factory(
        native_app__package__non_existent_key="some_value",
    )
    assert pdf_dict["native_app"]["package"]["non_existent_key"] == "some_value"


# TODO: actually test no file written
def test_no_write(temp_dir):
    pdf_dict, _ = PdfV10Factory(
        skip_write=True,
    )
    assert 1 == 1


# TODO add test returned string


def test_artifacts_str(temp_dir):
    pdf_dict, _ = PdfV10Factory(
        native_app__artifacts=["some_value"],
    )
    assert pdf_dict["native_app"]["artifacts"] == ["some_value"]


def test_artifacts_mapping(temp_dir):
    pdf_dict, _ = PdfV10Factory(
        native_app__artifacts=[{"src": "some_src", "dest": "some_dest"}],
    )
    assert pdf_dict["native_app"]["artifacts"] == [
        {"src": "some_src", "dest": "some_dest"}
    ]
