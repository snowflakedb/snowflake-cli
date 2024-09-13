from tests.nativeapp.factories import DefinitionV10Factory


def test_my_factory(temp_dir):
    pdfv10 = DefinitionV10Factory(temp_dir=temp_dir, native_app__name="myapp")
    assert pdfv10.native_app.name == "myapp"


def test_my_factory_minimal(temp_dir):
    pdfv10 = DefinitionV10Factory(
        temp_dir=temp_dir,
        merge_project_definition={"native_app": {"name": "myapp"}},
        native_app__artifacts=["README.md", "setup.sql"],
    )
    assert pdfv10.native_app.name == "myapp"
    assert pdfv10.native_app.artifacts.length == 2


def test_my_factory_merge_def(temp_dir):
    pdfv10 = DefinitionV10Factory(
        temp_dir=temp_dir, merge_project_definition={"native_app": {"name": "myapp"}}
    )
    assert pdfv10.native_app.name == "myapp"
