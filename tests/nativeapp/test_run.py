# Note: run.py and hence test_run.py is WIP with work spread over three tickets.
# Therefore more tests will be added as need arises.

from snowcli.cli.nativeapp.run import get_required_field_from_definition
from tests.testing_utils.fixtures import *
from strictyaml import as_document

mock_project_definition = {
    "name": "sample_project_name",
    "package": {"name": "sample_package_name", "role": "sample_package_role"},
}

mock_project_definition_override = {
    "native_app": {
        "name": "sample_project_name",
        "application": {
            "name": "sample_application_name",
            "role": "sample_application_role",
        },
    }
}


@pytest.mark.parametrize(
    "index_a, index_b, expected",
    [
        ("package", "name", "sample_package_name"),
        ("application", "role", "sample_application_role"),
    ],
)
def test_get_required_field_from_definition(index_a, index_b, expected):
    assert (
        get_required_field_from_definition(
            index_a,
            index_b,
            mock_project_definition,
            as_document(mock_project_definition_override),
        )
        == expected
    )
