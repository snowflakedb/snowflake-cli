import pytest

from tests.nativeapp.factories import (
    ProjectV2Factory,
    ApplicationPackageEntityModelFactory,
)


@pytest.mark.integration
@pytest.mark.parametrize("package_identifier", ["myapp_pkg", dict(name="myapp_pkg")])
def test_app_diff(temporary_directory, runner, package_identifier):
    ProjectV2Factory(
        pdf__entities=dict(
            pkg=ApplicationPackageEntityModelFactory(
                identifier=package_identifier,
                artifacts=[{"src": "*", "dest": "./"}],
            ),
        ),
        files={"setup.sql": ""},
    )
    result = runner.invoke_with_connection(["app", "deploy", "--no-validate"])
    assert result.exit_code == 0

    with open("README.md", "w") as f:
        f.write("Hello world!")

    result = runner.invoke_with_connection(["app", "diff"])
    assert result.exit_code == 0
    assert "README.md" in result.output
