from pathlib import Path
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.snowpark.package.anaconda_packages import (
    AnacondaPackages,
    AvailablePackage,
)
from snowflake.cli._plugins.snowpark.snowpark_entity import (
    CreateMode,
    FunctionEntity,
    ProcedureEntity,
)
from snowflake.cli._plugins.snowpark.snowpark_entity_model import FunctionEntityModel
from snowflake.cli._plugins.workspace.context import ActionContext, WorkspaceContext

from tests.testing_utils.mock_config import mock_config_key

CONNECTOR = "snowflake.connector.connect"
CONTEXT = ""
EXECUTE_QUERY = "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"
ANACONDA_PACKAGES = "snowflake.cli._plugins.snowpark.package.anaconda_packages.AnacondaPackagesManager.find_packages_available_in_snowflake_anaconda"


@pytest.fixture
def example_function_workspace(
    project_directory,
):  # TODO: try to make a common fixture for all entities
    with mock_config_key("enable_native_app_children", True):
        with project_directory("snowpark_functions_v2") as pdir:
            with Path(pdir / "snowflake.yml").open() as definition_file:
                definition = yaml.safe_load(definition_file)
                model = FunctionEntityModel(
                    **definition.get("entities", {}).get("func1")
                )

                workspace_context = WorkspaceContext(
                    console=mock.MagicMock(),
                    project_root=pdir,
                    get_default_role=lambda: "test_role",
                    get_default_warehouse=lambda: "test_warehouse",
                )

                return (
                    FunctionEntity(workspace_ctx=workspace_context, entity_model=model),
                    ActionContext(
                        get_entity=lambda *args: None,
                    ),
                )


def test_cannot_instantiate_without_feature_flag():
    with pytest.raises(NotImplementedError) as err:
        FunctionEntity()
    assert str(err.value) == "Snowpark entity is not implemented yet"

    with pytest.raises(NotImplementedError) as err:
        ProcedureEntity()
    assert str(err.value) == "Snowpark entity is not implemented yet"


@mock.patch(ANACONDA_PACKAGES)
def test_nativeapp_children_interface_old_build(
    mock_anaconda, example_function_workspace, snapshot
):
    mock_anaconda.return_value = AnacondaPackages(
        {
            "pandas": AvailablePackage("pandas", "1.2.3"),
            "numpy": AvailablePackage("numpy", "1.2.3"),
            "snowflake_snowpark_python": AvailablePackage(
                "snowflake_snowpark_python", "1.2.3"
            ),
        }
    )

    sl, action_context = example_function_workspace

    sl.bundle(None, False, False, None, False)

    deploy_sql_str = sl.get_deploy_sql(CreateMode.create)
    grant_sql_str = sl.get_usage_grant_sql(app_role="app_role")

    assert deploy_sql_str == snapshot
    assert (
        grant_sql_str
        == f"GRANT USAGE ON FUNCTION IDENTIFIER('func1') TO ROLE app_role;"
    )


@mock.patch(ANACONDA_PACKAGES)
def test_nativeapp_children_interface(
    mock_anaconda,
    example_function_workspace,
    snapshot,
    enable_snowpark_glob_support_feature_flag,
):
    mock_anaconda.return_value = AnacondaPackages(
        {
            "pandas": AvailablePackage("pandas", "1.2.3"),
            "numpy": AvailablePackage("numpy", "1.2.3"),
            "snowflake_snowpark_python": AvailablePackage(
                "snowflake_snowpark_python", "1.2.3"
            ),
        }
    )

    sl, action_context = example_function_workspace

    sl.bundle(None, False, False, None, False)
    bundle_artifact = (
        sl.root / "output" / "bundle" / "snowpark" / "my_snowpark_project" / "app.py"
    )
    deploy_sql_str = sl.get_deploy_sql(CreateMode.create)
    grant_sql_str = sl.get_usage_grant_sql(app_role="app_role")

    assert bundle_artifact.exists()
    assert deploy_sql_str == snapshot
    assert (
        grant_sql_str
        == f"GRANT USAGE ON FUNCTION IDENTIFIER('func1') TO ROLE app_role;"
    )


@mock.patch(EXECUTE_QUERY)
def test_action_describe(mock_execute, example_function_workspace):
    entity, action_context = example_function_workspace
    result = entity.action_describe(action_context)

    mock_execute.assert_called_with("DESCRIBE FUNCTION IDENTIFIER('func1');")


@mock.patch(EXECUTE_QUERY)
def test_action_drop(mock_execute, example_function_workspace):
    entity, action_context = example_function_workspace
    result = entity.action_drop(action_context)

    mock_execute.assert_called_with("DROP FUNCTION IDENTIFIER('func1');")


@pytest.mark.parametrize(
    "execution_arguments", [None, ["arg1", "arg2"], ["foo", 42, "bar"]]
)
@mock.patch(EXECUTE_QUERY)
def test_action_execute(
    mock_execute, execution_arguments, example_function_workspace, snapshot
):
    entity, action_context = example_function_workspace
    result = entity.action_execute(action_context, execution_arguments)

    mock_execute.assert_called_with(snapshot)


@mock.patch(ANACONDA_PACKAGES)
def test_bundle_old_build(mock_anaconda, example_function_workspace):
    mock_anaconda.return_value = AnacondaPackages(
        {
            "pandas": AvailablePackage("pandas", "1.2.3"),
            "numpy": AvailablePackage("numpy", "1.2.3"),
            "snowflake_snowpark_python": AvailablePackage(
                "snowflake_snowpark_python", "1.2.3"
            ),
        }
    )
    entity, action_context = example_function_workspace
    entity.action_bundle(action_context, None, False, False, None, False)

    assert (entity.root / "app.py").exists()


@mock.patch(ANACONDA_PACKAGES)
def test_bundle(
    mock_anaconda, example_function_workspace, enable_snowpark_glob_support_feature_flag
):
    mock_anaconda.return_value = AnacondaPackages(
        {
            "pandas": AvailablePackage("pandas", "1.2.3"),
            "numpy": AvailablePackage("numpy", "1.2.3"),
            "snowflake_snowpark_python": AvailablePackage(
                "snowflake_snowpark_python", "1.2.3"
            ),
        }
    )
    entity, action_context = example_function_workspace
    entity.action_bundle(action_context, None, False, False, None, False)

    output = entity.root / "output" / "bundle" / "snowpark"  # noqa
    assert output.exists()
    assert (output / "my_snowpark_project" / "app.py").exists()


def test_describe_function_sql(example_function_workspace):
    entity, _ = example_function_workspace
    assert entity.get_describe_sql() == "DESCRIBE FUNCTION IDENTIFIER('func1');"


def test_drop_function_sql(example_function_workspace):
    entity, _ = example_function_workspace
    assert entity.get_drop_sql() == "DROP FUNCTION IDENTIFIER('func1');"


@pytest.mark.parametrize(
    "execution_arguments", [None, ["arg1", "arg2"], ["foo", 42, "bar"]]
)
def test_function_get_execute_sql(
    execution_arguments, example_function_workspace, snapshot
):
    entity, _ = example_function_workspace
    assert entity.get_execute_sql(execution_arguments) == snapshot


@pytest.mark.parametrize(
    "mode",
    [CreateMode.create, CreateMode.create_or_replace, CreateMode.create_if_not_exists],
)
def test_get_deploy_sql(mode, example_function_workspace, snapshot):
    entity, _ = example_function_workspace
    assert entity.get_deploy_sql(mode) == snapshot


def test_get_usage_grant_sql(example_function_workspace):
    entity, _ = example_function_workspace
    assert (
        entity.get_usage_grant_sql("test_role")
        == "GRANT USAGE ON FUNCTION IDENTIFIER('func1') TO ROLE test_role;"
    )


def test_get_deploy_sql_with_repository_packages(example_function_workspace, snapshot):
    entity, _ = example_function_workspace
    entity.model.artifact_repository = "snowflake.snowpark.pypi_shared_repository"
    entity.model.artifact_repository_packages = ["package1", "package2"]
    entity.model.resource_constraint = {"architecture": "x86"}
    deploy_sql = entity.get_deploy_sql(CreateMode.create)
    assert deploy_sql == snapshot
