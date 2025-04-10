from unittest import mock

import pytest
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.workspace.context import WorkspaceContext
from snowflake.cli._plugins.workspace.manager import WorkspaceManager
from snowflake.cli.api.entities.utils import EntityActions
from snowflake.cli.api.exceptions import CycleDetectedError
from snowflake.cli.api.project.definition import load_project
from snowflake.cli.api.project.errors import SchemaValidationError

from tests.testing_utils.mock_config import mock_config_key

EXECUTE_QUERY = "snowflake.cli.api.sql_execution.BaseSqlExecutor.execute_query"


@pytest.fixture
def example_workspace(project_directory):
    # TODO: try to make a common fixture for all entities
    def _workspace_fixture(project_name: str, entity_name: str, entity_class: type):
        with mock_config_key("enable_native_app_children", True):
            with project_directory(project_name) as pdir:
                project = load_project([pdir / "snowflake.yml"])

                model = project.project_definition.entities.get(entity_name)

                workspace_context = WorkspaceContext(
                    console=mock.MagicMock(),
                    project_root=pdir,
                    get_default_role=lambda: "test_role",
                    get_default_warehouse=lambda: "test_warehouse",
                )

                workspace_manager = WorkspaceManager(project.project_definition, pdir)

            return (
                entity_class(workspace_ctx=workspace_context, entity_model=model),
                workspace_manager.action_ctx,
            )

    return _workspace_fixture


@pytest.fixture
def cyclic_dependency_workspace(example_workspace):
    return example_workspace(
        "depends_on_with_cyclic_dependency", "test_streamlit", StreamlitEntity
    )


@pytest.fixture
def basic_workspace(example_workspace):
    return example_workspace("depends_on_basic", "test_streamlit", StreamlitEntity)


def test_cyclic(cyclic_dependency_workspace):
    with pytest.raises(CycleDetectedError) as err:
        with mock_config_key("enable_native_app_children", True):
            entity, action_ctx = cyclic_dependency_workspace
            _ = entity.dependent_entities(action_ctx)
    assert err.value.message == "Cycle detected in entity dependencies: test_function"


def test_dependencies_must_exist_in_project_file(
    project_directory, alter_snowflake_yml
):
    with project_directory("depends_on_with_cyclic_dependency") as pdir:
        alter_snowflake_yml(
            snowflake_yml_path=pdir / "snowflake.yml",
            parameter_path="entities.test_streamlit.meta.depends_on.0",
            value="foo",
        )
        alter_snowflake_yml(
            snowflake_yml_path=pdir / "snowflake.yml",
            parameter_path="entities.test_procedure.meta.depends_on.0",
            value="bar",
        )
        with pytest.raises(SchemaValidationError) as err:
            project = load_project([pdir / "snowflake.yml"])

    assert (
        " Entity test_procedure depends on non-existing entities: bar"
        in err.value.message
    )
    assert (
        " Entity test_streamlit depends on non-existing entities: foo"
        in err.value.message
    )


def test_dependencies_basic(basic_workspace):
    with mock_config_key("enable_native_app_children", True):
        entity, action_ctx = basic_workspace
        result = entity.dependent_entities(action_ctx)

    assert len(result) == 3
    assert result[0].entity_id == "test_function2"
    assert result[1].entity_id == "test_function"
    assert result[2].entity_id == "test_procedure"


@pytest.mark.skip(
    reason="With switch to entites, this test would require multiple mocks. Move to integration"
)
@mock.patch(EXECUTE_QUERY)
def test_deploy_with_dependencies(mock_execute, basic_workspace):
    with mock_config_key("enable_native_app_children", True):
        entity, action_ctx = basic_workspace
        entity.perform(EntityActions.DEPLOY, action_ctx, _open=False, replace=False)

    assert mock_execute.call_count == 4
    assert "IDENTIFIER('test_function2')" in mock_execute.call_args_list[0][0][0]
    assert "IDENTIFIER('test_function')" in mock_execute.call_args_list[1][0][0]
    assert "IDENTIFIER('test_procedure')" in mock_execute.call_args_list[2][0][0]
    assert "IDENTIFIER('test_streamlit')" in mock_execute.call_args_list[3][0][0]


@mock.patch(EXECUTE_QUERY)
def test_if_bundling_dependencies_resolves_requirements(mock_execute, basic_workspace):
    with mock_config_key("enable_native_app_children", True):
        entity, action_ctx = basic_workspace
        entity.perform(EntityActions.BUNDLE, action_ctx)

        output_dir = entity.root / "output" / "bundle"
        dependencies_zip = output_dir / "snowpark" / "dependencies.zip"

        assert output_dir.exists()
        assert dependencies_zip.exists()
