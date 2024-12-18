from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest
from snowflake.cli._plugins.snowpark.snowpark_entity import (
    FunctionEntity,
    ProcedureEntity,
)
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli._plugins.workspace.context import WorkspaceContext
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.definition_manager import DefinitionManager

from tests.testing_utils.mock_config import mock_config_key


def test_cannot_instantiate_without_feature_flag():
    with pytest.raises(NotImplementedError) as err:
        FunctionEntity()
    assert str(err.value) == "Snowpark entities are not implemented yet"

    with pytest.raises(NotImplementedError) as err:
        ProcedureEntity()
    assert str(err.value) == "Snowpark entities are not implemented yet"


def test_function_implements_nativeapp_children_interface(temp_dir):
    with mock_config_key("enable_native_app_children", True):
        dm = DefinitionManager()
        ctx = WorkspaceContext(
            console=cc,
            project_root=dm.project_root,
            get_default_role=lambda: "mock_role",
            get_default_warehouse=lambda: "mock_warehouse",
        )
        main_file = "main.py"
        (Path(temp_dir) / main_file).touch()
        model = FunctionEntityModel(
            type="function",
            handler="my_schema.my_func",
            returns="integer",
            signature=[
                {"name": "input_number", "type": "integer"},
                {"name": "input_string", "type": "text"},
            ],
            stage="my_stage",
            artifacts=[main_file],
        )
        model._entity_id = "my_func"  # noqa: SLF001
        schema = "my_schema"
        fn = FunctionEntity(model, ctx)

        fn.bundle()
        bundle_artifact = Path(temp_dir) / "output" / "deploy" / main_file
        deploy_sql_str = fn.get_deploy_sql(schema=schema)
        grant_sql_str = fn.get_usage_grant_sql(app_role="app_role", schema=schema)

        assert bundle_artifact.exists()
        assert (
            deploy_sql_str
            == dedent(
                """
            CREATE OR REPLACE FUNCTION my_schema.my_func(input_number integer, input_string text)
            RETURNS integer
            LANGUAGE python
            RUNTIME_VERSION=3.8
            IMPORTS=()
            HANDLER='my_schema.my_func'
            PACKAGES=('snowflake-snowpark-python');
            """
            ).strip()
        )
        assert (
            grant_sql_str
            == "GRANT USAGE ON FUNCTION my_schema.my_func(integer, text) TO APPLICATION ROLE app_role;"
        )


def test_procedure_implements_nativeapp_children_interface(temp_dir):
    with mock_config_key("enable_native_app_children", True):
        dm = DefinitionManager()
        ctx = WorkspaceContext(
            console=cc,
            project_root=dm.project_root,
            get_default_role=lambda: "mock_role",
            get_default_warehouse=lambda: "mock_warehouse",
        )
        main_file = "main.py"
        (Path(temp_dir) / main_file).touch()
        model = ProcedureEntityModel(
            type="procedure",
            handler="my_schema.my_sproc",
            returns="integer",
            signature=[
                {"name": "input_number", "type": "integer"},
                {"name": "input_string", "type": "text"},
            ],
            stage="my_stage",
            artifacts=[main_file],
        )
        model._entity_id = "my_sproc"  # noqa: SLF001
        schema = "my_schema"
        fn = ProcedureEntity(model, ctx)

        fn.bundle()
        bundle_artifact = Path(temp_dir) / "output" / "deploy" / main_file
        deploy_sql_str = fn.get_deploy_sql(schema=schema)
        grant_sql_str = fn.get_usage_grant_sql(app_role="app_role", schema=schema)

        assert bundle_artifact.exists()
        assert (
            deploy_sql_str
            == dedent(
                """
            CREATE OR REPLACE PROCEDURE my_schema.my_sproc(input_number integer, input_string text)
            RETURNS integer
            LANGUAGE python
            RUNTIME_VERSION=3.8
            IMPORTS=()
            HANDLER='my_schema.my_sproc'
            PACKAGES=('snowflake-snowpark-python');
            """
            ).strip()
        )
        assert (
            grant_sql_str
            == "GRANT USAGE ON PROCEDURE my_schema.my_sproc(integer, text) TO APPLICATION ROLE app_role;"
        )
