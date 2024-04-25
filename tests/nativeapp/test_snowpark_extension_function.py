import logging
import os
from pathlib import Path

import pytest
from snowflake.cli.plugins.nativeapp.setup_script_compiler.snowpark_extension_function import (
    TEMP_OBJECT_NAME_PREFIX,
    get_handler_path_without_suffix,
    get_object_type_as_text,
)
from snowflake.snowpark._internal.utils import TempObjectType

log = logging.getLogger(__name__)

DUMMY_SCHEMA = "dummy_schema"
DUMMY_HANDLER = "dummy_handler"


def dummy_func():
    pass


@pytest.mark.parametrize(
    "file_path_str, resolved_path",
    [
        [
            f"src{os.path.sep}java{os.path.sep}annotation{os.path.sep}dummy.jar",
            "src.java.annotation.dummy",
        ],
        [
            f"{os.path.sep}resources{os.path.sep}python{os.path.sep}annotation{os.path.sep}dummy.tar.gz",
            "src.resources.annotation.dummy",
        ],
    ],
)
def test_get_handler_path_without_suffix(file_path_str, resolved_path):
    assert get_handler_path_without_suffix(Path(file_path_str)) == resolved_path


@pytest.mark.parametrize(
    "object_type, resolved_type",
    [
        [TempObjectType.FUNCTION, "FUNCTION"],
        [TempObjectType.PROCEDURE, "PROCEDURE"],
        [TempObjectType.TABLE_FUNCTION, "TABLE FUNCTION"],
        [TempObjectType.AGGREGATE_FUNCTION, "AGGREGATE FUNCTION"],
    ],
)
def test_get_object_type_as_text(object_type, resolved_type):
    assert get_object_type_as_text(object_type) == resolved_type


@pytest.mark.parametrize(
    "native_app_params, handler, resolved_schema",
    [
        [None, DUMMY_HANDLER, None],
        [{"application_roles": []}, DUMMY_HANDLER, None],
        [{"schema": DUMMY_SCHEMA}, None, DUMMY_SCHEMA],
    ],
)
def test_set_schema(
    native_app_params, handler, resolved_schema, dummy_extension_function_obj
):
    dummy_extension_function_obj.native_app_params = native_app_params
    dummy_extension_function_obj.handler = handler
    dummy_extension_function_obj.set_schema()
    assert dummy_extension_function_obj.schema == resolved_schema


@pytest.mark.parametrize(
    "ext_func_name, handler, schema, resolved_name",
    [
        [
            f"{TEMP_OBJECT_NAME_PREFIX}_dummy",
            DUMMY_HANDLER,
            DUMMY_SCHEMA,
            f"{DUMMY_SCHEMA}.{DUMMY_HANDLER}",
        ],
        ["dummy_name", None, None, "dummy_name"],
    ],
)
def test_set_object_name_for_udf_sp(
    ext_func_name, handler, schema, resolved_name, dummy_extension_function_obj
):
    dummy_extension_function_obj.object_name = ext_func_name
    dummy_extension_function_obj.handler = handler
    dummy_extension_function_obj.schema = schema
    dummy_extension_function_obj.set_object_name_for_udf_sp()
    assert dummy_extension_function_obj.object_name == resolved_name


@pytest.mark.parametrize(
    "func_name, resolved_name",
    [
        [lambda: None, "src.python.annotation.dummy.<lambda>"],
        [dummy_func, "src.python.annotation.dummy.dummy_func"],
    ],
)
def test_set_handler_type_callable(
    func_name, resolved_name, dummy_extension_function_obj
):
    file_path_str = (
        f"src{os.path.sep}python{os.path.sep}annotation{os.path.sep}dummy.py"
    )
    dummy_extension_function_obj.set_destination_file(Path(file_path_str))
    dummy_extension_function_obj.func = func_name
    dummy_extension_function_obj.set_handler()
    assert dummy_extension_function_obj.handler == resolved_name


def test_set_handler_type_tuple(dummy_extension_function_obj):
    file_path_str = "src/annotation/dummy.py"
    func_name = "dummy_func"
    dummy_extension_function_obj.func = (file_path_str, func_name)
    dummy_extension_function_obj.set_handler()
    assert dummy_extension_function_obj.handler == None


@pytest.mark.parametrize(
    "native_app_params, resolved_stmts",
    [
        [None, ""],
        [{"schema": DUMMY_SCHEMA}, ""],
        [
            {"schema": DUMMY_SCHEMA, "application_roles": ["app_viewer", "app_admin"]},
            "GRANT USAGE ON FUNCTION DUMMY_NAME\nTO APPLICATION ROLE APP_VIEWER;\n\nGRANT USAGE ON FUNCTION DUMMY_NAME\nTO APPLICATION ROLE APP_ADMIN;\n",
        ],
    ],
)
def test_generate_grant_sql_statements(
    native_app_params, resolved_stmts, dummy_extension_function_obj
):
    dummy_extension_function_obj.native_app_params = native_app_params
    dummy_extension_function_obj.set_source_file(Path("some/dummy.py"))
    dummy_extension_function_obj.set_application_roles()
    assert (
        dummy_extension_function_obj.generate_grant_sql_statements() == resolved_stmts
    )
