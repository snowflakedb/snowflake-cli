from pathlib import Path
from typing import Dict, List, Optional
from unittest import mock

import pytest
import snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils as ef_utils

from tests.testing_utils.files_and_dirs import temp_local_dir


def test_sanitize_ex_fn_attribute():
    ex_fn = {"name": "john", "city": "toronto"}
    ef_utils._sanitize_ex_fn_attribute(attr="name", ex_fn=ex_fn)  # noqa: SLF001
    assert ex_fn["name"] == "john"
    ef_utils._sanitize_ex_fn_attribute(  # noqa: SLF001
        attr="name", ex_fn=ex_fn, make_uppercase=True
    )
    assert ex_fn["name"] == "JOHN"

    ex_fn["city"] = ""
    ef_utils._sanitize_ex_fn_attribute(attr="city", ex_fn=ex_fn)  # noqa: SLF001
    assert ex_fn["city"] is None

    ex_fn["team"] = "nade"
    ef_utils._sanitize_ex_fn_attribute(  # noqa: SLF001
        attr="team", ex_fn=ex_fn, make_uppercase=True, expected_type=str
    )
    assert ex_fn["team"] == "NADE"

    optional_expected_type: Optional[List] = []
    with pytest.raises(ef_utils.MalformedExtensionFunctionError):
        ef_utils._sanitize_ex_fn_attribute(  # noqa: SLF001
            attr="team",
            ex_fn=ex_fn,
            make_uppercase=True,
            expected_type=type(optional_expected_type),
        )

    ex_fn["team"] = None
    ef_utils._sanitize_ex_fn_attribute(  # noqa: SLF001
        attr="team",
        ex_fn=ex_fn,
        make_uppercase=True,
        expected_type=type(optional_expected_type),
    )
    assert ex_fn["team"] is None

    ex_fn["team"] = ["nade"]
    ef_utils._sanitize_ex_fn_attribute(  # noqa: SLF001
        attr="team",
        ex_fn=ex_fn,
        make_uppercase=True,
        expected_type=type(optional_expected_type),
    )
    assert ex_fn["team"] == ["nade"]

    with pytest.raises(ef_utils.MalformedExtensionFunctionError):
        ef_utils._sanitize_ex_fn_attribute(  # noqa: SLF001
            attr="team", ex_fn=ex_fn, make_uppercase=True, expected_type=Dict
        )


def test_create_missing_attr_str():
    result = ef_utils._create_missing_attr_str(  # noqa: SLF001
        attribute="object type", py_file=Path("some/path")
    )
    assert (
        result
        == f"Required attribute 'object type' of extension function is missing for an extension function defined in python file {Path('some/path').absolute()}."
    )


@pytest.mark.skip(reason="This is a TODO, and will be fixed as a fast follow.")
@pytest.mark.parametrize(
    ("input_param, expected"),
    [
        ("a/b/c", "a.b.c"),
        # ("a/b/c/d.py", "a.b.c.d"),
        # ("a/b/c/d.jar", "a.b.c.d.jar"),
        # ("/a/b/c/d.py", "a.b.c.d"),
        # ("/a/b/c/d.py.zip", "a.b.c.d.py.zip"),  # TODO: what do we want to do here?
    ],
)
def test_get_handler_path_without_suffix(input_param, expected):
    dir_structure = {
        "output/deploy/a/b/c": None,
        "output/deploy/a/b/c/d/main.py": "# this is a file\n",
        "output/deploy/a/b/c/d/main1.jar": "",
    }
    with temp_local_dir(dir_structure=dir_structure) as local_path:
        dest_path = Path(local_path, "output", "deploy", input_param)
        actual = ef_utils._get_handler_path_without_suffix(  # noqa: SLF001
            file_path=dest_path,
            suffix_str_to_rm=".py",
            deploy_root=Path(local_path, "output", "deploy"),
        )
        assert actual == expected


@pytest.mark.parametrize(
    ("input_param, expected"),
    [
        ("TABLE_FUNCTION", "TABLE FUNCTION"),
        ("FUNCTION", "FUNCTION"),
    ],
)
def test_get_object_type_as_text(input_param, expected):
    actual = ef_utils.get_object_type_as_text(input_param)  # noqa: SLF001
    assert actual == expected


@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils._get_handler_path_without_suffix"
)
def test_get_handler(mock_helper):
    mock_helper.return_value = "a.b.c.d"
    actual = ef_utils._get_handler(  # noqa: SLF001
        Path("a/b/c/d.py"), "DUMMY_FUNC", deploy_root=Path("some/path")
    )
    assert actual == "a.b.c.d.DUMMY_FUNC"


def test_get_handler_exception():
    with pytest.raises(ef_utils.MalformedExtensionFunctionError):
        ef_utils._get_handler(  # noqa: SLF001
            Path("a/b/c/d.py"), ("FUNC1", "FUNC2"), deploy_root=Path("some/path")
        )


@pytest.mark.parametrize(
    ("object_name, schema, func, expected"),
    [
        ("SNOWPARK_TEMP_DUMMY", "core", "DUMMY_HANDLER", "core.DUMMY_HANDLER"),
        ("DUMMY_HANDLER", "core", "DUMMY_HANDLER", "core.DUMMY_HANDLER"),
    ],
)
def test_get_schema_and_name_for_extension_function(
    object_name, schema, func, expected
):
    actual = ef_utils._get_schema_and_name_for_extension_function(  # noqa: SLF001
        object_name, schema, func
    )
    assert actual == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ["'john", False],
        ["'john'", True],
        ["john'", False],
        ['"john"', False],
    ],
)
def test_is_single_quoted(name, expected):
    assert ef_utils._is_single_quoted(name=name) == expected  # noqa: SLF001


def test_ensure_single_quoted():
    assert ef_utils._ensure_single_quoted(  # noqa: SLF001
        ["'john", "'john'", "john'", '"john"']
    ) == [
        "''john'",
        "'john'",
        "'john''",
        "'\"john\"'",
    ]


def test_get_all_imports():
    input_lst = [
        "tests/resources/test_udf_dir/test_udf_file.py",
        "/tmp/temp.txt",
        (
            "tests/resources/test_udf_dir/test_udf_file.py",
            "resources.test_udf_dir.test_udf_file",
        ),
    ]
    actual = ef_utils._get_all_imports(input_lst, ".py")  # noqa: SLF001
    assert (
        actual
        == "'tests/resources/test_udf_dir/test_udf_file.py','/tmp/temp.txt','resources/test_udf_dir/test_udf_file.py'"
    )


def test_is_function_wellformed():
    ex_fn = {}
    assert not ef_utils._is_function_wellformed(ex_fn=ex_fn)  # noqa: SLF001

    ex_fn["func"] = []
    assert not ef_utils._is_function_wellformed(ex_fn=ex_fn)  # noqa: SLF001

    ex_fn["func"] = " "
    assert not ef_utils._is_function_wellformed(ex_fn=ex_fn)  # noqa: SLF001

    ex_fn["func"] = "dummy"
    assert ef_utils._is_function_wellformed(ex_fn=ex_fn)  # noqa: SLF001

    ex_fn["func"] = (None, " ")
    assert not ef_utils._is_function_wellformed(ex_fn=ex_fn)  # noqa: SLF001

    ex_fn["func"] = (None, "dummy")
    assert ef_utils._is_function_wellformed(ex_fn=ex_fn)  # noqa: SLF001


def test_add_defaults_to_extension_function_required_keys():
    ex_fn = {}
    some_path = Path("some/path")
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("object_type")

    ex_fn["object_type"] = "function"
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("object_name")

    ex_fn["object_name"] = "some_name"
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("return_sql")

    ex_fn["return_sql"] = "returns null"
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("func")

    # Func possibilities already tested in a test case above this

    ex_fn["func"] = "dummy_func"
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("raw_imports")

    ex_fn["raw_imports"] = ["some/path"]
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("schema")

    ex_fn["schema"] = "core"
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("runtime_version")

    ex_fn["runtime_version"] = "3.8"
    assert ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    # This check is because we cannot stop the function execution beyond required keys, but there are other tests below that go in depth

    assert ex_fn["object_type"] == "FUNCTION"
    assert ex_fn["object_name"] == "SOME_NAME"
    assert ex_fn["return_sql"] == "RETURNS NULL"
    assert ex_fn["func"] == "dummy_func"
    assert ex_fn["raw_imports"] == ["some/path"]
    assert ex_fn["schema"] == "CORE"
    assert not ex_fn["anonymous"]
    assert not ex_fn["replace"]
    assert not ex_fn["if_not_exists"]
    assert ex_fn["input_args"] == []
    assert ex_fn["all_imports"] is None
    assert ex_fn["input_sql_types"] == []
    assert ex_fn["all_packages"] is None
    assert ex_fn["external_access_integrations"] is None
    assert ex_fn["secrets"] is None
    assert ex_fn["inline_python_code"] is None
    assert ex_fn["execute_as"] is None
    assert ex_fn["runtime_version"] is "3.8"
    assert ex_fn["handler"] is None
    assert ex_fn["application_roles"] == []


def test_add_defaults_to_extension_function_other_malformed_keys():
    ex_fn = {
        "object_type": "function",
        "object_name": "some_name",
        "return_sql": "returns null",
        "func": "dummy_func",
        "raw_imports": ["some/path"],
        "schema": "core",
        "replace": True,
        "if_not_exists": True,
        "runtime_version": "3.8",
    }
    some_path = Path("some/path")

    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("incompatible")

    ex_fn["if_not_exists"] = False
    ex_fn["input_args"] = ["dummy"]
    ex_fn["input_sql_types"] = ["dummy", "values"]
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("number of extension function parameters")

    ex_fn["input_sql_types"] = ["dummy"]
    ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert ex_fn["application_roles"] == []

    ex_fn["application_roles"] = ["app_viewer", None, {}]
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert err.value.message.__contains__("application_roles")

    ex_fn["application_roles"] = ["app_viewer", "app_admin"]
    ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert ex_fn["application_roles"] == ["APP_VIEWER", "APP_ADMIN"]
