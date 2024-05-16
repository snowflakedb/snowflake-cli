from pathlib import Path

import pytest
import snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils as ef_utils

# --------------------------------------------------------
# ------------- get_object_type_as_text ------------------
# --------------------------------------------------------


@pytest.mark.parametrize(
    ("input_param, expected"),
    [
        ("TABLE_FUNCTION", "TABLE FUNCTION"),
        ("FUNCTION", "FUNCTION"),
        ("AGGREGATE-FUNCTION", "AGGREGATE-FUNCTION"),
    ],
)
def test_get_object_type_as_text(input_param, expected):
    actual = ef_utils.get_object_type_as_text(input_param)
    assert actual == expected


# --------------------------------------------------------
# --------- sanitize_extension_function_data -------------
# --------------------------------------------------------


def test_sanitize_extension_function_data_required_keys(snapshot):
    """
    This test will start off with an empty dictionary which will act as the extension function.
    With every exception sanitize_extension_function_data() hits, we add in the required info
    to progress to the next exception/execution.
    """
    ex_fn = {}
    some_path = Path("some/path")

    # Test for absence or malformed object_type, object_name and return_sql
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "object_type" in err.value.message

    ex_fn["object_type"] = ["function"]
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "object_type" in err.value.message

    ex_fn["object_type"] = "function"
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "object_name" in err.value.message

    ex_fn["object_name"] = "some_name"
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "return_sql" in err.value.message

    ex_fn["return_sql"] = "returns null"

    # Test for absence of func, and malformed func
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "func" in err.value.message

    wrong_func_possibilities = [[], "", " ", (None, ""), (None, " ")]
    for val in wrong_func_possibilities:
        ex_fn["func"] = val
        with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
            ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
        assert "func" in err.value.message

    right_func_possibilities = [[None, "dummy"], "dummy"]
    for val in right_func_possibilities:
        ex_fn["func"] = val
        with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
            ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
        assert "raw_imports" in err.value.message

    # Test for absence or malformed schema and runtime_version
    ex_fn["raw_imports"] = {""}
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "raw_imports" in err.value.message

    ex_fn["raw_imports"] = ["some/path"]
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "schema" in err.value.message

    ex_fn["schema"] = "core"
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "runtime_version" in err.value.message

    ex_fn["runtime_version"] = "3.8"
    ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)

    assert ex_fn == snapshot


def test_sanitize_extension_function_data_other_malformed_keys():
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
    assert "incompatible" in err.value.message

    ex_fn["if_not_exists"] = False
    ex_fn["input_args"] = ["dummy"]
    ex_fn["input_sql_types"] = ["dummy", "values"]
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "number of extension function parameters" in err.value.message

    ex_fn["input_sql_types"] = ["dummy"]
    ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert ex_fn["application_roles"] == []

    ex_fn["application_roles"] = ["app_viewer", None, {}]
    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert "application_roles" in err.value.message

    ex_fn["application_roles"] = ["app_viewer", "app_admin"]
    ef_utils.sanitize_extension_function_data(ex_fn=ex_fn, py_file=some_path)
    assert ex_fn["application_roles"] == ["APP_VIEWER", "APP_ADMIN"]


# --------------------------------------------------------
# ------------------- enrich_ex_fn -----------------------
# --------------------------------------------------------


def test_enrich_ex_fn(snapshot):
    ex_fn = {
        "func": [None, "dummy"],
        "object_name": "SNOWPARK_TEMP_",
        "schema": None,
        "raw_imports": [],
    }

    with pytest.raises(ef_utils.MalformedExtensionFunctionError) as err:
        ef_utils.enrich_ex_fn(
            ex_fn=ex_fn,
            py_file=Path("some", "file.py"),
            deploy_root=Path("output", "deploy"),
        )
    assert "determine handler name" in err.value.message

    ex_fn["func"] = "dummy"
    ef_utils.enrich_ex_fn(
        ex_fn=ex_fn,
        py_file=Path("some", "file.py"),
        deploy_root=Path("output", "deploy"),
    )
    assert ex_fn["object_name"] == "dummy"

    ex_fn["object_name"] = "MY_FUNC"
    ef_utils.enrich_ex_fn(
        ex_fn=ex_fn,
        py_file=Path("some", "file.py"),
        deploy_root=Path("output", "deploy"),
    )
    assert ex_fn["object_name"] == "MY_FUNC"

    ex_fn["schema"] = ""
    ef_utils.enrich_ex_fn(
        ex_fn=ex_fn,
        py_file=Path("some", "file.py"),
        deploy_root=Path("output", "deploy"),
    )
    assert ex_fn["object_name"] == "MY_FUNC"

    ex_fn["schema"] = "core"
    ex_fn["raw_imports"] = [
        "a/b/c.py",
        "a/b/c",
        ["a/b/c.py", "a.b.c"],
        ["a/b/c.jar", "a.b.c"],
        ["a/b/c", "a.b.c"],
    ]
    ef_utils.enrich_ex_fn(
        ex_fn=ex_fn,
        py_file=Path("some", "file.py"),
        deploy_root=Path("output", "deploy"),
    )
    assert ex_fn["object_name"] == "core.MY_FUNC"
    assert ex_fn["all_imports"] == snapshot
