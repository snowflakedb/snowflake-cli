from pathlib import Path
from unittest import mock

import pytest
import snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils as ef_utils

from tests.testing_utils.files_and_dirs import temp_local_dir


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
    actual = ef_utils._get_object_type_as_text(input_param)  # noqa: SLF001
    assert actual == expected


@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils._get_handler_path_without_suffix"
)
@pytest.mark.parametrize(
    ("dest_file, func, expected, helper_ret"),
    [
        ("a/b/c/d.py", "DUMMY_FUNC", "a.b.c.d.DUMMY_FUNC", "a.b.c.d"),
        ("a/b/c/d.py", ("FUNC1", "FUNC2"), None, "a.b.c.d"),
    ],
)
def test_get_handler(mock_helper, dest_file, func, expected, helper_ret):
    mock_helper.return_value = helper_ret
    actual = ef_utils._get_handler(  # noqa: SLF001
        Path(dest_file), func, deploy_root=Path("some/path")
    )
    assert actual == expected


@pytest.mark.parametrize(
    ("object_name, schema, handler, expected"),
    [
        ("SNOWPARK_TEMP_DUMMY", "core", "DUMMY_HANDLER", "core.DUMMY_HANDLER"),
        ("DUMMY_HANDLER", "core", "DUMMY_HANDLER", "core.DUMMY_HANDLER"),
    ],
)
def test_get_object_name_for_udf_sp(object_name, schema, handler, expected):
    actual = ef_utils._get_schema_and_name_for_extension_function(  # noqa: SLF001
        object_name, schema, handler
    )
    assert actual == expected


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
