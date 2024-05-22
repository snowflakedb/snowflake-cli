import copy
import subprocess
from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli.api.project.schemas.native_app.path_mapping import ProcessorMapping
from snowflake.cli.plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    SandboxExecutionError,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
    _determine_virtual_env,
    _execute_in_sandbox,
    generate_create_sql_ddl_statement,
    generate_grant_sql_ddl_statements,
)

from tests.testing_utils.files_and_dirs import temp_local_dir

PROJECT_ROOT = Path("/path/to/project")

# --------------------------------------------------------
# ------------- _determine_virtual_env -------------------
# --------------------------------------------------------


@pytest.mark.parametrize(
    "input_param, expected",
    [
        [ProcessorMapping(name="dummy", properties={"random": "random"}), {}],
        [ProcessorMapping(name="dummy", properties={"env": {"random": "random"}}), {}],
        [
            ProcessorMapping(
                name="dummy",
                properties={"env": {"type": "conda", "name": "snowpark-dev"}},
            ),
            {"env_type": ExecutionEnvironmentType.CONDA, "name": "snowpark-dev"},
        ],
        [
            ProcessorMapping(
                name="dummy", properties={"env": {"type": "venv", "path": "some/path"}}
            ),
            {
                "env_type": ExecutionEnvironmentType.VENV,
                "path": PROJECT_ROOT / "some/path",
            },
        ],
        [
            ProcessorMapping(
                name="dummy", properties={"env": {"type": "venv", "path": "/some/path"}}
            ),
            {"env_type": ExecutionEnvironmentType.VENV, "path": Path("/some/path")},
        ],
        [
            ProcessorMapping(name="dummy", properties={"env": {"type": "current"}}),
            {"env_type": ExecutionEnvironmentType.CURRENT},
        ],
        [ProcessorMapping(name="dummy", properties={"env": {"type": "other"}}), {}],
        [
            ProcessorMapping(name="dummy", properties={"env": {"type": "conda"}}),
            {"env_type": ExecutionEnvironmentType.CONDA, "name": None},
        ],
        [
            ProcessorMapping(name="dummy", properties={"env": {"type": "venv"}}),
            {"env_type": ExecutionEnvironmentType.VENV, "path": None},
        ],
    ],
)
def test_determine_virtual_env(input_param, expected):
    actual = _determine_virtual_env(project_root=PROJECT_ROOT, processor=input_param)
    assert actual == expected


# --------------------------------------------------------
# --------------- _execute_in_sandbox --------------------
# --------------------------------------------------------


@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor.execute_script_in_sandbox"
)
def test_execute_in_sandbox_full_entity(mock_sandbox):
    mock_completed_process = mock.MagicMock(spec=subprocess.CompletedProcess)
    mock_completed_process.stdout = '[{"name": "john"}, {"name": "jane"}]'
    mock_completed_process.stderr = ""
    mock_completed_process.returncode = 0
    mock_sandbox.return_value = mock_completed_process

    entity = _execute_in_sandbox(
        py_file="some_file", deploy_root=Path("some/path"), kwargs={}
    )
    assert entity == [{"name": "john"}, {"name": "jane"}]


@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor.execute_script_in_sandbox"
)
def test_execute_in_sandbox_all_possible_none_cases(mock_sandbox):
    mock_completed_process = mock.MagicMock(spec=subprocess.CompletedProcess)
    mock_completed_process.returncode = 1
    mock_completed_process.stderr = "DUMMY"
    mock_completed_process.stdout = "DUMMY"
    mock_sandbox.return_value = mock_completed_process
    assert (
        _execute_in_sandbox(
            py_file="some_file", deploy_root=Path("some/path"), kwargs={}
        )
        is None
    )

    mock_completed_process.returncode = 0
    mock_completed_process.stderr = "DUMMY"
    mock_completed_process.stdout = '[{"name": "john"}, {"name": "jane"}'  # Bad input
    assert (
        _execute_in_sandbox(
            py_file="some_file", deploy_root=Path("some/path"), kwargs={}
        )
        is None
    )

    mock_sandbox.side_effect = SandboxExecutionError("dummy error")
    assert (
        _execute_in_sandbox(
            py_file="some_file", deploy_root=Path("some/path"), kwargs={}
        )
        is None
    )

    mock_sandbox.side_effect = ValueError("dummy error")
    assert (
        _execute_in_sandbox(
            py_file="some_file", deploy_root=Path("some/path"), kwargs={}
        )
        is None
    )


# --------------------------------------------------------
# ------- generate_create_sql_ddl_statements -------------
# --------------------------------------------------------


def test_generate_create_sql_ddl_statements_w_all_entries(
    native_app_codegen_full_json, snapshot
):
    assert generate_create_sql_ddl_statement(native_app_codegen_full_json) == snapshot


def test_generate_create_sql_ddl_statements_w_select_entries(
    native_app_codegen_full_json, snapshot
):
    native_app_codegen_full_json["replace"] = False
    native_app_codegen_full_json["all_imports"] = ""
    native_app_codegen_full_json["all_packages"] = ""
    native_app_codegen_full_json["external_access_integrations"] = None
    native_app_codegen_full_json["secrets"] = None
    native_app_codegen_full_json["execute_as"] = None
    native_app_codegen_full_json["inline_python_code"] = None
    assert generate_create_sql_ddl_statement(native_app_codegen_full_json) == snapshot


def test_generate_create_sql_ddl_statements_none():
    ex_fn = {
        "object_type": "PROCEDURE",
        "object_name": "CORE.MYFUNC",
        "anonymous": True,
    }
    assert generate_create_sql_ddl_statement(ex_fn=ex_fn) is None


# --------------------------------------------------------
# ------- generate_grant_sql_ddl_statements --------------
# --------------------------------------------------------


def test_generate_grant_sql_ddl_statements(snapshot):
    ex_fn = {
        "object_type": "TABLE_FUNCTION",
        "object_name": "CORE.MYFUNC",
        "application_roles": ["APP_ADMIN", "APP_VIEWER"],
    }
    assert generate_grant_sql_ddl_statements(ex_fn=ex_fn) == snapshot


def test_generate_grant_sql_ddl_statements_none():
    ex_fn = {"application_roles": None}
    assert generate_grant_sql_ddl_statements(ex_fn=ex_fn) is None
    ex_fn["application_roles"] = []
    assert generate_grant_sql_ddl_statements(ex_fn=ex_fn) is None


# --------------------------------------------------------
# ------------- SnowparkAnnotationProcessor --------------
# --------------------------------------------------------

default_dir_structure = {
    "a/b/c/main.py": "# this is a file\n",
    "a/b/c/d/main.py": "# this is a file\n",
    "a/b/c/main.txt": "# this is a file\n",
    "a/b/c/d/main.txt": "# this is a file\n",
    "a/b/main.py": "# this is a file\n",
    "a/b/main.txt": "# this is a file\n",
    "output/deploy": None,
    "output/deploy/stagepath/main.py": "# this is a file\n",
}

minimal_dir_structure = {
    "a/b/c/main.py": "# this is a file\n",
    "a/b/c/data.py": "# this is a file\n",
    "output/deploy": None,
    "output/deploy/stagepath/main.py": "# this is a file\n",
    "output/deploy/stagepath/data.py": "# this is a file\n",
}

# Test when exception is thrown while collecting information from callback
@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
    side_effect=SandboxExecutionError("dummy"),
)
def test_process_exception(mock_sandbox, native_app_project_instance):
    with temp_local_dir(default_dir_structure) as local_path:
        native_app_project_instance.native_app.artifacts = [
            {
                "src": "a/b/c/*.py",  # Will pick "a/b/c/main.py"
                "dest": "stagepath/",
                "processors": ["SNOWPARK"],
            }
        ]
        artifact_to_process = native_app_project_instance.native_app.artifacts[0]
        dest_file_py_file_to_ddl_map = SnowparkAnnotationProcessor(
            project_definition=native_app_project_instance,
            project_root=local_path,
            deploy_root=Path(local_path, "output/deploy"),
        ).process(
            artifact_to_process=artifact_to_process,
            processor_mapping=ProcessorMapping(name="SNOWPARK"),
        )
        assert len(dest_file_py_file_to_ddl_map) == 0


@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_generate_sql_ddl_statements_empty(mock_sandbox, native_app_project_instance):
    with temp_local_dir(minimal_dir_structure) as local_path:
        native_app_project_instance.native_app.artifacts = [
            {"src": "a/b/c/*.py", "dest": "stagepath/", "processors": ["SNOWPARK"]}
        ]
        mock_sandbox.side_effect = [None, []]
        dest_file_py_file_to_ddl_map = SnowparkAnnotationProcessor(
            project_definition=native_app_project_instance,
            project_root=local_path,
            deploy_root=Path(local_path, "output/deploy"),
        ).process(
            artifact_to_process=native_app_project_instance.native_app.artifacts[0],
            processor_mapping=ProcessorMapping(name="SNOWPARK"),
        )
        assert len(dest_file_py_file_to_ddl_map) == 0


@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_generate_sql_ddl_statements(
    mock_sandbox, native_app_project_instance, native_app_codegen_full_json, snapshot
):
    with temp_local_dir(minimal_dir_structure) as local_path:
        processor_mapping = ProcessorMapping(
            name="snowpark",
            properties={"env": {"type": "conda", "name": "snowpark-dev"}},
        )
        native_app_project_instance.native_app.artifacts = [
            {
                "src": "a/b/c/*.py",
                "dest": "stagepath/",
                "processors": [processor_mapping],
            }
        ]
        mock_sandbox.side_effect = [
            [native_app_codegen_full_json],
            [copy.deepcopy(native_app_codegen_full_json)],
        ]
        dest_file_py_file_to_ddl_map = SnowparkAnnotationProcessor(
            project_definition=native_app_project_instance,
            project_root=local_path,
            deploy_root=Path(local_path, "output/deploy"),
        ).process(
            artifact_to_process=native_app_project_instance.native_app.artifacts[0],
            processor_mapping=processor_mapping,
        )
        assert len(dest_file_py_file_to_ddl_map) == 2
        values = list(dest_file_py_file_to_ddl_map.values())
        assert values[0] == snapshot
        assert values[1] == snapshot


@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_generate_sql_ddl_statements_filtered_create(
    mock_sandbox, native_app_project_instance, native_app_codegen_full_json, snapshot
):
    with temp_local_dir(minimal_dir_structure) as local_path:
        native_app_project_instance.native_app.artifacts = [
            {"src": "a/b/c/*.py", "dest": "stagepath/", "processors": ["SNOWPARK"]}
        ]
        copy_instance = copy.deepcopy(native_app_codegen_full_json)
        copy_instance["object_type"] = "PROCEDURE"
        copy_instance["anonymous"] = True
        mock_sandbox.side_effect = [
            [native_app_codegen_full_json, None],
            [copy_instance, {}],
        ]

        dest_file_py_file_to_ddl_map = SnowparkAnnotationProcessor(
            project_definition=native_app_project_instance,
            project_root=local_path,
            deploy_root=Path(local_path, "output/deploy"),
        ).process(
            artifact_to_process=native_app_project_instance.native_app.artifacts[0],
            processor_mapping=ProcessorMapping(name="SNOWPARK"),
        )

        assert len(dest_file_py_file_to_ddl_map) == 1
        assert list(dest_file_py_file_to_ddl_map.values())[0] == snapshot
