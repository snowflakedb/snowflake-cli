# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import copy
import subprocess
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
from snowflake.cli._plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    SandboxExecutionError,
)
from snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
    _determine_virtual_env,
    _execute_in_sandbox,
    edit_setup_script_with_exec_imm_sql,
    generate_create_sql_ddl_statement,
    generate_grant_sql_ddl_statements,
)
from snowflake.cli.api.project.schemas.native_app.path_mapping import ProcessorMapping

from tests.nativeapp.utils import assert_dir_snapshot, create_native_app_project_model
from tests.testing_utils.files_and_dirs import pushd, temp_local_dir
from tests_common import IS_WINDOWS

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
    "snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor.execute_script_in_sandbox"
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
    "snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor.execute_script_in_sandbox"
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
# ------- generate_create_sql_ddl_statement -------------
# --------------------------------------------------------


def test_generate_create_sql_ddl_statements_w_all_entries(
    native_app_extension_function, os_agnostic_snapshot
):
    assert (
        generate_create_sql_ddl_statement(native_app_extension_function)
        == os_agnostic_snapshot
    )


def test_generate_create_sql_ddl_statements_w_select_entries(
    native_app_extension_function, os_agnostic_snapshot
):
    native_app_extension_function.imports = None
    native_app_extension_function.packages = None
    native_app_extension_function.schema_name = None
    native_app_extension_function.secrets = None
    native_app_extension_function.external_access_integrations = None
    assert (
        generate_create_sql_ddl_statement(native_app_extension_function)
        == os_agnostic_snapshot
    )


# --------------------------------------------------------
# ------- generate_grant_sql_ddl_statements --------------
# --------------------------------------------------------


def test_generate_grant_sql_ddl_statements(
    native_app_extension_function, os_agnostic_snapshot
):
    assert (
        generate_grant_sql_ddl_statements(native_app_extension_function)
        == os_agnostic_snapshot
    )


# --------------------------------------------------------
# --------- edit_setup_script_with_exec_imm_sql ----------
# --------------------------------------------------------


def test_edit_setup_script_with_exec_imm_sql(os_agnostic_snapshot):
    manifest_contents = dedent(
        f"""\
        manifest_version: 1

        artifacts:
            setup_script: moduleA/moduleC/setup.sql
        """
    )
    dir_structure = {
        "output/deploy/manifest.yml": manifest_contents,
        "output/deploy/moduleA/moduleC/setup.sql": "create application role app_public;",
        "output/deploy/__generated/snowpark/moduleB/dummy.sql": "#this is a file",
        "output/deploy/__generated/snowpark/dummy.sql": "#this is a file",
    }

    with temp_local_dir(dir_structure=dir_structure) as local_path:
        with pushd(local_path):
            deploy_root = Path(local_path, "output", "deploy")
            generated_root = Path(deploy_root, "__generated", "snowpark")
            collected_sql_files = [
                Path(generated_root, "moduleB", "dummy.sql"),
                Path(generated_root, "dummy.sql"),
            ]

            edit_setup_script_with_exec_imm_sql(
                collected_sql_files=collected_sql_files,
                deploy_root=deploy_root,
                generated_root=generated_root,
            )

            assert_dir_snapshot(
                deploy_root.relative_to(local_path), os_agnostic_snapshot
            )


def test_edit_setup_script_with_exec_imm_sql_noop(os_agnostic_snapshot):
    manifest_contents = dedent(
        f"""\
        manifest_version: 1

        artifacts:
            setup_script: moduleA/moduleC/setup.sql
        """
    )
    dir_structure = {
        "output/deploy/manifest.yml": manifest_contents,
        "output/deploy/moduleA/moduleC/setup.sql": None,
        "output/deploy/__generated/snowpark/__generated.sql": "#some text",
    }

    with temp_local_dir(dir_structure=dir_structure) as local_path:
        with pushd(local_path):
            deploy_root = Path(local_path, "output", "deploy")
            collected_sql_files = [
                Path(deploy_root, "__generated", "snowpark", "dummy.sql"),
            ]
            edit_setup_script_with_exec_imm_sql(
                collected_sql_files=collected_sql_files,
                deploy_root=deploy_root,
                generated_root=Path(deploy_root, "__generated", "snowpark"),
            )

            assert_dir_snapshot(
                deploy_root.relative_to(local_path), os_agnostic_snapshot
            )


@pytest.mark.skipif(
    IS_WINDOWS, reason="Symlinks on Windows are restricted to Developer mode or admins"
)
def test_edit_setup_script_with_exec_imm_sql_symlink(os_agnostic_snapshot):
    manifest_contents = dedent(
        f"""\
        manifest_version: 1

        artifacts:
            setup_script: setup.sql
        """
    )
    dir_structure = {
        "setup.sql": "create application role admin;",
        "output/deploy/manifest.yml": manifest_contents,
    }

    with temp_local_dir(dir_structure=dir_structure) as local_path:
        with pushd(local_path):
            deploy_root = Path(local_path, "output", "deploy")

            deploy_root_setup_script = Path(deploy_root, "setup.sql")
            deploy_root_setup_script.symlink_to(Path(local_path, "setup.sql"))

            generated_root = Path(deploy_root, "__generated", "snowpark")
            collected_sql_files = [
                Path(generated_root, "moduleB", "dummy.sql"),
                Path(generated_root, "dummy.sql"),
            ]
            edit_setup_script_with_exec_imm_sql(
                collected_sql_files=collected_sql_files,
                deploy_root=deploy_root,
                generated_root=Path(deploy_root, "__generated", "snowpark"),
            )

            assert_dir_snapshot(
                deploy_root.relative_to(local_path), os_agnostic_snapshot
            )


# --------------------------------------------------------
# ------------- SnowparkAnnotationProcessor --------------
# --------------------------------------------------------
manifest_contents = dedent(
    f"""\
    manifest_version: 1

    artifacts:
        setup_script: moduleA/moduleC/setup.sql
    """
)

minimal_dir_structure = {
    "a/b/c/main.py": "# this is a file\n",
    "a/b/c/data.py": "# this is a file\n",
    "output/deploy": None,
    "output/deploy/manifest.yml": manifest_contents,
    "output/deploy/moduleA/moduleC/setup.sql": "create application role app_public;",
    "output/deploy/stagepath/main.py": "# this is a file\n",
    "output/deploy/stagepath/data.py": "# this is a file\n",
    "output/deploy/stagepath/extra_import1.zip": None,
    "output/deploy/stagepath/extra_import2.zip": None,
}


@mock.patch(
    "snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_process_no_collected_functions(
    mock_sandbox, native_app_project_instance, os_agnostic_snapshot
):
    with temp_local_dir(minimal_dir_structure) as local_path:
        with pushd(local_path):
            native_app_project_instance.native_app.artifacts = [
                {"src": "a/b/c/*.py", "dest": "stagepath/", "processors": ["SNOWPARK"]}
            ]
            mock_sandbox.side_effect = [None, []]
            project = create_native_app_project_model(
                project_definition=native_app_project_instance.native_app,
                project_root=local_path,
            )
            processor = SnowparkAnnotationProcessor(project.get_bundle_context())
            processor.process(
                artifact_to_process=native_app_project_instance.native_app.artifacts[0],
                processor_mapping=ProcessorMapping(name="SNOWPARK"),
                write_to_sql=False,  # For testing
            )

            assert_dir_snapshot(
                project.deploy_root.relative_to(local_path), os_agnostic_snapshot
            )


@mock.patch(
    "snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_process_with_collected_functions(
    mock_sandbox,
    native_app_project_instance,
    native_app_extension_function_raw_data,
    os_agnostic_snapshot,
):

    with temp_local_dir(minimal_dir_structure) as local_path:
        with pushd(local_path):
            imports_variation = copy.deepcopy(native_app_extension_function_raw_data)
            imports_variation["imports"] = [
                "@dummy_stage_str",
                "/",
                "stagepath/extra_import1.zip",
                "stagepath/some_dir_str",
                "/stagepath/withslash.py",
                "stagepath/data.py",
            ]
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
                [native_app_extension_function_raw_data],
                [imports_variation],
            ]
            project = create_native_app_project_model(
                project_definition=native_app_project_instance.native_app,
                project_root=local_path,
            )
            project_context = project.get_bundle_context()
            processor_context = copy.copy(project_context)
            processor_context.generated_root = (
                project_context.generated_root / "snowpark"
            )
            processor_context.bundle_root = project_context.bundle_root / "snowpark"
            processor = SnowparkAnnotationProcessor(processor_context)
            processor.process(
                artifact_to_process=native_app_project_instance.native_app.artifacts[0],
                processor_mapping=processor_mapping,
            )

            assert_dir_snapshot(
                project.deploy_root.relative_to(local_path), os_agnostic_snapshot
            )


@pytest.mark.parametrize(
    "package_decl",
    [
        ["snowflake-snowpark-python"],
        ["'snowflake-snowpark-python'"],
        ["other-package", "'snowflake-snowpark-python'"],
        ["snowflake-snowpark-python==0.15.0"],
        ["'snowflake-snowpark-python==0.15.0'"],
        ["snowflake-snowpark-python<0.16.0"],
        ["snowflake-snowpark-python<=0.17.0"],
        ["snowflake-snowpark-python>=0.14.0"],
        ["snowflake-snowpark-python>0.13.0"],
        ["snowflake-snowpark-python===0.15.0"],
        ["snowflake-snowpark-python===0.15.0-rc1"],
        ["'snowflake-snowpark-python===0.15.0-rc1'"],
        ["snowflake-snowpark-python<<0.15.0"],  # not PEP 508 compliant
        ["snowflake-snowpark-python2"],  # not a match
        ["   snowflake-snowpark-python  ==  0.15.0   "],  # whitespace is ignored
    ],
)
@mock.patch(
    "snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_package_normalization(
    mock_sandbox,
    package_decl,
    native_app_project_instance,
    native_app_extension_function_raw_data,
    os_agnostic_snapshot,
):

    with temp_local_dir(minimal_dir_structure) as local_path:
        with pushd(local_path):
            processor_mapping = ProcessorMapping(
                name="snowpark",
            )
            native_app_project_instance.native_app.artifacts = [
                {
                    "src": "a/b/c/main.py",
                    "dest": "stagepath/",
                    "processors": [processor_mapping],
                }
            ]
            native_app_extension_function_raw_data["packages"] = package_decl
            mock_sandbox.side_effect = [[native_app_extension_function_raw_data]]
            project = create_native_app_project_model(
                project_definition=native_app_project_instance.native_app,
                project_root=local_path,
            )
            project_context = project.get_bundle_context()
            processor_context = copy.copy(project_context)
            processor_context.generated_root = (
                project_context.generated_root / "snowpark"
            )
            processor_context.bundle_root = project_context.bundle_root / "snowpark"
            processor = SnowparkAnnotationProcessor(processor_context)
            processor.process(
                artifact_to_process=native_app_project_instance.native_app.artifacts[0],
                processor_mapping=processor_mapping,
            )

            dest_file = project.generated_root / "snowpark" / "stagepath" / "main.sql"
            assert dest_file.is_file()
            assert dest_file.read_text(encoding="utf-8") == os_agnostic_snapshot
