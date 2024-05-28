from __future__ import annotations

import contextlib
import copy
import os
import subprocess
from pathlib import Path
from textwrap import dedent
from typing import Iterator, List, Set
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
    edit_setup_script_with_exec_imm_sql,
    generate_create_sql_ddl_statement,
    generate_grant_sql_ddl_statements,
)

from tests.testing_utils.files_and_dirs import temp_local_dir

PROJECT_ROOT = Path("/path/to/project")


# contextlib.chdir isn't available before Python 3.11, so this is an alternative for older versions
@contextlib.contextmanager
def in_cwd(path: Path) -> Iterator[None]:
    old_cwd = os.getcwd()
    os.chdir(str(path))
    try:
        yield
    except Exception:
        pass

    os.chdir(old_cwd)


def stringify(p: Path):
    if p.is_dir():
        return f"d {p}"
    else:
        return f"f {p}"


def all_paths_under_dir(root: Path) -> List[Path]:
    assert root.is_dir()

    paths: Set[Path] = set()
    for subdir, dirs, files in os.walk(root):
        subdir_path = Path(subdir)
        paths.add(subdir_path)
        for d in dirs:
            paths.add(subdir_path / d)
        for f in files:
            paths.add(subdir_path / f)

    return sorted(paths)


def assert_dir_snapshot(root: Path, snapshot) -> None:
    all_paths = all_paths_under_dir(root)

    # Verify the contents of the directory matches expectations
    assert "\n".join([stringify(p) for p in all_paths]) == snapshot

    # Verify that each file under the directory matches expectations
    for path in all_paths:
        if path.is_file():
            snapshot_contents = f"===== Contents of: {path} =====\n"
            snapshot_contents += path.read_text(encoding="utf-8")
            assert snapshot_contents == snapshot


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
# ------- generate_create_sql_ddl_statement -------------
# --------------------------------------------------------


def test_generate_create_sql_ddl_statements_w_all_entries(
    native_app_extension_function, snapshot
):
    assert generate_create_sql_ddl_statement(native_app_extension_function) == snapshot


def test_generate_create_sql_ddl_statements_w_select_entries(
    native_app_extension_function, snapshot
):
    native_app_extension_function.imports = None
    native_app_extension_function.packages = None
    native_app_extension_function.schema_name = None
    native_app_extension_function.secrets = None
    native_app_extension_function.external_access_integrations = None
    assert generate_create_sql_ddl_statement(native_app_extension_function) == snapshot


# --------------------------------------------------------
# ------- generate_grant_sql_ddl_statements --------------
# --------------------------------------------------------


def test_generate_grant_sql_ddl_statements(native_app_extension_function, snapshot):
    assert generate_grant_sql_ddl_statements(native_app_extension_function) == snapshot


# --------------------------------------------------------
# --------- edit_setup_script_with_exec_imm_sql ----------
# --------------------------------------------------------


def test_edit_setup_script_with_exec_imm_sql(snapshot):
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
        "output/deploy/__generated/moduleB/dummy.sql": "#this is a file",
        "output/deploy/__generated/dummy.sql": "#this is a file",
    }

    with temp_local_dir(dir_structure=dir_structure) as local_path:
        with in_cwd(local_path):
            deploy_root = Path(local_path, "output", "deploy")
            generated_root = Path(deploy_root, "__generated")
            collected_sql_files = [
                Path(generated_root, "moduleB", "dummy.sql"),
                Path(generated_root, "dummy.sql"),
            ]

            edit_setup_script_with_exec_imm_sql(
                collected_sql_files=collected_sql_files,
                deploy_root=deploy_root,
                generated_root=generated_root,
            )

            assert_dir_snapshot(deploy_root.relative_to(local_path), snapshot)


def test_edit_setup_script_with_exec_imm_sql_noop(snapshot):
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
        "output/deploy/__generated/__generated.sql": "#some text",
    }

    with temp_local_dir(dir_structure=dir_structure) as local_path:
        with in_cwd(local_path):
            deploy_root = Path(local_path, "output", "deploy")
            collected_sql_files = [
                Path(deploy_root, "__generated", "dummy.sql"),
            ]
            edit_setup_script_with_exec_imm_sql(
                collected_sql_files=collected_sql_files,
                deploy_root=deploy_root,
                generated_root=Path(deploy_root, "__generated"),
            )

            assert_dir_snapshot(deploy_root.relative_to(local_path), snapshot)


def test_edit_setup_script_with_exec_imm_sql_symlink(snapshot):
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
        with in_cwd(local_path):
            deploy_root = Path(local_path, "output", "deploy")

            deploy_root_setup_script = Path(deploy_root, "setup.sql")
            deploy_root_setup_script.symlink_to(Path(local_path, "setup.sql"))

            generated_root = Path(deploy_root, "__generated")
            collected_sql_files = [
                Path(generated_root, "moduleB", "dummy.sql"),
                Path(generated_root, "dummy.sql"),
            ]
            edit_setup_script_with_exec_imm_sql(
                collected_sql_files=collected_sql_files,
                deploy_root=deploy_root,
                generated_root=Path(deploy_root, "__generated"),
            )

            assert_dir_snapshot(deploy_root.relative_to(local_path), snapshot)


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
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_process_no_collected_functions(
    mock_sandbox, native_app_project_instance, snapshot
):
    with temp_local_dir(minimal_dir_structure) as local_path:
        with in_cwd(local_path):
            native_app_project_instance.native_app.artifacts = [
                {"src": "a/b/c/*.py", "dest": "stagepath/", "processors": ["SNOWPARK"]}
            ]
            mock_sandbox.side_effect = [None, []]
            deploy_root = Path(local_path, "output/deploy")
            generated_root = Path(deploy_root, "__generated")
            SnowparkAnnotationProcessor(
                project_definition=native_app_project_instance,
                project_root=local_path,
                deploy_root=deploy_root,
                generated_root=generated_root,
            ).process(
                artifact_to_process=native_app_project_instance.native_app.artifacts[0],
                processor_mapping=ProcessorMapping(name="SNOWPARK"),
                write_to_sql=False,  # For testing
            )

            assert_dir_snapshot(deploy_root.relative_to(local_path), snapshot)


@mock.patch(
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_process_with_collected_functions(
    mock_sandbox,
    native_app_project_instance,
    native_app_extension_function_raw_data,
    snapshot,
):

    with temp_local_dir(minimal_dir_structure) as local_path:
        with in_cwd(local_path):
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
            deploy_root = Path(local_path, "output/deploy")
            generated_root = Path(deploy_root, "__generated")
            SnowparkAnnotationProcessor(
                project_definition=native_app_project_instance,
                project_root=local_path,
                deploy_root=deploy_root,
                generated_root=generated_root,
            ).process(
                artifact_to_process=native_app_project_instance.native_app.artifacts[0],
                processor_mapping=processor_mapping,
            )

            assert_dir_snapshot(deploy_root.relative_to(local_path), snapshot)


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
    "snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor._execute_in_sandbox",
)
def test_package_normalization(
    mock_sandbox,
    package_decl,
    native_app_project_instance,
    native_app_extension_function_raw_data,
    snapshot,
):

    with temp_local_dir(minimal_dir_structure) as local_path:
        with in_cwd(local_path):
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
            deploy_root = Path(local_path, "output/deploy")
            generated_root = Path(deploy_root, "__generated")
            SnowparkAnnotationProcessor(
                project_definition=native_app_project_instance,
                project_root=local_path,
                deploy_root=deploy_root,
                generated_root=generated_root,
            ).process(
                artifact_to_process=native_app_project_instance.native_app.artifacts[0],
                processor_mapping=processor_mapping,
            )

            dest_file = generated_root / "stagepath" / "main.py"
            assert dest_file.is_file()
            assert dest_file.read_text(encoding="utf-8") == snapshot
