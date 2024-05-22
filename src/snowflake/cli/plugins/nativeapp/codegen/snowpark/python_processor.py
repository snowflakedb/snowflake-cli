from __future__ import annotations

import json
import pprint
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional

from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.api.utils.rendering import jinja_render_from_file
from snowflake.cli.plugins.nativeapp.artifacts import (
    BundleMap,
)
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import ArtifactProcessor
from snowflake.cli.plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    SandboxExecutionError,
    execute_script_in_sandbox,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils import (
    enrich_ex_fn,
    get_object_type_as_text,
    sanitize_extension_function_data,
)

DEFAULT_TIMEOUT = 30
TEMPLATE_PATH = Path(__file__).parent / "callback_source.py.jinja"


def _is_python_file(file_path: Path):
    """
    Checks if the given file is a python file.
    """
    return file_path.suffix == ".py"


def _determine_virtual_env(
    project_root: Path, processor: ProcessorMapping
) -> Dict[str, Any]:
    """
    Determines a virtual environment to run the Snowpark processor in, either through the project definition or by querying the current environment.
    """
    if (processor.properties is None) or ("env" not in processor.properties):
        return {}

    env_props = processor.properties["env"]
    env_type = env_props.get("type", None)

    if env_type is None:
        return {}

    if env_type.upper() == ExecutionEnvironmentType.CONDA.name:
        env_name = env_props.get("name", None)
        if env_name is None:
            cc.warning(
                "No name found in project definition file for the conda environment to run the Snowpark processor in. Will attempt to auto-detect the current conda environment."
            )
        return {"env_type": ExecutionEnvironmentType.CONDA, "name": env_name}
    elif env_type.upper() == ExecutionEnvironmentType.VENV.name:
        env_path_str = env_props.get("path", None)
        if env_path_str is None:
            cc.warning(
                "No path found in project definition file for the conda environment to run the Snowpark processor in. Will attempt to auto-detect the current venv path."
            )
            env_path = None
        else:
            env_path = Path(env_path_str)
            if not env_path.is_absolute():
                env_path = project_root / env_path
        return {
            "env_type": ExecutionEnvironmentType.VENV,
            "path": env_path,
        }
    elif env_type.upper() == ExecutionEnvironmentType.CURRENT.name:
        return {
            "env_type": ExecutionEnvironmentType.CURRENT,
        }
    return {}


def _execute_in_sandbox(
    py_file: str, deploy_root: Path, kwargs: Dict[str, Any]
) -> Optional[List[Dict[str, Any]]]:
    # Create the code snippet to be executed in the sandbox
    script_source = jinja_render_from_file(
        template_path=TEMPLATE_PATH, data={"py_file": py_file}
    )

    try:
        completed_process = execute_script_in_sandbox(
            script_source=script_source,
            cwd=deploy_root,
            timeout=DEFAULT_TIMEOUT,
            **kwargs,
        )
        cc.message(f"stdout: {completed_process.stdout}")
        cc.message(f"stderr: {completed_process.stderr}")
    except SandboxExecutionError as sdbx_err:
        cc.warning(
            f"Could not fetch Snowpark objects from {py_file} due to {sdbx_err}, continuing execution for the rest of the python files."
        )
        return None
    except Exception as err:
        cc.warning(
            f"Could not fetch Snowpark objects from {py_file} due to {err}, continuing execution for the rest of the python files."
        )
        return None

    if completed_process.returncode != 0:
        cc.warning(
            f"Could not fetch Snowpark objects from {py_file} due to the following error:\n {completed_process.stderr}"
        )
        cc.warning("Continuing execution for the rest of the python files.")
        return None

    try:
        return json.loads(completed_process.stdout)
    except Exception as exc:
        cc.warning(
            f"Could not load JSON into python due to the following exception: {exc}"
        )
        cc.warning(f"Continuing execution for the rest of the python files.")
        return None


class SnowparkAnnotationProcessor(ArtifactProcessor):
    """
    Built-in Processor to discover Snowpark-annotated objects in a given set of python files,
    and generate SQL code for creation of extension functions based on those discovered objects.
    """

    def __init__(
        self,
        project_definition: NativeApp,
        project_root: Path,
        deploy_root: Path,
    ):
        super().__init__(
            project_definition=project_definition,
            project_root=project_root,
            deploy_root=deploy_root,
        )
        self.project_definition = project_definition
        self.project_root = project_root
        self.deploy_root = deploy_root

    def process(
        self,
        artifact_to_process: PathMapping,
        processor_mapping: Optional[ProcessorMapping],
        **kwargs,
    ) -> Dict[Path, str]:
        """
        Intended to be the main method which can perform all relevant processing, and/or write to a target file, which depends on the type of processor.
        For SnowparkAnnotationProcessor, the target file is the setup script.
        """

        kwargs = (
            _determine_virtual_env(self.project_root, processor_mapping)
            if processor_mapping is not None
            else {}
        )

        # 1. Get the artifact src to dest mapping
        bundle_map = BundleMap(
            project_root=self.project_root, deploy_root=self.deploy_root
        )
        bundle_map.add(artifact_to_process)

        # 2. Get raw extension functions through Snowpark callback
        dest_file_py_file_to_collected_raw_ex_fns: Dict[Path, Optional[Any]] = {}

        def is_python_file_artifact(src: Path, dest: Path):
            return src.is_file() and src.suffix == ".py"

        for src_file, dest_file in bundle_map.all_mappings(
            absolute=True, expand_directories=True, predicate=is_python_file_artifact
        ):
            try:
                collected_raw_ex_fns = _execute_in_sandbox(
                    py_file=str(dest_file.resolve()),
                    deploy_root=self.deploy_root,
                    kwargs=kwargs,
                )
            except Exception as exc:
                cc.warning(
                    f"Error processing extension functions in {src_file}: {exc}"
                )  # Display the actual file for the user to inspect
                cc.warning("Skipping generating code of all objects from this file.")
                collected_raw_ex_fns = None

            if not collected_raw_ex_fns:
                continue

            cc.message(f"This is the file path in deploy root: {dest_file}\n")
            cc.message("This is the list of collected extension functions:")
            cc.message(pprint.pformat(collected_raw_ex_fns))

            filtered_collection = list(
                filter(
                    lambda item: (item is not None) and (len(item) > 0),
                    collected_raw_ex_fns,
                )
            )
            if len(filtered_collection) != len(collected_raw_ex_fns):
                cc.warning(
                    "Discovered extension functions that have value None or do not contain any information."
                )
                cc.warning(
                    "Skipping generating code of all such objects from this file."
                )

            # 4. Enrich the raw extension functions by setting additional properties
            for raw_ex_fn in filtered_collection:
                sanitize_extension_function_data(ex_fn=raw_ex_fn, py_file=dest_file)
                enrich_ex_fn(
                    ex_fn=raw_ex_fn,
                    py_file=dest_file,
                    deploy_root=self.deploy_root,
                )

            dest_file_py_file_to_collected_raw_ex_fns[dest_file] = filtered_collection

        # For each extension function, generate its related SQL statements
        dest_file_py_file_to_ddl_map: Dict[
            Path, str
        ] = self.generate_sql_ddl_statements(dest_file_py_file_to_collected_raw_ex_fns)

        # TODO: Temporary for testing, while feature is being built in phases
        return dest_file_py_file_to_ddl_map

    def generate_sql_ddl_statements(
        self, dest_file_py_file_to_collected_raw_ex_fns: Dict[Path, Optional[Any]]
    ) -> Dict[Path, str]:
        """
        Generates SQL DDL statements based on the entities collected from a set of python files in the artifact_to_process.
        """
        dest_file_py_file_to_ddl_map: Dict[Path, str] = {}
        for py_file in dest_file_py_file_to_collected_raw_ex_fns:

            collected_ex_fns = dest_file_py_file_to_collected_raw_ex_fns[
                py_file
            ]  # Collected entities is List[Dict[str, Any]]
            if collected_ex_fns is None:
                continue

            ddl_lst_per_ef: List[str] = []
            for ex_fn in collected_ex_fns:
                create_sql = generate_create_sql_ddl_statements(ex_fn)
                if create_sql:
                    ddl_lst_per_ef.append(create_sql)
                    grant_sql = generate_grant_sql_ddl_statements(ex_fn)
                    if grant_sql:
                        ddl_lst_per_ef.append(grant_sql)

            if len(ddl_lst_per_ef) > 0:
                dest_file_py_file_to_ddl_map[py_file] = "\n".join(ddl_lst_per_ef)

        return dest_file_py_file_to_ddl_map


def generate_create_sql_ddl_statements(ex_fn: Dict[str, Any]) -> Optional[str]:
    """
    Generates a "CREATE FUNCTION/PROCEDURE ... " SQL DDL statement based on a dictionary of extension function properties.
    Logic for this create statement has been lifted from snowflake-snowpark-python v1.15.0 package.
    Anonymous procedures are not allowed in Native Apps, and hence if a user passes in the two corresponding properties,
    this function will skip the DDL generation.
    """

    object_type = ex_fn["object_type"]
    object_name = ex_fn["object_name"]

    if object_type == "PROCEDURE" and ex_fn["anonymous"]:
        cc.warning(
            dedent(
                f"""{object_type.replace(' ', '-')} {object_name} cannot be an anonymous procedure in a Snowflake Native App.
                    Skipping generation of 'CREATE FUNCTION/PROCEDURE ...' SQL statement for this object."""
            )
        )
        return None

    replace_in_sql = f" OR REPLACE " if ex_fn["replace"] else ""

    sql_func_args = ",".join(
        [
            f"{a['name']} {t}"
            for a, t in zip(ex_fn["input_args"], ex_fn["input_sql_types"])
        ]
    )

    imports_in_sql = (
        f"\nIMPORTS=({ex_fn['all_imports']})" if ex_fn["all_imports"] else ""
    )

    packages_in_sql = (
        f"\nPACKAGES=({ex_fn['all_packages']})" if ex_fn["all_packages"] else ""
    )

    external_access_integrations = ex_fn["external_access_integrations"]
    external_access_integrations_in_sql = (
        f"""\nEXTERNAL_ACCESS_INTEGRATIONS=({','.join(external_access_integrations)})"""
        if external_access_integrations
        else ""
    )

    secrets = ex_fn["secrets"]
    secrets_in_sql = (
        f"""\nSECRETS=({",".join([f"'{k}'={v}" for k, v in secrets.items()])})"""
        if secrets
        else ""
    )

    execute_as = ex_fn["execute_as"]
    if execute_as is None:
        execute_as_sql = ""
    else:
        execute_as_sql = f"\nEXECUTE AS {execute_as}"

    inline_python_code = ex_fn["inline_python_code"]
    if inline_python_code:
        inline_python_code_in_sql = f"""\
AS $$
{inline_python_code}
$$
"""
    else:
        inline_python_code_in_sql = ""

    create_query = f"""\
CREATE{replace_in_sql}
{get_object_type_as_text(object_type)} {'IF NOT EXISTS' if ex_fn["if_not_exists"] else ''}{object_name}({sql_func_args})
{ex_fn["return_sql"]}
LANGUAGE PYTHON
RUNTIME_VERSION={ex_fn["runtime_version"]} {imports_in_sql}{packages_in_sql}{external_access_integrations_in_sql}{secrets_in_sql}
HANDLER='{ex_fn["handler"]}'{execute_as_sql}
{inline_python_code_in_sql}"""

    return create_query


def generate_grant_sql_ddl_statements(ex_fn: Dict[str, Any]) -> Optional[str]:
    """
    Generates a "GRANT USAGE TO ... " SQL DDL statement based on a dictionary of extension function properties.
    If no application roles are present, then the function returns None.
    """

    if ex_fn["application_roles"] is None:
        cc.warning(
            "Skipping generation of 'GRANT USAGE ON ...' SQL statement for this object due to lack of application roles."
        )
        return None

    grant_sql_statements = []
    for app_role in ex_fn["application_roles"]:
        grant_sql_statement = dedent(
            f"""\
            GRANT USAGE ON {get_object_type_as_text(ex_fn["object_type"])} {ex_fn["object_name"]}
            TO APPLICATION ROLE {app_role};
            """
        )
        grant_sql_statements.append(grant_sql_statement)

    if len(grant_sql_statements) == 0:
        return None
    return "\n".join(grant_sql_statements)
