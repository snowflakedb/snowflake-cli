from __future__ import annotations

import json
import pprint
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional

from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.errors import SchemaValidationError
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
    get_qualified_object_name,
    get_sql_argument_signature,
    get_sql_object_type,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.models import (
    ExtensionFunctionTypeEnum,
    NativeAppExtensionFunction,
)
from snowflake.cli.plugins.stage.diff import to_stage_path

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


def _is_python_file_artifact(src: Path, dest: Path):
    return src.is_file() and src.suffix == ".py"


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
    ) -> str:  # String output is temporary until we have better e2e testing mechanism
        """
        Collects code annotations from Snowpark python files containing extension functions and augments the existing
        setup script with generated SQL that registers these functions.
        """

        bundle_map = BundleMap(
            project_root=self.project_root, deploy_root=self.deploy_root
        )
        bundle_map.add(artifact_to_process)

        collected_extension_functions_by_path = self.collect_extension_functions(
            bundle_map, processor_mapping
        )

        collected_output = []
        for py_file, extension_fns in collected_extension_functions_by_path.items():
            for extension_fn in extension_fns:
                create_stmt = generate_create_sql_ddl_statement(extension_fn)
                if create_stmt is None:
                    continue

                cc.message(
                    "-- Generating Snowpark annotation SQL code for {}".format(py_file)
                )
                cc.message(create_stmt)
                collected_output.append(
                    f"-- {py_file.relative_to(bundle_map.deploy_root())}"
                )
                collected_output.append(create_stmt)

                grant_statements = generate_grant_sql_ddl_statements(extension_fn)
                if grant_statements is not None:
                    cc.message(grant_statements)
                    collected_output.append(grant_statements)

        return "\n".join(collected_output)

    def _normalize(self, extension_fn: NativeAppExtensionFunction, py_file: Path):
        if extension_fn.name is None:
            # The extension function was not named explicitly, use the name of the Python function object as its name
            extension_fn.name = extension_fn.handler

        # Compute the fully qualified handler
        extension_fn.handler = f"{py_file.stem}.{extension_fn.handler}"

        if extension_fn.imports is None:
            extension_fn.imports = []
        extension_fn.imports.append(f"/{to_stage_path(py_file)}")

    def collect_extension_functions(
        self, bundle_map: BundleMap, processor_mapping: Optional[ProcessorMapping]
    ) -> Dict[Path, List[NativeAppExtensionFunction]]:
        kwargs = (
            _determine_virtual_env(self.project_root, processor_mapping)
            if processor_mapping is not None
            else {}
        )

        collected_extension_fns_by_path: Dict[
            Path, List[NativeAppExtensionFunction]
        ] = {}

        for src_file, dest_file in bundle_map.all_mappings(
            absolute=True, expand_directories=True, predicate=_is_python_file_artifact
        ):
            collected_extension_function_json = _execute_in_sandbox(
                py_file=str(dest_file.resolve()),
                deploy_root=self.deploy_root,
                kwargs=kwargs,
            )

            if collected_extension_function_json is None:
                cc.warning(f"Error processing extension functions in {src_file}")
                cc.warning("Skipping generating code of all objects from this file.")
                continue

            collected_extension_functions = []
            for extension_function_json in collected_extension_function_json:
                try:
                    extension_fn = NativeAppExtensionFunction(**extension_function_json)
                    self._normalize(
                        extension_fn,
                        py_file=dest_file.relative_to(bundle_map.deploy_root()),
                    )
                    collected_extension_functions.append(extension_fn)
                except SchemaValidationError:
                    cc.warning("Invalid extension function definition")

            if collected_extension_functions:
                cc.message(f"This is the file path in deploy root: {dest_file}\n")
                cc.message("This is the list of collected extension functions:")
                cc.message(pprint.pformat(collected_extension_functions))

                collected_extension_fns_by_path[
                    dest_file
                ] = collected_extension_functions

        return collected_extension_fns_by_path


def generate_create_sql_ddl_statement(
    extension_fn: NativeAppExtensionFunction,
) -> Optional[str]:
    """
    Generates a "CREATE FUNCTION/PROCEDURE ... " SQL DDL statement based on an extension function definition.
    Logic for this create statement has been lifted from snowflake-snowpark-python v1.15.0 package.
    """

    object_type = get_sql_object_type(extension_fn)
    if object_type is None:
        cc.warning(f"Unsupported extension function type: {extension_fn.function_type}")
        return None

    arguments_in_sql = ", ".join(
        [get_sql_argument_signature(arg) for arg in extension_fn.signature]
    )

    create_query = dedent(
        f"""
               CREATE OR REPLACE
               {object_type} {get_qualified_object_name(extension_fn)}({arguments_in_sql})
               RETURNS {extension_fn.returns}
               LANGUAGE PYTHON
               RUNTIME_VERSION={extension_fn.runtime}
    """
    ).strip()

    if extension_fn.imports:
        create_query += f"\nIMPORTS=({extension_fn.imports})"

    if extension_fn.packages:
        create_query += f"\nPACKAGES=({', '.join(extension_fn.packages)})"

    if extension_fn.external_access_integrations:
        create_query += f"\nEXTERNAL_ACCESS_INTEGRATIONS=({', '.join(extension_fn.external_access_integrations)})"

    if extension_fn.secrets:
        create_query += f"""\nSECRETS=({', '.join([f"'{k}'={v}" for k, v in extension_fn.secrets.items()])})"""

    create_query += f"\nHANDLER='{extension_fn.handler}'"

    if extension_fn.function_type == ExtensionFunctionTypeEnum.PROCEDURE:
        if extension_fn.execute_as_caller:
            create_query += f"\nEXECUTE AS CALLER"
        else:
            create_query += f"\nEXECUTE AS OWNER"
    create_query += ";\n"

    return create_query


def generate_grant_sql_ddl_statements(
    extension_fn: NativeAppExtensionFunction,
) -> Optional[str]:
    """
    Generates a "GRANT USAGE TO ... " SQL DDL statement based on a dictionary of extension function properties.
    If no application roles are present, then the function returns None.
    """

    if not extension_fn.application_roles:
        cc.warning(
            "Skipping generation of 'GRANT USAGE ON ...' SQL statement for this object due to lack of application roles."
        )
        return None

    grant_sql_statements = []
    for app_role in extension_fn.application_roles:
        grant_sql_statement = dedent(
            f"""\
            GRANT USAGE ON {get_sql_object_type(extension_fn)} {get_qualified_object_name(extension_fn)}
            TO APPLICATION ROLE {app_role};
            """
        ).strip()
        grant_sql_statements.append(grant_sql_statement)

    return "\n".join(grant_sql_statements)
