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

import json
import re
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Set

from pydantic import ValidationError
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.api.rendering.jinja import jinja_render_from_file
from snowflake.cli.plugins.nativeapp.artifacts import (
    BundleMap,
    find_setup_script_file,
)
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    is_python_file_artifact,
)
from snowflake.cli.plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    SandboxExecutionError,
    execute_script_in_sandbox,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils import (
    deannotate_module_source,
    ensure_all_string_literals,
    ensure_string_literal,
    get_function_type_signature_for_grant,
    get_qualified_object_name,
    get_sql_argument_signature,
    get_sql_object_type,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.models import (
    ExtensionFunctionTypeEnum,
    NativeAppExtensionFunction,
)
from snowflake.cli.plugins.nativeapp.project_model import NativeAppProjectModel
from snowflake.cli.plugins.stage.diff import to_stage_path

DEFAULT_TIMEOUT = 30
TEMPLATE_PATH = Path(__file__).parent / "callback_source.py.jinja"
SNOWPARK_LIB_NAME = "snowflake-snowpark-python"
SNOWPARK_LIB_REGEX = re.compile(
    # support PEP 508, even though not all of it is supported in Snowflake yet
    rf"'{SNOWPARK_LIB_NAME}\s*((<|<=|!=|==|>=|>|~=|===)\s*[a-zA-Z0-9_.*+!-]+)?'"
)
STAGE_PREFIX = "@"


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
        na_project: NativeAppProjectModel,
    ):
        super().__init__(na_project=na_project)

    def process(
        self,
        artifact_to_process: PathMapping,
        processor_mapping: Optional[ProcessorMapping],
        **kwargs,
    ) -> None:
        """
        Collects code annotations from Snowpark python files containing extension functions and augments the existing
        setup script with generated SQL that registers these functions.
        """

        bundle_map = BundleMap(
            project_root=self._na_project.project_root,
            deploy_root=self._na_project.deploy_root,
        )
        bundle_map.add(artifact_to_process)

        collected_extension_functions_by_path = self.collect_extension_functions(
            bundle_map, processor_mapping
        )

        collected_output = []
        collected_sql_files: List[Path] = []
        for py_file, extension_fns in sorted(
            collected_extension_functions_by_path.items()
        ):
            sql_file = self.generate_new_sql_file_name(
                py_file=py_file,
            )
            collected_sql_files.append(sql_file)
            insert_newline = False
            for extension_fn in extension_fns:
                create_stmt = generate_create_sql_ddl_statement(extension_fn)
                if create_stmt is None:
                    continue

                relative_py_file = py_file.relative_to(bundle_map.deploy_root())

                grant_statements = generate_grant_sql_ddl_statements(extension_fn)
                if grant_statements is not None:
                    collected_output.append(grant_statements)

                with open(sql_file, "a") as file:
                    if insert_newline:
                        file.write("\n")
                    insert_newline = True
                    file.write(
                        f"-- Generated by the Snowflake CLI from {relative_py_file}\n"
                    )
                    file.write(f"-- DO NOT EDIT\n")
                    file.write(create_stmt)
                    if grant_statements is not None:
                        file.write("\n")
                        file.write(grant_statements)

            self.deannotate(py_file, extension_fns)

        if collected_sql_files:
            edit_setup_script_with_exec_imm_sql(
                collected_sql_files=collected_sql_files,
                deploy_root=bundle_map.deploy_root(),
                generated_root=self._generated_root,
            )

    @property
    def _generated_root(self):
        return self._na_project.generated_root / "snowpark"

    def _normalize_imports(
        self,
        extension_fn: NativeAppExtensionFunction,
        py_file: Path,
        deploy_root: Path,
    ):
        normalized_imports: Set[str] = set()
        # Add the py_file, which is the source of the extension function
        normalized_imports.add(f"/{to_stage_path(py_file)}")

        for raw_import in extension_fn.imports:
            if not Path(deploy_root, raw_import).exists():
                # This should capture import_str of different forms: stagenames, malformed paths etc
                # But this will also return True if import_str == "/". Regardless, we append it all to normalized_imports
                cc.warning(
                    f"{raw_import} does not exist in the deploy root. Skipping validation of this import."
                )

            if raw_import.startswith(STAGE_PREFIX) or raw_import.startswith("/"):
                normalized_imports.add(raw_import)
            else:
                normalized_imports.add(f"/{to_stage_path(Path(raw_import))}")

        # To ensure order when running tests
        sorted_imports = list(normalized_imports)
        sorted_imports.sort()
        extension_fn.imports = sorted_imports

    def _normalize(
        self,
        extension_fn: NativeAppExtensionFunction,
        py_file: Path,
        deploy_root: Path,
    ):
        if extension_fn.name is None:
            # The extension function was not named explicitly, use the name of the Python function object as its name
            extension_fn.name = extension_fn.handler

        # Compute the fully qualified handler
        # If user defined their udf as @udf(lambda: x, ...) then extension_fn.handler is <lambda>.
        extension_fn.handler = f"{py_file.stem}.{extension_fn.handler}"

        extension_fn.packages = [
            self._normalize_package_name(pkg) for pkg in extension_fn.packages
        ]
        snowpark_lib_name = ensure_string_literal(SNOWPARK_LIB_NAME)
        if snowpark_lib_name not in extension_fn.packages:
            extension_fn.packages.append(snowpark_lib_name)

        if extension_fn.imports is None:
            extension_fn.imports = []
        self._normalize_imports(
            extension_fn=extension_fn,
            py_file=py_file,
            deploy_root=deploy_root,
        )

    def _normalize_package_name(self, pkg: str) -> str:
        """
        Returns a normalized version of the provided package name, as a Snowflake SQL string literal. Since the
        Snowpark library can sometimes add a spurious version to its own package name, we strip this here too so
        that the native application does not accidentally rely on stale packages once the snowpark library is updated
        in the cloud.

        Args:
            pkg (str): The package name to normalize.
        Returns:
            A normalized version of the package name, as a Snowflake SQL string literal.
        """
        normalized_package_name = ensure_string_literal(pkg.strip())
        if SNOWPARK_LIB_REGEX.fullmatch(normalized_package_name):
            return ensure_string_literal(SNOWPARK_LIB_NAME)
        return normalized_package_name

    def collect_extension_functions(
        self, bundle_map: BundleMap, processor_mapping: Optional[ProcessorMapping]
    ) -> Dict[Path, List[NativeAppExtensionFunction]]:
        kwargs = (
            _determine_virtual_env(self._na_project.project_root, processor_mapping)
            if processor_mapping is not None
            else {}
        )

        collected_extension_fns_by_path: Dict[
            Path, List[NativeAppExtensionFunction]
        ] = {}

        for src_file, dest_file in sorted(
            bundle_map.all_mappings(
                absolute=True,
                expand_directories=True,
                predicate=is_python_file_artifact,
            )
        ):
            cc.step(
                "Processing Snowpark annotations from {}".format(
                    dest_file.relative_to(bundle_map.deploy_root())
                )
            )
            collected_extension_function_json = _execute_in_sandbox(
                py_file=str(dest_file.resolve()),
                deploy_root=self._na_project.deploy_root,
                kwargs=kwargs,
            )

            if collected_extension_function_json is None:
                continue

            collected_extension_functions = []
            for extension_function_json in collected_extension_function_json:
                try:
                    extension_fn = NativeAppExtensionFunction(**extension_function_json)
                    self._normalize(
                        extension_fn,
                        py_file=dest_file.relative_to(bundle_map.deploy_root()),
                        deploy_root=bundle_map.deploy_root(),
                    )
                    collected_extension_functions.append(extension_fn)
                except ValidationError:
                    cc.warning("Invalid extension function definition")

            if collected_extension_functions:
                collected_extension_fns_by_path[
                    dest_file
                ] = collected_extension_functions

        return collected_extension_fns_by_path

    def generate_new_sql_file_name(self, py_file: Path) -> Path:
        """
        Generates a SQL filename for the generated root from the python file, and creates its parent directories.
        """
        relative_py_file = py_file.relative_to(self._na_project.deploy_root)
        sql_file = Path(self._generated_root, relative_py_file.with_suffix(".sql"))
        if sql_file.exists():
            cc.warning(
                f"""\
                File {sql_file} already exists, will append SQL statements to this file.
            """
            )
        sql_file.parent.mkdir(exist_ok=True, parents=True)
        return sql_file

    def deannotate(
        self, py_file: Path, extension_fns: List[NativeAppExtensionFunction]
    ):
        with open(py_file, "r", encoding="utf-8") as f:
            code = f.read()

        if py_file.is_symlink():
            # if the file is a symlink, make sure we don't overwrite the original
            py_file.unlink()

        new_code = deannotate_module_source(code, extension_fns)

        with open(py_file, "w", encoding="utf-8") as f:
            f.write(new_code)


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
        create_query += (
            f"\nIMPORTS=({', '.join(ensure_all_string_literals(extension_fn.imports))})"
        )

    if extension_fn.packages:
        create_query += f"\nPACKAGES=({', '.join(ensure_all_string_literals([pkg.strip() for pkg in extension_fn.packages]))})"

    if extension_fn.external_access_integrations:
        create_query += f"\nEXTERNAL_ACCESS_INTEGRATIONS=({', '.join(ensure_all_string_literals(extension_fn.external_access_integrations))})"

    if extension_fn.secrets:
        create_query += f"""\nSECRETS=({', '.join([f"{ensure_string_literal(k)}={v}" for k, v in extension_fn.secrets.items()])})"""

    create_query += f"\nHANDLER={ensure_string_literal(extension_fn.handler)}"

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
            f"Skipping generation of 'GRANT USAGE ON ...' SQL statement for {extension_fn.function_type.upper()} {extension_fn.handler} due to lack of application roles."
        )
        return None

    grant_sql_statements = []
    object_type = (
        "PROCEDURE"
        if extension_fn.function_type == ExtensionFunctionTypeEnum.PROCEDURE
        else "FUNCTION"
    )
    for app_role in extension_fn.application_roles:
        grant_sql_statement = dedent(
            f"""\
            GRANT USAGE ON {object_type} {get_qualified_object_name(extension_fn)}({get_function_type_signature_for_grant(extension_fn)})
            TO APPLICATION ROLE {app_role};
            """
        ).strip()
        grant_sql_statements.append(grant_sql_statement)

    return "\n".join(grant_sql_statements)


def edit_setup_script_with_exec_imm_sql(
    collected_sql_files: List[Path], deploy_root: Path, generated_root: Path
):
    """
    Adds an 'execute immediate' to setup script for every SQL file in the map
    """
    # Create a __generated.sql in the __generated folder
    generated_file_path = Path(generated_root, f"__generated.sql")
    generated_file_path.parent.mkdir(exist_ok=True, parents=True)

    if generated_file_path.exists():
        cc.warning(
            f"""\
            File {generated_file_path} already exists.
            Could not complete code generation of Snowpark Extension Functions.
            """
        )
        return

    # For every SQL file, add SQL statement 'execute immediate' to __generated.sql script.
    with open(generated_file_path, "a") as file:
        for sql_file in collected_sql_files:
            sql_file_relative_path = sql_file.relative_to(
                deploy_root
            )  # Path on stage, without the leading slash
            file.write(
                f"EXECUTE IMMEDIATE FROM '/{to_stage_path(sql_file_relative_path)}';\n"
            )

    # Find the setup script in the deploy root.
    setup_file_path = find_setup_script_file(deploy_root=deploy_root)
    with open(setup_file_path, "r", encoding="utf-8") as file:
        code = file.read()
    # Unlink to prevent over-writing source file
    if setup_file_path.is_symlink():
        setup_file_path.unlink()

    # Write original contents and the execute immediate sql to the setup script
    generated_file_relative_path = generated_file_path.relative_to(deploy_root)
    with open(setup_file_path, "w", encoding="utf-8") as file:
        file.write(code)
        file.write(
            f"\nEXECUTE IMMEDIATE FROM '/{to_stage_path(generated_file_relative_path)}';"
        )
        file.write(f"\n")
