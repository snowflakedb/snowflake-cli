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
    is_glob,
    resolve_without_follow,
)
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import ArtifactProcessor
from snowflake.cli.plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    SandboxExecutionError,
    execute_script_in_sandbox,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils import (
    _enrich_entity,
    _get_object_type_as_text,
)
from snowflake.cli.plugins.nativeapp.utils import (
    filter_files,
    get_all_file_paths_under_dir,
)

DEFAULT_TIMEOUT = 30


def is_python_file(file_path: Path):
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


TEMPLATE_PATH = Path(__file__).parent / "callback_source.py.jinja"


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


def _add_py_file_dest_to_dict(
    dest_path: Path,
    py_file: Path,
    src_py_file_to_dest_py_file_map: Dict[Path, Path],
    deploy_root: Path,
):
    dest_file = dest_path / py_file.name
    if (
        dest_file.exists()
    ):  # Should already exist since bundle is called before processing begins
        src_py_file_to_dest_py_file_map[py_file] = dest_file
    else:
        cc.warning(f"{dest_path} does not exist in {deploy_root}.")


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
    ) -> Dict[Path, Optional[Any]]:
        """
        Intended to be the main method which can perform all relevant processing, and/or write to a target file, which depends on the type of processor.
        For SnowparkAnnotationProcessor, the target file is the setup script.
        """

        kwargs = (
            _determine_virtual_env(self.project_root, processor_mapping)
            if processor_mapping is not None
            else {}
        )

        # 1. Get all src.py -> dest.py mapping
        # TODO: Logic to replaced in a follow up PR by NADE
        src_py_file_to_dest_py_file_map = self.get_src_py_file_to_dest_py_file_map(
            artifact_to_process
        )

        # 2. Get entities through Snowpark callback
        dest_file_py_file_to_collected_entities: Dict[Path, Optional[Any]] = {}
        for src_file, dest_file in src_py_file_to_dest_py_file_map.items():
            if dest_file.suffix == ".py":
                try:
                    collected_entities = _execute_in_sandbox(
                        py_file=str(dest_file.resolve()),
                        deploy_root=self.deploy_root,
                        kwargs=kwargs,
                    )
                except Exception as exc:
                    cc.warning(
                        f"Error processing extension functions in {src_file}: {exc}"
                    )  # Display the actual file for the user to inspect
                    cc.warning(
                        "Skipping generating code of all objects from this file."
                    )
                    collected_entities = None

                dest_file_py_file_to_collected_entities[dest_file] = collected_entities

                if collected_entities is None:
                    continue

                cc.message(f"This is the file path in deploy root: {dest_file}\n")
                cc.message("This is the list of collected entities:")
                cc.message(pprint.pformat(collected_entities))

                # 4. Enrich entities by setting additional properties
                for entity in collected_entities:
                    _enrich_entity(
                        entity=entity,
                        py_file=dest_file,
                        deploy_root=self.deploy_root,
                        suffix_str=".py",
                    )

        dest_file_py_file_to_ddl_map = self.generate_sql_ddl_statements(
            dest_file_py_file_to_collected_entities
        )

        # TODO: Temporary for testing, while feature is being built in phases
        return dest_file_py_file_to_ddl_map

    def get_src_py_file_to_dest_py_file_map(
        self,
        artifact_to_process: PathMapping,
    ) -> Dict[Path, Path]:
        """
        For the project definition for a native app, find the mapping between src python files and their destination python files.
        """

        src_py_file_to_dest_py_file_map: Dict[Path, Path] = {}
        artifact_src = artifact_to_process.src

        resolved_root = self.deploy_root.resolve()
        dest_path = resolve_without_follow(
            Path(resolved_root, artifact_to_process.dest)
        )

        # Case 1: When artifact has the following src/dest pairing
        # src: john/doe/folder1/*.py OR john/doe/folder1/**/*.py
        # dest: stagepath/
        # OR
        # Case 2: When artifact has the following src/dest pairing
        # src: john/doe/folder1/**/* (in this case, all files and directories under src need to be considered)
        # dest: stagepath/
        if (is_glob(artifact_src) and artifact_src.endswith(".py")) or (
            "**" in artifact_src
        ):
            src_files_gen = self.project_root.glob(artifact_src)
            src_py_files_gen = filter_files(
                generator=src_files_gen, predicate_func=is_python_file
            )
            for py_file in src_py_files_gen:
                _add_py_file_dest_to_dict(
                    dest_path=dest_path,
                    py_file=py_file,
                    src_py_file_to_dest_py_file_map=src_py_file_to_dest_py_file_map,
                    deploy_root=self.deploy_root,
                )

        # Case 3: When artifact has the following src/dest pairing
        # src: john/doe/folder1/*
        # dest: stagepath/ (in this case, the directories under folder1 will be symlinked, which means files inside those directories also need to be considered due to implicit availability from directory symlink)
        elif is_glob(artifact_src):
            src_files_and_dirs = self.project_root.glob(artifact_src)
            for path in src_files_and_dirs:
                if path.is_dir():
                    file_gen = get_all_file_paths_under_dir(path)
                    py_file_gen = filter_files(
                        generator=file_gen, predicate_func=is_python_file
                    )
                    for py_file in py_file_gen:
                        _add_py_file_dest_to_dict(
                            dest_path=dest_path,
                            py_file=py_file,
                            src_py_file_to_dest_py_file_map=src_py_file_to_dest_py_file_map,
                            deploy_root=self.deploy_root,
                        )
                elif path.is_file() and path.suffix == ".py":
                    _add_py_file_dest_to_dict(
                        dest_path=dest_path,
                        py_file=path,
                        src_py_file_to_dest_py_file_map=src_py_file_to_dest_py_file_map,
                        deploy_root=self.deploy_root,
                    )

        # TODO: Unify Case 2 and Case 3 once symlinking "bugfix" is in.

        # Case 4: When artifact has the following src/dest pairing
        # src: john/doe/folder1/main.py
        # dest: stagepath/stagemain.py
        elif artifact_src.endswith(".py") and artifact_to_process.dest.endswith(".py"):
            if dest_path.exists():
                src_py_file_to_dest_py_file_map[
                    Path(self.project_root, artifact_src)
                ] = dest_path
            else:
                cc.warning(f"{dest_path} does not exist in {self.deploy_root}.")

        # Case 5: When artifact has the following src/dest pairing
        # src: john/doe/folder1.py.zip
        # dest: stagepath/stagefolder1.py.zip
        # TODO: Does this case 5 need to be considered?

        return src_py_file_to_dest_py_file_map

    def generate_sql_ddl_statements(
        self, dest_file_py_file_to_collected_entities: Dict[Path, Optional[Any]]
    ) -> Dict[Path, str]:
        """
        Generates SQL DDL statements based on the entities collected from a set of python files in the artifact_to_process.
        """
        dest_file_py_file_to_ddl_map: Dict[Path, str] = {}
        for py_file in dest_file_py_file_to_collected_entities:

            collected_entities = dest_file_py_file_to_collected_entities[
                py_file
            ]  # Collected entities is List[Dict[str, Any]]
            if collected_entities is None:
                continue

            ddl_lst_per_ef: List[str] = []
            for extension_function in collected_entities:
                can_proceed = add_defaults_to_extension_function(extension_function)
                if not can_proceed:
                    cc.warning(
                        f"Skipping generation of 'CREATE FUNCTION/PROCEDURE ...' SQL statement for this object."
                    )
                    continue
                ddl_lst_per_ef.append(
                    generate_create_sql_ddl_statements(extension_function)
                )
                ddl_lst_per_ef.append(
                    generate_grant_sql_ddl_statements(extension_function)
                )

            dest_file_py_file_to_ddl_map[py_file] = "\n".join(ddl_lst_per_ef)

        return dest_file_py_file_to_ddl_map


def add_defaults_to_extension_function(ex_fn: Dict[str, Any]) -> bool:
    """
    Helper function to add in defaults for keys that do not exist in the dictionary or contain empty strings when they should not.
    This helper function is needed because different callback sources can create dictionaries with different/missing keys, and since
    Snowflake CLI may not own all callback implementations, the dictionaries need to have the minimum set of keys and their default
    values to be used in creation of the SQL DDL statements.

    Returns:
    A boolean value, True if everything has been successfully validated and assigned, False if an error was encountered.
    """

    # Must have keys
    try:
        ex_fn["object_type"] = ex_fn["object_type"].upper()
        assert ex_fn["object_type"] is not ""
        ex_fn["object_name"] = ex_fn["object_name"].upper()
        assert ex_fn["object_name"] is not ""
        ex_fn["return_sql"] = ex_fn["return_sql"].upper()
        assert ex_fn["return_sql"] is not ""
    except KeyError as err:
        cc.warning(f"{err}")
        return False
    except AssertionError as err:
        cc.warning(f"{err}")
        return False

    # Other optional keys
    ex_fn["anonymous"] = ex_fn.get("anonymous", False)
    ex_fn["replace"] = ex_fn.get("replace", False)
    ex_fn["if_not_exists"] = ex_fn.get("if_not_exists", False)

    if ex_fn["replace"] and ex_fn["if_not_exists"]:
        cc.warning("Options 'replace' and 'if_not_exists' are incompatible.")
        return False

    try:
        assert len(ex_fn["input_args"]) == len(ex_fn["input_sql_types"])
    except AssertionError as err:
        cc.warning(
            "The number of function parameters does not match the number of parameter types."
        )
        return False

    has_imports = ex_fn.get("all_imports", None) and len(ex_fn["all_imports"]) > 0
    ex_fn["all_imports"] = ex_fn["all_imports"] if has_imports else None

    has_packages = ex_fn.get("all_packages", None) and len(ex_fn["all_packages"]) > 0
    ex_fn["all_packages"] = ex_fn["all_packages"] if has_packages else None

    has_eai = (
        ex_fn.get("external_access_integrations", None)
        and len(ex_fn["external_access_integrations"]) > 0
    )
    ex_fn["external_access_integrations"] = (
        ex_fn["external_access_integrations"] if has_eai else None
    )

    has_secrets = ex_fn.get("secrets", None) and len(ex_fn["secrets"]) > 0
    ex_fn["secrets"] = ex_fn["secrets"] if has_secrets else None

    has_execute_as = ex_fn.get("execute_as", None) and len(ex_fn["execute_as"]) > 0
    ex_fn["execute_as"] = ex_fn["execute_as"].upper() if has_execute_as else None

    has_inline_code = (
        ex_fn["inline_python_code"] and len(ex_fn["inline_python_code"]) > 0
    )
    ex_fn["inline_python_code"] = (
        ex_fn["inline_python_code"] if has_inline_code else None
    )

    # Cannot use KeyError check has only Java, Python and Scala need this value
    has_runtime_version = ex_fn["runtime_version"] and len(ex_fn["runtime_version"]) > 0
    ex_fn["runtime_version"] = ex_fn["runtime_version"] if has_runtime_version else None

    # Cannot use KeyError check has only Java, Python and Scala need this value
    has_handler = ex_fn["handler"] and len(ex_fn["handler"]) > 0
    ex_fn["handler"] = ex_fn["handler"] if has_handler else None

    has_app_roles = (
        ex_fn.get("application_roles", None) and len(ex_fn["application_roles"]) > 0
    )
    ex_fn["application_roles"] = (
        [app_role.upper() for app_role in ex_fn["application_roles"]]
        if has_app_roles
        else None
    )

    return True


def generate_create_sql_ddl_statements(ex_fn: Dict[str, Any]) -> str:
    """
    Generates a "CREATE FUNCTION/PROCEDURE ... " SQL DDL statement based on a dictionary of extension function properties.
    Logic for this create statement has been lifted from snowflake-snowpark-python v1.15.0 package.
    """

    object_type = ex_fn["object_type"]
    object_name = ex_fn["object_name"]

    if object_type == "PROCEDURE" and ex_fn.get("anonymous", False):
        cc.warning(
            dedent(
                f"""{object_type.replace(' ', '-')} {object_name} cannot be an anonymous procedure in a Snowflake Native App.
                    Skipping generation of 'CREATE FUNCTION/PROCEDURE ...' SQL statement for this object."""
            )
        )
        return ""

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
        f"""\nSECRETS=({",".join([f"'{k}'={v}" for k, v in secrets.items()])})\n"""
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

    try:
        assert ex_fn["runtime_version"] is not None
        assert ex_fn["handler"] is not None
    except AssertionError as err:
        cc.warning(f"Could not generate SQL DDL due to incorrect information:\n{err}")
        return ""

    create_query = dedent(
        f"""\
CREATE{replace_in_sql}
{_get_object_type_as_text(object_type)} {'IF NOT EXISTS' if ex_fn["if_not_exists"] else ''}{object_name}({sql_func_args})
{ex_fn["return_sql"]}
LANGUAGE PYTHON
RUNTIME_VERSION={ex_fn["runtime_version"]} {imports_in_sql}{packages_in_sql}{external_access_integrations_in_sql}{secrets_in_sql}
HANDLER='{ex_fn["handler"]}'{execute_as_sql}
{inline_python_code_in_sql}"""
    )

    return create_query


def generate_grant_sql_ddl_statements(ex_fn: Dict[str, Any]) -> str:
    """
    Generates a "GRANT USAGE TO ... " SQL DDL statement based on a dictionary of extension function properties.
    """

    if ex_fn["application_roles"] is None:
        cc.warning(
            "Skipping generation of 'GRANT USAGE ON ...' SQL statement for this object due to lack of application roles."
        )
        return ""

    grant_sql_statements = []
    for app_role in ex_fn["application_roles"]:
        grant_sql_statement = dedent(
            f"""\
            GRANT USAGE ON {_get_object_type_as_text(ex_fn["object_type"])} {ex_fn["object_name"]}
            TO APPLICATION ROLE {app_role};
            """
        )
        grant_sql_statements.append(grant_sql_statement)
    return "\n".join(grant_sql_statements)
