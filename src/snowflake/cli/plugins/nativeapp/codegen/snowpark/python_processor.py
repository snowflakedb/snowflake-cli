import json
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from click import ClickException
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    Processor,
)
from snowflake.cli.plugins.nativeapp.artifacts import (
    is_glob,
    resolve_without_follow,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function import (
    ExtensionFunctionProperties,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.processor_base import (
    BuiltInAnnotationProcessor,
)
from snowflake.cli.plugins.nativeapp.utils import (
    get_all_file_paths_under_dir,
    get_all_python_files,
)


class VirtualEnvironmentType(Enum):
    AUTO_DETECT = 0
    VENV = 1
    CONDA = 2
    SYSTEM_DEFAULT = 3


def determine_virtual_env(processor: Processor) -> Dict[str, Any]:
    """
    Determines a virtual environment to run the Snowpark processor in, either through the project definition or by querying the current environment.
    """
    if "env" in processor.properties:
        env_type = processor.properties["env"]["type"]
        if env_type.lower() == "conda":
            if "name" not in processor.properties["env"]:
                cc.warning(
                    "Please provide a name for the conda environment to run the Snowpark processor in. Will auto-detect and use the currently active environment."
                )
            else:
                return {
                    "env_type": VirtualEnvironmentType.CONDA,
                    "name": processor.properties["env"]["name"],
                }
        elif env_type.lower() == "venv":
            if "path" not in processor.properties["env"]:
                cc.warning(
                    "Please provide a path for the venv to run the Snowpark processor in. Will auto-detect and use the currently active environment."
                )
            else:
                return {
                    "env_type": VirtualEnvironmentType.VENV,
                    "path": processor.properties["env"]["path"],
                }
    return {}


def execute_in_sandbox(py_file: str, kwargs: Dict[str, Any]):
    # Create the code snippet to be executed in the sandbox

    script_source = f"""
import functools
import sys

try:
    import snowflake.snowpark
except ModuleNotFoundError:
    sys.exit(1)

found_correct_version = (snowflake.snowpark.__version__ >= "1.15.0") and hasattr(snowflake.snowpark.context, "_is_execution_environment_sandboxed_for_client") and hasattr(snowflake.snowpark.context, "_should_continue_registration")

if not found_correct_version:
    sys.exit(1)

__SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_return_list = []

def __SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_replacement():
    global __SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_return_list

    def  __SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_transform_snowpark_object_to_json(extension_function_properties):

        from typing import Callable

        extension_function_dict = {{}}
        extension_function_dict["object_type"] = extension_function_properties.object_type.name
        extension_function_dict["object_name"] = extension_function_properties.object_name
        extension_function_dict["input_args"] = [{{"name": input_arg.name, "datatype": type(input_arg.datatype).__name__}} for input_arg in extension_function_properties.input_args]
        extension_function_dict["input_sql_types"] = extension_function_properties.input_sql_types
        extension_function_dict["return_sql"] = extension_function_properties.return_sql
        extension_function_dict["runtime_version"] = extension_function_properties.runtime_version
        extension_function_dict["all_imports"] = extension_function_properties.all_imports
        extension_function_dict["all_packages"] = extension_function_properties.all_packages
        extension_function_dict["handler"] = extension_function_properties.handler
        extension_function_dict["external_access_integrations"] = extension_function_properties.external_access_integrations
        extension_function_dict["secrets"] = extension_function_properties.secrets
        extension_function_dict["inline_python_code"] = extension_function_properties.inline_python_code
        extension_function_dict["native_app_params"] = extension_function_properties.native_app_params
        extension_function_dict["raw_imports"] = extension_function_properties.raw_imports
        extension_function_dict["replace"] = extension_function_properties.replace
        extension_function_dict["if_not_exists"] = extension_function_properties.if_not_exists
        extension_function_dict["execute_as"] = extension_function_properties.execute_as
        extension_function_dict["anonymous"] = extension_function_properties.anonymous
        raw_func = extension_function_properties.func
        extension_function_dict["func"] = raw_func.__name__ if isinstance(raw_func, Callable) else raw_func
        return extension_function_dict


    def __SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_append_to_list(callback_return_list, extension_function_properties):
        extension_function_dict = __SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_transform_snowpark_object_to_json(extension_function_properties)
        #TODO: print(extension_function_dict) here in case of downstream UDF creation failures
        callback_return_list.append(extension_function_dict)
        return False

    return functools.partial(__SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_append_to_list, __SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_return_list)

with open({py_file}, mode='r', encoding='utf-8') as udf_code:
    code = udf_code.read()


snowflake.snowpark.context._is_execution_environment_sandboxed_for_client = True
snowflake.snowpark.context._should_continue_registration = __SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_replacement()
snowflake.snowpark.session._is_execution_environment_sandboxed_for_client = True

# Remove reference to the callback from globals() since the function is already assigned to snowflake.snowpark.context._should_continue_registration
del __SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_replacement

try:
    exec(code, globals())
except Exception:
    sys.exit(10)

print(__SNOWFLAKE_CLI_NATIVE_APP_INTERNAL_callback_return_list)
    """

    completed_process = execute_script_in_sandbox(script_source=script_source, **kwargs)
    if completed_process.returncode != 0:
        raise ClickException(f"Unable to fetch Snowpark objects from {py_file}")

    json_data = json.loads(completed_process.stdout)
    return json_data


def execute_script_in_sandbox(
    script_source: str, env_type: Optional[VirtualEnvironmentType] = None, **kwargs
) -> Any:
    pass


def add_py_file_dest_to_dict(
    dest_path: Path,
    py_file: Path,
    src_py_file_to_dest_py_file_map: Dict[Path, Path],
    deploy_root: Path,
):
    dest_file = dest_path / py_file.name
    if dest_file.exists():
        src_py_file_to_dest_py_file_map[py_file] = dest_file
    else:
        cc.warning(f"{dest_path} does not exist in {deploy_root}.")


class SnowparkAnnotationProcessor(BuiltInAnnotationProcessor):
    """
    Built-in Processor to discover Snowpark-annotated objects in a given set of python files,
    and generate SQL code for creation of extension functions based on those discovered objects.
    """

    def __init__(
        self,
        project_definition: NativeApp,
        project_root: Path,
        deploy_root: Path,
        artifact_to_process: PathMapping,
        processor: Union[str, Processor],
    ):
        super().__init__()
        self.project_definition = project_definition
        self.project_root = project_root
        self.deploy_root = deploy_root
        self.artifact_to_process = artifact_to_process
        self.processor = processor

        self.kwargs = (
            determine_virtual_env(processor) if isinstance(processor, Processor) else {}
        )
        # raise ClickException("Could not find the required minimum version of snowflake-snowpark-python package in the currently active/specified virtual environment.")

    def process(self):
        """
        Intended to be the main method which can perform all relevant processing, and/or write to a target file, which depends on the type of processor.
        For SnowparkAnnotationProcessor, the target file is the setup script.
        """
        # 1. Get all src py files
        all_py_files_to_process = self.get_all_py_files_to_process()

        # 2. Get all src.py -> dest.py mapping
        src_py_file_to_dest_py_file_map = self.get_src_py_file_to_dest_py_file_map()

        # 3. Get entities through Snowpark callback
        src_py_file_to_collected_entities: Dict[Path, ExtensionFunctionProperties] = {}
        for py_file in all_py_files_to_process:
            try:
                collected_entities = execute_in_sandbox(
                    py_file=py_file.resolve(), kwargs=self.kwargs
                )
            except ClickException:
                cc.warning(
                    f"Error processing extension functions in {py_file}, skipping generating code of all objects from this file."
                )
                collected_entities = []
            src_py_file_to_collected_entities[py_file] = collected_entities

            # 4. Enrich entities by setting additional properties
            for entity in collected_entities:
                entity.set_additional_properties(
                    py_file=py_file, dest_file=src_py_file_to_dest_py_file_map[py_file]
                )

    def get_all_py_files_to_process(self) -> List[Path]:
        """
        For the project definition for a native app, find all python files in source that need to be consumed by the Snowpark Annotation Processor.
        """

        src_py_files: List[Path] = []
        artifact_src = self.artifact_to_process.src

        # Case 1: When artifact has the following src/dest pairing
        # src: john/doe/folder1/*.py OR john/doe/folder1/**/*.py
        # dest: stagepath/
        if is_glob(artifact_src) and artifact_src.endswith(".py"):
            src_py_files.extend(list(self.project_root.glob(artifact_src)))

        # Case 2: When artifact has the following src/dest pairing
        # src: john/doe/folder1/**/* (in this case, all files and directories under src need to be considered)
        # dest: stagepath/
        elif "**" in artifact_src:
            all_source_paths = list(self.project_root.glob(artifact_src))
            src_py_files.extend(get_all_python_files(all_source_paths))

        # Case 3: When artifact has the following src/dest pairing
        # src: john/doe/folder1/*
        # dest: stagepath/ (in this case, the directories under folder1 will be symlinked, which means files inside those directories also need to be considered due to implicit availability from directory symlink)
        elif is_glob(artifact_src):
            all_source_paths = list(self.project_root.glob(artifact_src))
            for path in all_source_paths:
                if path.is_dir():
                    all_files = get_all_file_paths_under_dir(path)
                    src_py_files.extend(get_all_python_files(all_files))
                elif path.is_file() and path.suffix == ".py":
                    src_py_files.append(path)

        # TODO: Unify Case 2 and Case 3 once symlinking "bugfix" is in.

        # Case 4: When artifact has the following src/dest pairing
        # src: john/doe/folder1/main.py
        # dest: stagepath/stagemain.py
        elif artifact_src.endswith(".py") and self.artifact_to_process.dest.endswith(
            ".py"
        ):
            src_py_files.append(Path(self.project_root, artifact_src))

        # Case 5: When artifact has the following src/dest pairing
        # src: john/doe/folder1.py.zip
        # dest: stagepath/stagefolder1.py.zip
        # TODO: Does this case 5 need to be considered?

        return src_py_files

    def get_src_py_file_to_dest_py_file_map(self) -> Dict[Path, Path]:
        """
        For the project definition for a native app, find the mapping between src python files and their destination python files.
        """

        src_py_file_to_dest_py_file_map: Dict[Path, Path] = {}
        artifact_src = self.artifact_to_process.src

        resolved_root = self.deploy_root.resolve()
        dest_path = resolve_without_follow(
            Path(resolved_root, self.artifact_to_process.dest)
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
            src_py_files = list(self.project_root.glob(artifact_src))
            for py_file in src_py_files:
                add_py_file_dest_to_dict(
                    dest_path=dest_path,
                    py_file=py_file,
                    src_py_file_to_dest_py_file_map=src_py_file_to_dest_py_file_map,
                    deploy_root=self.deploy_root,
                )

        # Case 3: When artifact has the following src/dest pairing
        # src: john/doe/folder1/*
        # dest: stagepath/ (in this case, the directories under folder1 will be symlinked, which means files inside those directories also need to be considered due to implicit availability from directory symlink)
        elif is_glob(artifact_src):
            src_files_and_dirs = list(self.project_root.glob(artifact_src))
            for path in src_files_and_dirs:
                if path.is_dir():
                    all_files = get_all_file_paths_under_dir(path)
                    all_py_files = get_all_python_files(all_files)
                    for py_file in all_py_files:
                        add_py_file_dest_to_dict(
                            dest_path=dest_path,
                            py_file=py_file,
                            src_py_file_to_dest_py_file_map=src_py_file_to_dest_py_file_map,
                            deploy_root=self.deploy_root,
                        )
                elif path.is_file() and path.suffix == ".py":
                    add_py_file_dest_to_dict(
                        dest_path=dest_path,
                        py_file=path,
                        src_py_file_to_dest_py_file_map=src_py_file_to_dest_py_file_map,
                        deploy_root=self.deploy_root,
                    )

        # TODO: Unify Case 2 and Case 3 once symlinking "bugfix" is in.

        # Case 4: When artifact has the following src/dest pairing
        # src: john/doe/folder1/main.py
        # dest: stagepath/stagemain.py
        elif artifact_src.endswith(".py") and self.artifact_to_process.dest.endswith(
            ".py"
        ):
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
