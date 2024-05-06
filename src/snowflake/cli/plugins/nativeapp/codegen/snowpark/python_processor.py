import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from click import ClickException
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    Processor,
)
from snowflake.cli.api.utils.rendering import jinja_render_from_file
from snowflake.cli.plugins.nativeapp.artifacts import (
    is_glob,
    resolve_without_follow,
)
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    MissingProjectDefinitionPropertyError,
)
from snowflake.cli.plugins.nativeapp.codegen.sandbox import (
    ExecutionEnvironmentType,
    SandboxExecutionError,
    execute_script_in_sandbox,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.extension_function_utils import (
    _enrich_entity,
)
from snowflake.cli.plugins.nativeapp.utils import (
    get_all_file_paths_under_dir,
    get_all_python_files,
)

DEFAULT_TIMEOUT = 30


def _determine_virtual_env(processor: Processor) -> Dict[str, Any]:
    """
    Determines a virtual environment to run the Snowpark processor in, either through the project definition or by querying the current environment.
    """
    if "env" not in processor.properties:
        return {}

    env_props = processor.properties["env"]
    env_type = env_props.get("type", None)

    if env_type is None:
        return {}

    if env_type.upper() == ExecutionEnvironmentType.CONDA.name:
        env_name = env_props.get("name", None)
        if env_name is None:
            raise MissingProjectDefinitionPropertyError(
                "No name found in project definition file for the conda environment to run the Snowpark processor in. Will attempt to auto-detect the current conda environment."
            )
        return {"env_type": ExecutionEnvironmentType.CONDA, "name": env_name}
    elif env_type.lower() == ExecutionEnvironmentType.VENV.name:
        env_path = env_props.get("path", None)
        if env_path is None:
            raise MissingProjectDefinitionPropertyError(
                "No path found in project definition file for the conda environment to run the Snowpark processor in. Will attempt to auto-detect the current venv path."
            )
        return {
            "env_type": ExecutionEnvironmentType.VENV,
            "path": env_path,
        }
    elif env_type.lower() == ExecutionEnvironmentType.CURRENT.name:
        return {
            "env_type": ExecutionEnvironmentType.CURRENT,
        }
    return {}


def _execute_in_sandbox(
    py_file: str, deploy_root: Path, kwargs: Dict[str, Any]
) -> Optional[List[Dict[str, Any]]]:
    # Create the code snippet to be executed in the sandbox
    script_source = jinja_render_from_file(
        template_path=Path("./callback_source.py.jinja"), data={"py_file": py_file}
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

    try:
        return json.loads(completed_process.stdout)
    except Exception as exc:
        cc.warning(
            f"Could not load JSON into python due to the following exception: {exc}"
        )
        cc.warning(f"Continuing execution for the rest of the python files.")
        return []


def _add_py_file_dest_to_dict(
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
            _determine_virtual_env(processor)
            if isinstance(processor, Processor)
            else {}
        )
        # raise ClickException("Could not find the required minimum version of snowflake-snowpark-python package in the currently active/specified virtual environment.")

    def process(self):
        """
        Intended to be the main method which can perform all relevant processing, and/or write to a target file, which depends on the type of processor.
        For SnowparkAnnotationProcessor, the target file is the setup script.
        """
        # TODO: replace 1 and 2 with Guy's logic
        # 1. Get all src py files
        all_py_files_to_process = self.get_all_py_files_to_process()

        # 2. Get all src.py -> dest.py mapping
        src_py_file_to_dest_py_file_map = self.get_src_py_file_to_dest_py_file_map()

        # 3. Get entities through Snowpark callback
        src_py_file_to_collected_entities: Dict[Path, Optional[Any]] = {}
        for py_file in all_py_files_to_process:
            try:
                collected_entities = _execute_in_sandbox(
                    py_file=py_file.resolve(), kwargs=self.kwargs
                )
            except ClickException:
                cc.warning(
                    f"Error processing extension functions in {py_file}, skipping generating code of all objects from this file."
                )
            src_py_file_to_collected_entities[py_file] = collected_entities

            if collected_entities is None:
                continue

            # 4. Enrich entities by setting additional properties
            for entity in collected_entities:
                _enrich_entity(
                    entity=entity, py_file=src_py_file_to_dest_py_file_map[py_file]
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
            src_files_and_dirs = list(self.project_root.glob(artifact_src))
            for path in src_files_and_dirs:
                if path.is_dir():
                    all_files = get_all_file_paths_under_dir(path)
                    all_py_files = get_all_python_files(all_files)
                    for py_file in all_py_files:
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
