import json
from pathlib import Path
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


def _determine_virtual_env(processor: ProcessorMapping) -> Dict[str, Any]:
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
        env_path = env_props.get("path", None)
        if env_path is None:
            cc.warning(
                "No path found in project definition file for the conda environment to run the Snowpark processor in. Will attempt to auto-detect the current venv path."
            )
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
    ) -> Dict[Path, Optional[Any]]:
        """
        Intended to be the main method which can perform all relevant processing, and/or write to a target file, which depends on the type of processor.
        For SnowparkAnnotationProcessor, the target file is the setup script.
        """

        kwargs = (
            _determine_virtual_env(processor_mapping)
            if processor_mapping is not None
            else {}
        )

        # 1. Get all src.py -> dest.py mapping
        # TODO: Logic to replaced in a follow up PR by NADE
        src_py_file_to_dest_py_file_map = self.get_src_py_file_to_dest_py_file_map(
            artifact_to_process
        )

        # 2. Get entities through Snowpark callback
        src_py_file_to_collected_entities: Dict[Path, Optional[Any]] = {}
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

                src_py_file_to_collected_entities[dest_file] = collected_entities

                if collected_entities is None:
                    cc.message("No entities could be collected from the file path.")
                    continue

                cc.message(f"This is the file path in deploy root: {dest_file}\n")
                cc.message("This is the list of collected entities:")
                cc.message(collected_entities)

                # 4. Enrich entities by setting additional properties
                for entity in collected_entities:
                    _enrich_entity(
                        entity=entity,
                        py_file=dest_file,
                        deploy_root=self.deploy_root,
                        suffix_str=".py",
                    )

        # TODO: Temporary for testing, while feature is being built in phases
        return src_py_file_to_collected_entities

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
