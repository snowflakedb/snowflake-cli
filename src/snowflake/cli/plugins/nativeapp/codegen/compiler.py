from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    UnsupportedArtifactProcessorError,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)

SNOWPARK_PROCESSOR = "snowpark"

# Singleton dictionary of all processors created and shared between different artifact objects
cached_processors: Dict[str, ArtifactProcessor] = {}


def _try_create_processor(
    processor_mapping: ProcessorMapping,
    project_definition: NativeApp,
    project_root: Path,
    deploy_root: Path,
    **kwargs,
) -> Optional[ArtifactProcessor]:
    if processor_mapping.name.lower() == SNOWPARK_PROCESSOR:
        curr_processor = cached_processors.get(SNOWPARK_PROCESSOR, None)
        if curr_processor is not None:
            return curr_processor
        else:
            curr_processor = SnowparkAnnotationProcessor(
                project_definition=project_definition,
                project_root=project_root,
                deploy_root=deploy_root,
            )
            cached_processors[SNOWPARK_PROCESSOR] = curr_processor
            return curr_processor
    else:
        return None


def _find_and_execute_processors(
    project_definition: NativeApp, project_root: Path, deploy_root: Path
):
    """
    Go through every artifact object in the project definition of a native app, and execute processors in order of specification for each of the artifact object.
    """
    artifacts = [
        artifact
        for artifact in project_definition.artifacts
        if isinstance(artifact, PathMapping)
    ]

    for artifact in artifacts:
        for processor in artifact.processors:
            artifact_processor = _try_create_processor(
                project_definition=project_definition,
                project_root=project_root,
                deploy_root=deploy_root,
                processor_mapping=processor,
            )
            if artifact_processor is None:
                raise UnsupportedArtifactProcessorError(processor_name=processor.name)
            else:
                artifact_processor.process(
                    artifact_to_process=artifact, processor_mapping=processor
                )
