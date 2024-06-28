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

from typing import Dict, Optional

from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    ProcessorMapping,
)
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    UnsupportedArtifactProcessorError,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)
from snowflake.cli.plugins.nativeapp.project_model import NativeAppProjectModel

SNOWPARK_PROCESSOR = "snowpark"


class NativeAppCompiler:
    """
    Compiler class to perform custom processing on all relevant Native Apps artifacts (specified in the project definition file)
    before an application package can be created from those artifacts.
    An artifact can have more than one processor specified for itself, and this class will execute those processors in that order.
    The class also maintains a dictionary of processors it creates in order to reuse them across artifacts, since processor initialization
    is independent of the artifact to process.
    """

    def __init__(
        self,
        na_project: NativeAppProjectModel,
    ):
        self._na_project = na_project
        # dictionary of all processors created and shared between different artifact objects.
        self.cached_processors: Dict[str, ArtifactProcessor] = {}

    def compile_artifacts(self):
        """
        Go through every artifact object in the project definition of a native app, and execute processors in order of specification for each of the artifact object.
        May have side-effects on the filesystem by either directly editing source files or the deploy root.
        """
        should_proceed = False
        for artifact in self._na_project.artifacts:
            if artifact.processors:
                should_proceed = True
                break
        if not should_proceed:
            return

        with cc.phase("Invoking artifact processors"):
            for artifact in self._na_project.artifacts:
                for processor in artifact.processors:
                    artifact_processor = self._try_create_processor(
                        processor_mapping=processor,
                    )
                    if artifact_processor is None:
                        raise UnsupportedArtifactProcessorError(
                            processor_name=processor.name
                        )
                    else:
                        artifact_processor.process(
                            artifact_to_process=artifact, processor_mapping=processor
                        )

    def _try_create_processor(
        self,
        processor_mapping: ProcessorMapping,
        **kwargs,
    ) -> Optional[ArtifactProcessor]:
        """
        Fetch processor object if one already exists in the cached_processors dictionary.
        Else, initialize a new object to return, and add it to the cached_processors dictionary.
        """
        if processor_mapping.name.lower() == SNOWPARK_PROCESSOR:
            curr_processor = self.cached_processors.get(SNOWPARK_PROCESSOR, None)
            if curr_processor is not None:
                return curr_processor
            else:
                curr_processor = SnowparkAnnotationProcessor(
                    na_project=self._na_project,
                )
                self.cached_processors[SNOWPARK_PROCESSOR] = curr_processor
                return curr_processor
        else:
            return None
