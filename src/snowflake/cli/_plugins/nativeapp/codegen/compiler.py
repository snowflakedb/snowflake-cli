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

import copy
import re
from typing import Dict, Optional

from click import ClickException
from snowflake.cli._plugins.nativeapp.bundle_context import BundleContext
from snowflake.cli._plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    UnsupportedArtifactProcessorError,
)
from snowflake.cli._plugins.nativeapp.codegen.setup.native_app_setup_processor import (
    NativeAppSetupProcessor,
)
from snowflake.cli._plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)
from snowflake.cli._plugins.nativeapp.codegen.templates.templates_processor import (
    TemplatesProcessor,
)
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.api.metrics import CLICounterField
from snowflake.cli.api.project.schemas.entities.common import ProcessorMapping

ProcessorClassType = type[ArtifactProcessor]


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
        bundle_ctx: BundleContext,
    ):
        self._assert_absolute_paths(bundle_ctx)
        self._processor_classes_by_name: Dict[str, ProcessorClassType] = {}
        self._bundle_ctx = bundle_ctx
        # dictionary of all processors created and shared between different artifact objects.
        self.cached_processors: Dict[str, ArtifactProcessor] = {}

        self.register(SnowparkAnnotationProcessor)
        self.register(NativeAppSetupProcessor)
        self.register(TemplatesProcessor)

    def register(self, processor_cls: ProcessorClassType):
        """
        Registers a processor class to enable.
        """

        name = getattr(processor_cls, "NAME", None)
        assert name is not None

        if name in self._processor_classes_by_name:
            raise ValueError(f"Processor {name} is already registered")

        self._processor_classes_by_name[str(name)] = processor_cls

    @staticmethod
    def _assert_absolute_paths(bundle_ctx: BundleContext):
        for name in ["Project", "Deploy", "Bundle", "Generated"]:
            path = getattr(bundle_ctx, f"{name.lower()}_root")
            assert path.is_absolute(), f"{name} root {path} must be an absolute path."

    def compile_artifacts(self):
        """
        Go through every artifact object in the project definition of a native app, and execute processors in order of specification for each of the artifact object.
        May have side-effects on the filesystem by either directly editing source files or the deploy root.
        """
        metrics = get_cli_context().metrics
        metrics.set_counter_default(CLICounterField.TEMPLATES_PROCESSOR, 0)
        metrics.set_counter_default(CLICounterField.SNOWPARK_PROCESSOR, 0)

        if not self._should_invoke_processors():
            return

        with (
            cc.phase("Invoking artifact processors"),
            get_cli_context().metrics.span("artifact_processors"),
        ):
            if self._bundle_ctx.generated_root.exists():
                raise ClickException(
                    f"Path {self._bundle_ctx.generated_root} already exists. Please choose a different name for your generated directory in the project definition file."
                )

            for artifact in self._bundle_ctx.artifacts:
                for processor in artifact.processors:
                    if self._is_enabled(processor):
                        artifact_processor = self._try_create_processor(
                            processor_mapping=processor,
                        )
                        if artifact_processor is None:
                            raise UnsupportedArtifactProcessorError(
                                processor_name=processor.name
                            )
                        else:
                            artifact_processor.process(
                                artifact_to_process=artifact,
                                processor_mapping=processor,
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
        processor_name = processor_mapping.name.lower()
        current_processor = self.cached_processors.get(processor_name)

        if current_processor is not None:
            return current_processor

        processor_cls = self._processor_classes_by_name.get(processor_name)
        if processor_cls is None:
            # No registered processor with the specified name
            return None

        processor_ctx = copy.copy(self._bundle_ctx)
        processor_subdirectory = re.sub(r"[^a-zA-Z0-9_$]", "_", processor_name)
        processor_ctx.bundle_root = (
            self._bundle_ctx.bundle_root / processor_subdirectory
        )
        processor_ctx.generated_root = (
            self._bundle_ctx.generated_root / processor_subdirectory
        )
        current_processor = processor_cls(processor_ctx)
        self.cached_processors[processor_name] = current_processor

        return current_processor

    def _should_invoke_processors(self):
        for artifact in self._bundle_ctx.artifacts:
            for processor in artifact.processors:
                if self._is_enabled(processor):
                    return True
        return False

    def _is_enabled(self, processor: ProcessorMapping) -> bool:
        """
        Determines is a process is enabled. All processors are considered enabled
        unless they are explicitly disabled, typically via a feature flag.
        """
        processor_name = processor.name.lower()
        processor_cls = self._processor_classes_by_name.get(processor_name)
        if processor_cls is None:
            # Unknown processor, consider it enabled, even though trying to
            # invoke it later will raise an exception
            return True

        # if the processor class defines a static method named "is_enabled", then
        # call it. Otherwise, it's considered enabled by default.
        is_enabled_fn = getattr(processor_cls, "is_enabled", lambda: True)
        return is_enabled_fn()
