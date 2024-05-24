from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from click import ClickException
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)


class UnsupportedArtifactProcessorError(ClickException):
    """Exception thrown when a user has passed in an unsupported artifact processor."""

    def __init__(self, processor_name: str):
        super().__init__(
            f"Unsupported value {processor_name} detected for an artifact processor. Please refer to documentation for a list of supported types."
        )


class ArtifactProcessor(ABC):
    @abstractmethod
    def __init__(
        self,
        project_definition: NativeApp,
        project_root: Path,
        deploy_root: Path,
        generated_root: Path,
        **kwargs,
    ) -> None:
        assert project_root.is_absolute()
        assert deploy_root.is_absolute()
        assert generated_root.is_absolute()

    @abstractmethod
    def process(
        self,
        artifact_to_process: PathMapping,
        processor_mapping: Optional[ProcessorMapping],
        **kwargs,
    ) -> None:
        pass
