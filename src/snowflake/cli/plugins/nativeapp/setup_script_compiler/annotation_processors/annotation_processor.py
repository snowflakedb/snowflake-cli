from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Tuple

from snowflake.cli.plugins.nativeapp.artifacts import ArtifactMapping


class AnnotationProcessor(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_annotation_data_from_files(
        self, artifacts: List[ArtifactMapping], project_root: Path
    ) -> Tuple[Dict[Path, Tuple[Path, Path]], Dict[Path, List]]:
        pass
