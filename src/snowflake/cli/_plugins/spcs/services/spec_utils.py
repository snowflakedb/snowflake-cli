from pathlib import Path
from typing import Any, Dict

import yaml
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath


def load_spec(path: Path) -> Dict[str, Any]:
    # TODO(aivanou): Add validation towards schema
    with SecurePath(path).open("r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fh:
        return yaml.safe_load(fh)


def merge_specs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    raise NotImplementedError
