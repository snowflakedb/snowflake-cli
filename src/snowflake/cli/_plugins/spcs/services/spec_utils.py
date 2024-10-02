from pathlib import Path
from typing import Any, Dict, List

import yaml
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath


def load_spec(path: Path) -> Dict[str, Any]:
    # TODO(aivanou): Add validation towards schema
    with SecurePath(path).open("r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fh:
        return yaml.safe_load(fh)


def merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    with cli_console.phase("Applying overrides to service specification."):
        return _merge_dicts(base, override)


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for k, v in override.items():
        if k in result:
            if type(result[k]) != type(v):
                cli_console.warning(
                    f"Type mismatch while merging (key={k}, base={type(result[k])}, override={type(v)})"
                )
            elif isinstance(v, dict):
                v = _merge_dicts(result[k], v)
            elif isinstance(v, list) and all(
                isinstance(_v, dict) for _v in result[k] + v
            ):
                v = _merge_lists_of_dicts(result[k], v)
        result[k] = v

    return result


def _merge_lists_of_dicts(
    base: List[Dict[str, Any]], override: List[Dict[str, Any]], merge_key: str = "name"
) -> List[Dict[str, Any]]:
    result = {d[merge_key]: d for d in base}
    assert len(result) == len(base)

    for d in override:
        if d[merge_key] in result:
            d = _merge_dicts(result[d[merge_key]], d)
        result[d[merge_key]] = d

    return list(result.values())
