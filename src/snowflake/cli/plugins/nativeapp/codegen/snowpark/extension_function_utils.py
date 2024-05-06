import os
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from snowflake.cli.api.console import cli_console as cc
from snowflake.cli.plugins.nativeapp.utils import is_single_quoted

TEMP_OBJECT_NAME_PREFIX = "SNOWPARK_TEMP_"


def _get_handler_path_without_suffix(file_path: Path) -> str:
    file_parts = file_path.parts
    if os.path.sep in file_parts[0]:
        file_parts = file_parts[1:]

    return ".".join(file_parts).removesuffix(".py")


def _get_object_type_as_text(object_type: str) -> str:
    return object_type.replace("_", " ")


def _get_handler(dest_file: Path, func: Union[str, Tuple[str, str]]) -> Optional[str]:
    """
    Gets the handler for the extension function to be used in the creation of the SQL statement.
    """
    # FYI: Needs dest file to be set
    if isinstance(func, str):
        return f"{_get_handler_path_without_suffix(dest_file)}.{func}"
    else:
        # isinstance(self.func, Tuple[str, str]) is only possible if using decorator.register_from_file(), which is not allowed in codegen as of now.
        # When allowed, refer to https://github.com/snowflakedb/snowpark-python/blob/v1.15.0/src/snowflake/snowpark/_internal/udf_utils.py#L1092 on resolving handler name
        cc.warning(
            f"Could not determine handler name for {func[1]}, proceeding without the handler."
        )
        return None


def _get_object_name_for_udf_sp(
    object_name: str, schema: str, handler: str
) -> Optional[str]:
    """
    Gets the name of the extension function to be used in the creation of the SQL statement.
    """
    # FYI: Needs schema and handler to be set
    if object_name.startswith(TEMP_OBJECT_NAME_PREFIX):
        return f"{schema}.{handler}"
    else:
        return object_name


def _get_all_imports(raw_imports: List):
    """
    Sets validated imports in string form to be used in the creation of the SQL statement.
    """
    all_urls: List[str] = []
    # Input raw_imports can look like [("tests/resources/test_udf_dir/test_udf_file.py", "resources.test_udf_dir.test_udf_file")]
    for valid_import in raw_imports:
        # TODO: should add leading "/" for paths on stage?
        if isinstance(valid_import, str):
            # Similar to session.add_import("tests/resources/test_udf_dir/test_udf_file.py"), no checks on path being absolute
            # Or session.add_import("/tmp/temp.txt")
            all_urls.append(valid_import)
        else:
            # Similar to session.add_import("tests/resources/test_udf_dir/test_udf_file.py", import_path="resources.test_udf_dir.test_udf_file")'
            # Convert to path on stage
            import_path = valid_import[1]
            if import_path.ends_with(".py"):
                without_suffix = "/".join(import_path.split(".")[:-1])
                with_suffix = f"{without_suffix}.py"
                all_urls.append(with_suffix)
            else:
                without_suffix = "/".join(import_path.split("."))
                all_urls.append(without_suffix)
    return ",".join([url if is_single_quoted(url) else f"'{url}'" for url in all_urls])


def _enrich_entity(entity: Dict[str, Any], py_file: Path):
    entity["handler"] = _get_handler(dest_file=py_file, func=entity["func"])
    entity["object_name"] = _get_object_name_for_udf_sp(
        object_name=entity["object_name"],
        schema=entity["schema"],
        handler=entity["handler"],
    )
    entity["all_imports"] = _get_all_imports(raw_imports=entity["raw_imports"])
