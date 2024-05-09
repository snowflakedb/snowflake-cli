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

TEMP_OBJECT_NAME_PREFIX = "SNOWPARK_TEMP_"


def _get_handler_path_without_suffix(
    file_path: Path, deploy_root: Path, suffix_str_to_rm: Optional[str] = None
) -> str:
    """
    Get a handler for an extension function based on the file path on the stage. If a specific suffix needs to be removed from the path,
    then that is also taken into account.
    """
    # rel_file_path = file_path.absolute().relative_to(deploy_root.resolve())  # No leading slash
    # stem, suffix = os.path.splitext(rel_file_path)
    # if (suffix_str_to_rm is not None) and (suffix == suffix_str_to_rm):
    #     file_parts = Path(stem).parts
    # else:
    #     file_parts = rel_file_path.parts
    # return ".".join(file_parts)
    return "NotImplementedHandler"


def _get_object_type_as_text(object_type: str) -> str:
    return object_type.replace("_", " ")


def _get_handler(
    dest_file: Path, func: Union[str, Tuple[str, str]], deploy_root: Path
) -> Optional[str]:
    """
    Gets the handler for the extension function to be used in the creation of the SQL statement.
    """
    if isinstance(func, str):
        return f"{_get_handler_path_without_suffix(file_path=dest_file, suffix_str_to_rm='.py', deploy_root=deploy_root)}.{func}"
    else:
        # isinstance(self.func, Tuple[str, str]) is only possible if using decorator.register_from_file(), which is not allowed in codegen as of now.
        # When allowed, refer to https://github.com/snowflakedb/snowpark-python/blob/v1.15.0/src/snowflake/snowpark/_internal/udf_utils.py#L1092 on resolving handler name
        cc.warning(
            f"Could not determine handler name for {func[1]}, proceeding without the handler."
        )
        return None


def _get_schema_and_name_for_extension_function(
    object_name: str, schema: str, handler: str
) -> Optional[str]:
    """
    Gets the name of the extension function to be used in the creation of the SQL statement.
    It will use the schema and the handler as the object name if the function name is determined to be a Snowpark-generated placeholder.
    Otherwise, it will honor the user's input for object name.

    """
    if object_name.startswith(TEMP_OBJECT_NAME_PREFIX):
        return f"{schema}.{handler}"
    else:
        return f"{schema}.{object_name}"


def is_single_quoted(name: str) -> bool:
    """
    Helper function to do a generic check on whether the provided string is surrounded by single quotes.
    """
    return name.startswith("'") and name.endswith("'")


def ensure_single_quoted(obj_lst: List[str]) -> List[str]:
    """
    Helper function to ensure that a list of object strings is transformed to a list of object strings surrounded by single quotes.
    """
    return [obj if is_single_quoted(obj) else f"'{obj}'" for obj in obj_lst]


def _get_all_imports(
    raw_imports: List[Union[str, Tuple[str, str]]], suffix_str: str
) -> str:
    """
    Creates a string containing all the relevant imports for an extension function. This string is used in the creation of the SQL statement.

    Parameters:
        raw_imports (List[Union[str, Tuple[str, str]]]): The raw imports that will be used to create the final string.
                    The function needes to handle different input types, similar to snowpark.
                    Example 1: [("tests/resources/test_udf_dir/test_udf_file.py", "resources.test_udf_dir.test_udf_file")]
                    Example 2: session.add_import("tests/resources/test_udf_dir/test_udf_file.py")
                    Example 3: session.add_import("tests/resources/test_udf_dir/test_udf_file.py", import_path="resources.test_udf_dir.test_udf_file")
        suffix_str (str): The suffix to add to the import path, if determined by the function. Must contain the "." part of the suffix as well.

    Returns:
        A string containing all the imports.
    """
    all_urls: List[str] = []
    for raw_import in raw_imports:  # Example 1
        if isinstance(raw_import, str):  # Example 2
            all_urls.append(raw_import)
        else:  # Example 3
            local_path = Path(raw_import[0])
            stage_import = raw_import[1]
            suffix_str = local_path.suffix
            if suffix_str != "":
                # We use suffix check here instead of local_path.is_file() as local_path may not exist, making is_file() False.
                # We do not provide validation on local_path existing, and hence should not fail or treat it differently than any other file.
                without_suffix = "/".join(stage_import.split("."))
                all_urls.append(f"{without_suffix}{suffix_str}")
            else:
                file_path = "/".join(stage_import.split("."))
                all_urls.append(file_path)
    return ",".join(ensure_single_quoted(all_urls))


def _enrich_entity(
    entity: Dict[str, Any], py_file: Path, deploy_root: Path, suffix_str: str
):
    """
    Sets additional properties for a given entity object, that could not be set earlier due to missing information or limited access to the execution context.
    """
    entity["handler"] = _get_handler(
        dest_file=py_file, func=entity["func"], deploy_root=deploy_root
    )
    entity["object_name"] = _get_schema_and_name_for_extension_function(
        object_name=entity["object_name"],
        schema=entity["schema"],
        handler=entity["handler"],
    )
    entity["all_imports"] = _get_all_imports(
        raw_imports=entity["raw_imports"], suffix_str=suffix_str
    )
