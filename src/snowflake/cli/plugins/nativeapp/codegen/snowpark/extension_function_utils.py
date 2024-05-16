from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from click.exceptions import ClickException


class MalformedExtensionFunctionError(ClickException):
    """Required extension function attribute is missing."""

    def __init__(self, message: str):
        super().__init__(message=message)


# This prefix is created by Snowpark for an extension function when the user has not supplied any themselves.
# TODO: move to sandbox execution to omit object name in this case: https://github.com/snowflakedb/snowflake-cli/pull/1056/files#r1599784063
TEMP_OBJECT_NAME_PREFIX = "SNOWPARK_TEMP_"


def get_object_type_as_text(name: str) -> str:
    """
    Replace underscores with spaces in a given string.

    Parameters:
        name (str): Any arbitrary string
    Returns:
        A string that has replaced underscores with spaces.
    """
    return name.replace("_", " ")


def _sanitize_str_attribute(
    ex_fn: Dict[str, Any],
    attr: str,
    make_uppercase: bool = False,
    py_file: Optional[Path] = None,
    raise_err: bool = False,
):
    """
    Sanitizes a single key-value pair of the specified dictionary. As part of the sanitization,
    it goes through a few checks. A key must be created if it does not already exist.
    Then, it checks the type of the value of the key, i.e. if it is of type str, and if it contains any leading or trailing whitespaces.
    A user is able to specity if they want to re-assign a key to an uppercase instance of its original value.
    If any of the sanitization checks fail and the user wants to raise an error, it throws a MalformedExtensionFunctionError.
    """
    assign_to_none = True
    if ex_fn.get(attr, None):
        if not isinstance(ex_fn[attr], str):
            raise MalformedExtensionFunctionError(
                f"Attribute '{attr}' of extension function must be of type 'str'."
            )

        if (
            len(ex_fn[attr].strip()) > 0
        ):  # To prevent where attr value is "  " etc, which should still be invalid
            assign_to_none = False
            if make_uppercase:
                ex_fn[attr] = ex_fn[attr].upper()

    if assign_to_none:
        ex_fn[attr] = None

    if assign_to_none and raise_err:
        raise MalformedExtensionFunctionError(
            _create_missing_attr_message(attribute=attr, py_file=py_file)
        )


def _sanitize_list_or_dict_attribute(
    ex_fn: Dict[str, Any],
    attr: str,
    expected_type: Type,
    default_value: Any = None,
    py_file: Optional[Path] = None,
    raise_err: bool = False,
):
    """
    Sanitizes a single key-value pair of the specified dictionary. As part of the sanitization,
    it goes through a few checks. A key must be created if it does not already exist.
    Then, it checks the type of the value of the key. It also checks for the length of the value, which is why the value must be of type list or dict.
    A user is able to specity a default value that they want to assign a newly created key to.
    If any of the sanitization checks fail and the user wants to raise an error, it throws a MalformedExtensionFunctionError.
    """
    assign_to_default = True
    if ex_fn.get(attr, None):
        if not isinstance(ex_fn[attr], expected_type):
            raise MalformedExtensionFunctionError(
                f"Attribute '{attr}' of extension function must be of type '{expected_type}'."
            )

        if len(ex_fn[attr]) > 0:
            assign_to_default = False

    if assign_to_default:
        ex_fn[attr] = default_value

    if assign_to_default and raise_err:
        raise MalformedExtensionFunctionError(
            _create_missing_attr_message(attribute=attr, py_file=py_file)
        )


def _create_missing_attr_message(attribute: str, py_file: Optional[Path]):
    """
    This message string is used to create an instance of the MalformedExtensionFunctionError.
    """
    if py_file is None:
        raise ValueError("Python file path must not be None.")
    return f"Required attribute '{attribute}' of extension function is missing or incorrectly defined for an extension function defined in python file {py_file.absolute()}."


def _is_function_wellformed(ex_fn: Dict[str, Any]) -> bool:
    """
    Checks if the specified dictionary contains a key called 'func'.
    if it does, then the value must be of type str or a list of fixes size 2.
    It further checks the item at 1st index of this list.
    """
    if ex_fn.get("func", None):
        if isinstance(ex_fn["func"], str):
            return ex_fn["func"].strip() != ""
        elif isinstance(ex_fn["func"], list) and (len(ex_fn["func"]) == 2):
            return isinstance(ex_fn["func"][1], str) and ex_fn["func"][1].strip() != ""
    return False


def sanitize_extension_function_data(ex_fn: Dict[str, Any], py_file: Path):
    """
    Helper function to sanitize different attributes of a dictionary. As part of the sanitization, validations and default assignments are performed.
    This helper function is needed because different callback sources can create dictionaries with different/missing keys, and since
    Snowflake CLI may not own all callback implementations, the dictionaries need to have the minimum set of keys and their default
    values to be used in creation of the SQL DDL statements.

    Parameters:
        ex_fn (Dict[str, Any]): A dictionary of key value pairs to sanitize
        py_file (Path): The python file from which this dictionary was created.
    Returns:
        A boolean value, True if everything has been successfully validated and assigned, False if an error was encountered.
    """
    # TODO: accumulate errors/warnings instead of per-attribute interruption: https://github.com/snowflakedb/snowflake-cli/pull/1056/files#r1599904008

    # Must have keys to create an extension function in SQL for Native Apps
    _sanitize_str_attribute(
        ex_fn=ex_fn,
        attr="object_type",
        make_uppercase=True,
        py_file=py_file,
        raise_err=True,
    )

    _sanitize_str_attribute(
        ex_fn=ex_fn,
        attr="object_name",
        make_uppercase=True,
        py_file=py_file,
        raise_err=True,
    )

    _sanitize_str_attribute(
        ex_fn=ex_fn,
        attr="return_sql",
        make_uppercase=True,
        py_file=py_file,
        raise_err=True,
    )

    if not _is_function_wellformed(ex_fn=ex_fn):
        raise MalformedExtensionFunctionError(
            _create_missing_attr_message(attribute="func", py_file=py_file)
        )

    default_raw_imports: List[Union[str, Tuple[str, str]]] = []
    _sanitize_list_or_dict_attribute(
        ex_fn=ex_fn,
        attr="raw_imports",
        expected_type=list,
        default_value=default_raw_imports,
        py_file=py_file,
        raise_err=True,
    )

    _sanitize_str_attribute(ex_fn=ex_fn, attr="schema", make_uppercase=True)
    # Custom message, hence throwing an error separately
    if ex_fn["schema"] is None:
        raise MalformedExtensionFunctionError(
            f"Required attribute 'schema' in 'native_app_params' of extension function is missing for an extension function defined in python file {py_file.absolute()}."
        )

    # Other optional keys
    ex_fn["anonymous"] = ex_fn.get("anonymous", False)
    ex_fn["replace"] = ex_fn.get("replace", False)
    ex_fn["if_not_exists"] = ex_fn.get("if_not_exists", False)

    if ex_fn["replace"] and ex_fn["if_not_exists"]:
        raise MalformedExtensionFunctionError(
            "Options 'replace' and 'if_not_exists' are incompatible."
        )

    default_input_args: List[Dict[str, Any]] = []
    _sanitize_list_or_dict_attribute(
        ex_fn=ex_fn,
        attr="input_args",
        expected_type=list,
        default_value=default_input_args,
    )
    default_input_types: List[str] = []
    _sanitize_list_or_dict_attribute(
        ex_fn=ex_fn,
        attr="input_sql_types",
        expected_type=list,
        default_value=default_input_types,
    )
    if len(ex_fn["input_args"]) != len(ex_fn["input_sql_types"]):
        raise MalformedExtensionFunctionError(
            "The number of extension function parameters does not match the number of parameter types."
        )

    _sanitize_str_attribute(ex_fn=ex_fn, attr="all_imports")
    _sanitize_str_attribute(ex_fn=ex_fn, attr="all_packages")
    _sanitize_list_or_dict_attribute(
        ex_fn=ex_fn, attr="external_access_integrations", expected_type=list
    )
    _sanitize_list_or_dict_attribute(ex_fn=ex_fn, attr="secrets", expected_type=dict)
    _sanitize_str_attribute(ex_fn=ex_fn, attr="inline_python_code")
    _sanitize_str_attribute(ex_fn=ex_fn, attr="execute_as", make_uppercase=True)
    _sanitize_str_attribute(ex_fn=ex_fn, attr="handler")
    _sanitize_str_attribute(
        ex_fn=ex_fn, attr="runtime_version", py_file=py_file, raise_err=True
    )

    has_app_roles = (
        ex_fn.get("application_roles", None) and len(ex_fn["application_roles"]) > 0
    )
    if has_app_roles:
        if all(isinstance(app_role, str) for app_role in ex_fn["application_roles"]):
            ex_fn["application_roles"] = [
                app_role.upper() for app_role in ex_fn["application_roles"]
            ]
        else:
            raise MalformedExtensionFunctionError(
                f"Attribute 'application_roles' of extension function must be a list of strings."
            )
    else:
        ex_fn["application_roles"] = []


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
        raise MalformedExtensionFunctionError(
            f"Could not determine handler name for {func[1]}."
        )


def _get_schema_and_name_for_extension_function(
    object_name: str, schema: Optional[str], func: str
) -> Optional[str]:
    """
    Gets the name of the extension function to be used in the creation of the SQL statement.
    It will use the schema and the python function's name as the object name if the function name is determined to be a Snowpark-generated placeholder.
    Otherwise, it will honor the user's input for object name.
    """
    if object_name.startswith(TEMP_OBJECT_NAME_PREFIX):
        return f"{schema}.{func}" if schema else func
    else:
        return f"{schema}.{object_name}" if schema else object_name


def _is_single_quoted(name: str) -> bool:
    """
    Helper function to do a generic check on whether the provided string is surrounded by single quotes.
    """
    return name.startswith("'") and name.endswith("'")


def _ensure_single_quoted(obj_lst: List[str]) -> List[str]:
    """
    Helper function to ensure that a list of object strings is transformed to a list of object strings surrounded by single quotes.
    """
    return [obj if _is_single_quoted(obj) else f"'{obj}'" for obj in obj_lst]


def _get_all_imports(raw_imports: List[Union[str, Tuple[str, str]]]) -> str:
    """
    Creates a string containing all the relevant imports for an extension function. This string is used in the creation of the SQL statement.

    Parameters:
        raw_imports (List[Union[str, Tuple[str, str]]]): The raw imports that will be used to create the final string.
                    The function needes to handle different input types, similar to snowpark.
                    Example 1: [("tests/resources/test_udf_dir/test_udf_file.py", "resources.test_udf_dir.test_udf_file")]
                    Example 2: session.add_import("tests/resources/test_udf_dir/test_udf_file.py")
                    Example 3: session.add_import("tests/resources/test_udf_dir/test_udf_file.py", import_path="resources.test_udf_dir.test_udf_file")
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
            local_path_suffix = local_path.suffix
            if local_path_suffix != "":
                # 1. We use suffix check here instead of local_path.is_file() as local_path may not exist, making is_file() False.
                # We do not provide validation on local_path existing, and hence should not fail or treat it differently than any other file.
                # 2. stage_import may already have a suffix, but we do not provide validation on it.
                # It is on the user to know and use Snowpark's decorator attributes correctly.
                without_suffix = "/".join(stage_import.split("."))
                all_urls.append(f"{without_suffix}{local_path_suffix}")
            else:
                file_path = "/".join(stage_import.split("."))
                all_urls.append(file_path)
    return ",".join(_ensure_single_quoted(all_urls))


def enrich_ex_fn(ex_fn: Dict[str, Any], py_file: Path, deploy_root: Path):
    """
    Sets additional properties for a given extension function dictionary, that could not be set earlier due to missing information or limited access to the execution context.
    Parameters:
        ex_fn (Dict[str, Any]): A dictionary of key value pairs to sanitize
        py_file (Path): The python file from which this dictionary was created.
        deploy_root (Path): The deploy root of the the project.
    Returns:
        The original but edited extension function dictionary
    """
    ex_fn["handler"] = _get_handler(
        dest_file=py_file, func=ex_fn["func"], deploy_root=deploy_root
    )
    ex_fn["object_name"] = _get_schema_and_name_for_extension_function(
        object_name=ex_fn["object_name"],
        schema=ex_fn["schema"],
        func=ex_fn["func"],
    )
    ex_fn["all_imports"] = _get_all_imports(raw_imports=ex_fn["raw_imports"] or [])
