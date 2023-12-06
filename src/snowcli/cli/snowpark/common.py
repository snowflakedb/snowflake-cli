from __future__ import annotations

import os
from typing import Dict, List, Optional

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.constants import ObjectNames, ObjectType
from snowcli.utils import generate_deploy_stage_name
from snowflake.connector.cursor import DictCursor, SnowflakeCursor


def _map_type(type_: str):
    mapping = {"string": "varchar"}
    return mapping.get(type_.lower(), type_)


def object_to_signature(function_or_procedure: Dict):
    signature = ", ".join(
        _map_type(o["type"]) for o in function_or_procedure["signature"]
    )
    return f"{function_or_procedure['name']}({signature}) RETURN {_map_type(function_or_procedure['returns'])}".upper()


def remove_parameter_names(identifier: str):
    """
    Removes parameter names from identifier.
    Deploy commands for function and procedure requires identifier
    with parameter names (e.g. `hello(number int, name string)`),
    but describe command requires only parameter types (e.g. `hello(int, string)`)
    :param identifier: `hello(number int, name string)`
    :return: `hello(int, string)`
    """
    open_parenthesis_index = identifier.index("(")
    parameters = identifier[open_parenthesis_index + 1 : -1]
    if not parameters:
        return identifier
    types = [t.strip().split(" ")[1] for t in parameters.split(",")]
    return f"{identifier[0:open_parenthesis_index]}({', '.join(types)})"


def check_if_replace_is_required(
    object_type: ObjectType,
    current_state,
    handler: str,
    return_type: str,
) -> bool:
    import logging

    log = logging.getLogger(__name__)
    resource_json = _convert_resource_details_to_dict(current_state)
    anaconda_packages = resource_json["packages"]
    log.info(
        f"Found {len(anaconda_packages)} defined Anaconda "
        f"packages in deployed {object_type}..."
    )
    log.info("Checking if app configuration has changed...")
    updated_package_list = _get_snowflake_packages_delta(
        anaconda_packages,
    )

    if updated_package_list:
        diff = len(updated_package_list) - len(anaconda_packages)
        log.info(f"Found difference of {diff} packages. Replacing the {object_type}.")
        return True

    if (
        resource_json["handler"].lower() != handler.lower()
        or _sql_to_python_return_type_mapper(resource_json["returns"]).lower()
        != return_type.lower()
    ):
        log.info(
            f"Return type or handler types do not match. Replacing the {object_type}."
        )
        return True

    return False


def _convert_resource_details_to_dict(function_details: SnowflakeCursor) -> dict:
    import json

    function_dict = {}
    json_properties = ["packages", "installed_packages"]
    for function in function_details:
        if function[0] in json_properties:
            function_dict[function[0]] = json.loads(
                function[1].replace("'", '"'),
            )
        else:
            function_dict[function[0]] = function[1]
    return function_dict


def _get_snowflake_packages_delta(anaconda_packages) -> List[str]:
    updated_package_list = []
    if os.path.exists("requirements.snowflake.txt"):
        with open("requirements.snowflake.txt", encoding="utf-8") as f:
            # for each line, check if it exists in anaconda_packages. If it
            # doesn't, add it to the return string
            for line in f:
                if line.strip() not in anaconda_packages:
                    updated_package_list.append(line.strip())
        return updated_package_list
    else:
        return updated_package_list


def _sql_to_python_return_type_mapper(resource_return_type: str) -> str:
    """
    Some of the python data types get converted to SQL types, when function/procedure is created.
    So, to properly compare types, we use mapping based on:
    https://docs.snowflake.com/en/developer-guide/udf-stored-procedure-data-type-mapping#sql-python-data-type-mappings

    Mind you, this only applies to cases, in which Snowflake accepts python type as return.
    Ie. if function returns list, it has to be declared as 'array' during creation,
    therefore any conversion is not necessary
    """
    mapping = {
        "number(38,0)": "int",
        "timestamp_ntz(9)": "datetime",
        "timestamp_tz(9)": "datetime",
        "varchar(16777216)": "string",
    }

    return mapping.get(resource_return_type.lower(), resource_return_type.lower())


class SnowparkObjectManager(SqlExecutionMixin):
    @property
    def _object_type(self) -> ObjectType:
        raise NotImplementedError()

    @property
    def _object_execute(self):
        raise NotImplementedError()

    def create(self, *args, **kwargs) -> SnowflakeCursor:
        raise NotImplementedError()

    def execute(self, execution_identifier: str) -> SnowflakeCursor:
        return self._execute_query(f"{self._object_execute} {execution_identifier}")

    @staticmethod
    def artifact_stage_path(identifier: str):
        return generate_deploy_stage_name(identifier).lower()

    def get_existing_objects(self):
        """
        Returns list of existing objects identified by their signature, for example ['HELLO(VARCHAR) RETURN VARCHAR']
        """
        results = self._execute_query(
            f"show user {self._object_type.value.sf_plural_name}",
            cursor_class=DictCursor,
        )
        return {o["arguments"]: o for o in results}

    def create_query(
        self,
        identifier: str,
        return_type: str,
        handler: str,
        artifact_file: str,
        packages: List[str],
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        execute_as_caller: bool = False,
    ) -> str:
        packages_list = ",".join(f"'{p}'" for p in packages)
        query = [
            f"create or replace {self._object_type.value.sf_name} {identifier}",
            f"returns {return_type}",
            "language python",
            "runtime_version=3.8",
            f"imports=('{artifact_file}')",
            f"handler='{handler}'",
            f"packages=({packages_list})",
        ]

        if external_access_integrations:
            external_access_integration_name = ",".join(
                f"{e}" for e in external_access_integrations
            )
            query.append(
                f"external_access_integrations=({external_access_integration_name})"
            )

        if secrets:
            secret_name = ",".join(f"'{k}'={v}" for k, v in secrets.items())
            query.append(f"secrets=({secret_name})")

        if execute_as_caller:
            query.append("execute as caller")

        return "\n".join(query)


def build_udf_sproc_identifier(udf_sproc_dict):
    arguments = ", ".join(
        (f"{arg['name']} {arg['type']}" for arg in udf_sproc_dict["signature"])
    )
    return f"{udf_sproc_dict['name']}({arguments})"
