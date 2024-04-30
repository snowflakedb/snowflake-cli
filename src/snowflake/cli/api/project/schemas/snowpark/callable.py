from typing import Dict, List, Optional, Union

from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.snowpark.argument import Argument
from snowflake.cli.api.project.schemas.updatable_model import (
    IdentifierField,
    UpdatableModel,
)


class Callable(UpdatableModel):
    name: str = Field(
        title="Object identifier"
    )  # TODO: implement validator. If a name is filly qualified, database and schema cannot be specified
    database: Optional[str] = IdentifierField(
        title="Name of the database for the function or procedure", default=None
    )

    schema_name: Optional[str] = IdentifierField(
        title="Name of the schema for the function or procedure",
        default=None,
        alias="schema",
    )
    handler: str = Field(
        title="Function’s or procedure’s implementation of the object inside source module",
        examples=["functions.hello_function"],
    )
    returns: str = Field(
        title="Type of the result"
    )  # TODO: again, consider Literal/Enum
    signature: Union[str, List[Argument]] = Field(
        title="The signature parameter describes consecutive arguments passed to the object"
    )
    runtime: Optional[Union[str, float]] = Field(
        title="Python version to use when executing ", default=None
    )
    external_access_integrations: Optional[List[str]] = Field(
        title="Names of external access integrations needed for this procedure’s handler code to access external networks",
        default=[],
    )
    secrets: Optional[Dict[str, str]] = Field(
        title="Assigns the names of secrets to variables so that you can use the variables to reference the secrets",
        default={},
    )
    imports: Optional[List[str]] = Field(
        title="Stage and path to previously uploaded files you want to import",
        default=[],
    )

    @field_validator("runtime")
    @classmethod
    def convert_runtime(cls, runtime_input: Union[str, float]) -> str:
        if isinstance(runtime_input, float):
            return str(runtime_input)
        return runtime_input


class FunctionSchema(Callable):
    pass


class ProcedureSchema(Callable):
    execute_as_caller: Optional[bool] = Field(
        title="Determine whether the procedure is executed with the privileges of the owner (you) or with the privileges of the caller",
        default=False,
    )
