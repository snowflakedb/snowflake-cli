from snowflake.snowpark.functions import udaf
from snowflake.snowpark.types import IntegerType


class PythonSumUDAF:
    def __init__(self) -> None:
        self._sum = 0

    @property
    def aggregate_state(self):
        return self._sum

    def accumulate(self, input_value):
        self._sum += input_value

    def merge(self, other_sum):
        self._sum += other_sum

    def finish(self):
        return self._sum


sum_udaf = udaf(
    PythonSumUDAF,
    name="sum_int",
    replace=True,
    return_type=IntegerType(),
    input_types=[IntegerType()],
    native_app_params={
        "schema": "ext_code_schema",
        "application_roles": ["app_instance_role"],
    },
)

# Did not get commented out?
#: @udaf(
#:     name="sum_int_dec",
#:     replace=True,
#:     return_type=IntegerType(),
#:     input_types=[IntegerType()],
#:     native_app_params={
#:         "schema": "ext_code_schema",
#:         "application_roles": ["app_instance_role"],
#:     },
#: )
class PythonSumUDAFdec:
    def __init__(self) -> None:
        self._sum = 0

    @property
    def aggregate_state(self):
        return self._sum

    def accumulate(self, input_value):
        self._sum += input_value

    def merge(self, other_sum):
        self._sum += other_sum

    def finish(self):
        return self._sum
