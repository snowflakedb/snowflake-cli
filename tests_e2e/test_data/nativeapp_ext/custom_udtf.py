from snowflake.snowpark.functions import udtf
from snowflake.snowpark.types import IntegerType, StructField, StructType


class PrimeSieve:
    def process(self, n):
        is_prime = [True] * (n + 1)
        is_prime[0] = False
        is_prime[1] = False
        p = 2
        while p * p <= n:
            if is_prime[p]:
                # set all multiples of p to False
                for i in range(p * p, n + 1, p):
                    is_prime[i] = False
            p += 1
        # yield all prime numbers
        for p in range(2, n + 1):
            if is_prime[p]:
                yield (p,)


prime_udtf = udtf(
    PrimeSieve,
    output_schema=StructType([StructField("number", IntegerType())]),
    input_types=[IntegerType()],
    native_app_params={
        "schema": "ext_code_schema",
        "application_roles": ["app_instance_role"],
    },
)

# Did not get commented out?
#: @udtf(
#:     name="alt_int",
#:     replace=True,
#:     output_schema=StructType([StructField("number", IntegerType())]),
#:     input_types=[IntegerType()],
#:     native_app_params={
#:         "schema": "ext_code_schema",
#:         "application_roles": ["app_instance_role"],
#:     },
#: )
class Alternator:
    def __init__(self):
        self._positive = True

    def process(self, n):
        for i in range(n):
            if self._positive:
                yield (1,)
            else:
                yield (-1,)
            self._positive = not self._positive
