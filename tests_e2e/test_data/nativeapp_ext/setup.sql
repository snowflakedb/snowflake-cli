create or replace application role app_instance_role;

create or alter versioned schema ext_code_schema;

    grant usage
        on schema ext_code_schema
        to application role app_instance_role;

create or replace application role app_instance_role;

create or alter versioned schema ext_code_schema;

    grant usage
        on schema ext_code_schema
        to application role app_instance_role;


--- UDAF ----
CREATE OR REPLACE
AGGREGATE FUNCTION ext_code_schema.sum_int(self INT)
RETURNS INT
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
PACKAGES=('snowflake-snowpark-python')
HANDLER='PythonSumUDAF'
as $$
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
$$;

GRANT USAGE 
ON FUNCTION ext_code_schema.sum_int(INT)
TO APPLICATION ROLE app_instance_role;

-- UDTF
CREATE OR REPLACE
FUNCTION ext_code_schema.PrimeSieve(arg1 INT)
RETURNS TABLE (NUMBER INT)
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
PACKAGES=('snowflake-snowpark-python')
HANDLER='PrimeSieve'
as $$
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
$$;

GRANT USAGE ON FUNCTION ext_code_schema.PrimeSieve(INT)
TO APPLICATION ROLE app_instance_role;
