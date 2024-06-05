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
