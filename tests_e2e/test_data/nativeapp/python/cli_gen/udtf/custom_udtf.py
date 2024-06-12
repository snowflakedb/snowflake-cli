# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from snowflake.snowpark.functions import udtf
from snowflake.snowpark.types import IntegerType, StructField, StructType


@udtf(
    name="alt_int",
    replace=True,
    output_schema=StructType([StructField("number", IntegerType())]),
    input_types=[IntegerType()],
    native_app_params={
        "schema": "ext_code_schema",
        "application_roles": ["app_instance_role"],
    },
)
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
