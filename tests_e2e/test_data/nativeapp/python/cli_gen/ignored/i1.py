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

from snowflake.snowpark.functions import udf

# Should be ignored at callback, will process file but not generate any sql
udf(func=None)

random = udf(func=None)

"""
The following is an FYI
If a user is to define a lambda function in the decorator, like so:
udf(lambda x: x + 1, return_type=IntegerType(), native_app_params={"schema": "ext_code_schema", "application_roles": ["app_instance_role"]})
Then the generated function is this:

CREATE OR REPLACE
FUNCTION ext_code_schema."<lambda>"()
RETURNS INT
LANGUAGE PYTHON
RUNTIME_VERSION=3.11
IMPORTS=('/cli_gen/ignored/i2.py')
PACKAGES=('snowflake-snowpark-python')
HANDLER='i2.<lambda>';

GRANT USAGE ON FUNCTION ext_code_schema."<lambda>"()
TO APPLICATION ROLE app_instance_role;

Snowpark will throw an error in this case:
Error: snowflake.snowpark.exceptions.SnowparkSessionException: 1403): No default Session is found. Please create a session before you call function 'udf' or use decorator '@udf'. in function <lambda> with handler i2.<lambda>
This is because instead of using the Snowpark udf as a decorator, it is being used as a function and hence cannot be de-annotated before uploading to stage.
"""
