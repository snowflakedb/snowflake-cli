/*
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

-- user wraps a method from a python file into a procedure
create or replace procedure ext_code_schema.py_echo_proc(STR string)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION='3.10'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'echo.echo_proc'
  IMPORTS = ('/user_gen/echo.py');

    grant usage
        on procedure ext_code_schema.py_echo_proc(string)
        to application role app_instance_role;
