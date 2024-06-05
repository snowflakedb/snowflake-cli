-- user wraps a method from a python file into a function
create or replace function ext_code_schema.py_echo_fn(STR string)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.8
  PACKAGES=('snowflake-snowpark-python')
  HANDLER='echo.echo_fn'
  IMPORTS=('/user_gen/echo.py');

    grant usage
        on function ext_code_schema.py_echo_fn(string)
        to application role app_instance_role;
