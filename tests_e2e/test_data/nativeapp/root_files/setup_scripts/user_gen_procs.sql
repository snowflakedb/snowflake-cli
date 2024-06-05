-- user wraps a method from a python file into a procedure
create or replace procedure ext_code_schema.py_echo_proc(STR string)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION='3.8'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'echo.echo_proc'
  IMPORTS = ('/user_gen/echo.py');

    grant usage
        on procedure ext_code_schema.py_echo_proc(string)
        to application role app_instance_role;
