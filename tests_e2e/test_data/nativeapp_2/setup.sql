create or replace application role app_instance_role;

create or alter versioned schema ext_code_schema;

    grant usage
        on schema ext_code_schema
        to application role app_instance_role;


-- create or replace function ext_code_schema.py_echo_fn(STR string)
--   RETURNS STRING
--   LANGUAGE PYTHON
--   RUNTIME_VERSION=3.9
--   PACKAGES=('snowflake-snowpark-python')
--   HANDLER='echo_fn'
-- AS $$
-- def echo_fn(str):
--     return "echo_fn: " + str
-- $$
-- ;

--     grant usage
--         on function ext_code_schema.py_echo_fn(string)
--         to application role app_instance_role;


create or replace function ext_code_schema.py_echo_fn1(STR string)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.9
  PACKAGES=('snowflake-snowpark-python')
  HANDLER='echo.echo_fn'
  IMPORTS=('/echo.py');

    grant usage
        on function ext_code_schema.py_echo_fn1(string)
        to application role app_instance_role;

-- create or replace function ext_code_schema.java_echo_fn(STR string)
--   RETURNS STRING
--   LANGUAGE JAVA
--   HANDLER='TestFunc.echo'
--   IMPORTS=('/echo.jar');

--     grant usage
--         on function ext_code_schema.java_echo_fn(string)
--         to application role app_instance_role;
