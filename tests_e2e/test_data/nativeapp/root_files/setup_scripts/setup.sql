create or replace application role app_instance_role;

create or alter versioned schema ext_code_schema;

    grant usage
        on schema ext_code_schema
        to application role app_instance_role;

execute immediate from '/setup_scripts/user_gen_procs.sql';
execute immediate from '/setup_scripts/user_gen_udf.sql';
