CREATE OR REPLACE APPLICATION ROLE app_instance_role;
CREATE OR REPLACE SCHEMA non_versioned_schema;

    GRANT USAGE ON SCHEMA non_versioned_schema
        TO APPLICATION ROLE app_instance_role;

    CREATE OR REPLACE
        TABLE non_versioned_schema.id_table(id INT);

    INSERT INTO
        non_versioned_schema.id_table VALUES (1);

    GRANT SELECT
        ON non_versioned_schema.id_table
        TO APPLICATION ROLE app_instance_role;


CREATE OR ALTER VERSIONED SCHEMA versioned_schema;

    CREATE OR REPLACE
        SECURE PROCEDURE versioned_schema.area_of_square_sproc(side FLOAT)
        RETURNS FLOAT
        LANGUAGE SQL
        EXECUTE AS OWNER
    AS $$
        DECLARE
            area_of_square FLOAT;
        BEGIN
            area_of_square := :side * :side;
            RETURN area_of_square;
        END;
    $$;

    GRANT USAGE
        ON PROCEDURE versioned_schema.area_of_square_sproc(FLOAT)
        TO APPLICATION ROLE app_instance_role;

    CREATE OR REPLACE
        SECURE FUNCTION versioned_schema.get_hello_world()
        RETURNS STRING
        LANGUAGE SQL
        AS $$
            SELECT 'hello world!'
        $$;

    GRANT USAGE
        ON FUNCTION versioned_schema.get_hello_world()
        TO APPLICATION ROLE app_instance_role;
