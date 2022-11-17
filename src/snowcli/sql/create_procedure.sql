f"""
use database {database};
use schema {schema};
use warehouse {warehouse};

CREATE {"OR REPLACE " if overwrite else ""} PROCEDURE {name}{input_parameters}
         RETURNS {return_type}
         LANGUAGE PYTHON
         RUNTIME_VERSION=3.8
         IMPORTS=('{imports}')
         HANDLER='{handler}'
         PACKAGES=({','.join(["'{}'".format(package)
                   for package in packages]) if packages else ""})
        {"EXECUTE AS CALLER" if execute_as_caller else ""};


describe PROCEDURE {name}{signature};
"""
