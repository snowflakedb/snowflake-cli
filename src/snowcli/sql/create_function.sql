f"""
use database {database};
use schema {schema};
use warehouse {warehouse};

CREATE {"OR REPLACE " if overwrite else ""} FUNCTION {name}{input_parameters}
         RETURNS {return_type}
         LANGUAGE PYTHON
         RUNTIME_VERSION=3.8
         IMPORTS=('{imports}')
         HANDLER='{handler}'
         PACKAGES=({','.join(["'{}'".format(package)
                   for package in packages]) if packages else ""});


describe function {name}{signature};
"""
