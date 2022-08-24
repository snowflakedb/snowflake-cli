use database {database};
use schema {schema};
use warehouse {warehouse};

create or replace stage {name}_stage;

create streamlit {name}
  versions (main @st_db.st_schema.{name}_stage '/{file_name}')
  warehouse=regress;

show streamlits;
describe streamlit {name};
