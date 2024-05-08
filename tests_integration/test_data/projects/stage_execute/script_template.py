import os
from snowflake.core import Root
from snowflake.snowpark.context import get_active_session

session = get_active_session()
database = Root(session).databases[os.environ["TEST_DATABASE_NAME"]]

assert database.name.upper() == os.environ["TEST_DATABASE_NAME"].upper()
