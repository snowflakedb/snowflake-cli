import os
from snowflake.core import Root
from snowflake.core.database import DatabaseResource
from snowflake.core.schema import Schema
from snowflake.snowpark.session import Session

session = Session.builder.getOrCreate()
database: DatabaseResource = Root(session).databases[os.environ["TEST_DATABASE_NAME"]]

assert database.name.upper() == os.environ["TEST_DATABASE_NAME"].upper()

# Make a side effect that we can check in tests
database.schemas.create(Schema(name=os.environ["TEST_ID"]))
