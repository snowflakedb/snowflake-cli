import logging
import os

# Suppress logging from Snowflake connector
logging.getLogger("snowflake").setLevel(logging.ERROR)

# Restrict permissions of all created files
os.umask(0o077)
