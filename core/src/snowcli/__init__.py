import logging

# Suppress logging from Snowflake connector
logging.getLogger("snowflake").setLevel(logging.ERROR)
