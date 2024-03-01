import _snowflake
from http.client import HTTPSConnection
from snowflake.snowpark import Session


def check_secret_and_get_status_function():
    return _check_secret_and_get_status()


def check_secret_and_get_status_procedure(session: Session):
    return _check_secret_and_get_status()


def _check_secret_and_get_status():
    generic_secret = _snowflake.get_generic_secret_string("generic_secret")
    assert generic_secret

    host = "docs.snowflake.com"
    conn = HTTPSConnection(host)
    conn.request("GET", "/")
    response = conn.getresponse()
    return response.status
