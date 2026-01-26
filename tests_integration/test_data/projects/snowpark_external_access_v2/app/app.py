from urllib.request import Request, urlopen

import _snowflake
from snowflake.snowpark import Session


def check_secret_and_get_status_function():
    return _check_secret_and_get_status()


def check_secret_and_get_status_procedure(session: Session):
    return _check_secret_and_get_status()


def _check_secret_and_get_status():
    generic_secret = _snowflake.get_generic_secret_string("generic_secret")
    assert generic_secret

    url = "https://docs.snowflake.com"
    request = Request(
        url,
        headers={
            "User-Agent": "snowpark-external-access-debug/1.0",
            "Accept": "*/*",
        },
    )
    response = urlopen(request)
    return response.status
