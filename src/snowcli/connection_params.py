from dataclasses import dataclass


@dataclass
class ConnectionParams:
    user: str
    password: str
    account: str
    host: str = "snowflakecomputing.com"
    port: int = 443
    protocol: str = "https"
    # add a new parameter to support registry login
    session_parameters: str = '{"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "json"}'
