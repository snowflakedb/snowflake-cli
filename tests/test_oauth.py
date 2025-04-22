import os
from textwrap import dedent
from unittest import mock


@mock.patch("snowflake.connector.connect")
def test_oauth_from_parameters(mock_connect, runner):
    runner.invoke(
        [
            "sql",
            "-q",
            "select 1",
            "-x",
            "--authenticator",
            "OAUTH_AUTHORIZATION_CODE",
            "--oauth-client-id",
            "client_id",
            "--oauth-client-secret",
            "secret",
            "--oauth-authorization-url",
            "https://localhost:8000/authorize",
            "--oauth-redirect-uri",
            "http://localhost:8001/snowflake/oauth-redirect",
            "--oauth-scope",
            "session:role:PUBLIC",
            "--oauth-security-features",
            "Feature1 Feature2",
            "--client-store-temporary-credential",
        ]
    )

    mock_connect.assert_called_once_with(
        application="SNOWCLI.SQL",
        application_name="snowcli",
        authenticator="OAUTH_AUTHORIZATION_CODE",
        oauth_client_id="client_id",
        oauth_client_secret="secret",
        oauth_authorization_url="https://localhost:8000/authorize",
        oauth_redirect_uri="http://localhost:8001/snowflake/oauth-redirect",
        oauth_scope="session:role:PUBLIC",
        oauth_security_features="Feature1 Feature2",
        client_store_temporary_credential=True,
    )


@mock.patch("snowflake.connector.connect")
def test_oauth_from_config(mock_connect, runner, config_file):

    config = dedent(
        """\
    default_connection_name = "test"
    
    [connections.test]
    authenticator = "OAUTH_AUTHORIZATION_CODE"
    oauth_client_id = "client_id"
    oauth_client_secret = "secret"
    oauth_authorization_url = "https://localhost:8000/authorize"
    oauth_redirect_uri = "http://localhost:8001/snowflake/oauth-redirect"
    oauth_scope = "session:role:PUBLIC"
    oauth_security_features = "Feature1 Feature2"
    client_store_temporary_credential = true
    """
    )

    with config_file(config) as config_file:
        runner.invoke_with_config_file(config_file, ["sql", "-q", "select 1"])

    mock_connect.assert_called_once_with(
        application="SNOWCLI.SQL",
        application_name="snowcli",
        authenticator="OAUTH_AUTHORIZATION_CODE",
        oauth_client_id="client_id",
        oauth_client_secret="secret",
        oauth_authorization_url="https://localhost:8000/authorize",
        oauth_redirect_uri="http://localhost:8001/snowflake/oauth-redirect",
        oauth_scope="session:role:PUBLIC",
        oauth_security_features="Feature1 Feature2",
        client_store_temporary_credential=True,
    )


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_CONNECTIONS_TEST_AUTHENTICATOR": "OAUTH_AUTHORIZATION_CODE",
        "SNOWFLAKE_CONNECTIONS_TEST_OAUTH_CLIENT_ID": "client_id",
        "SNOWFLAKE_CONNECTIONS_TEST_OAUTH_CLIENT_SECRET": "secret",
        "SNOWFLAKE_CONNECTIONS_TEST_OAUTH_AUTHORIZATION_URL": "https://localhost:8000/authorize",
        "SNOWFLAKE_CONNECTIONS_TEST_OAUTH_REDIRECT_URI": "http://localhost:8001/snowflake/oauth-redirect",
        "SNOWFLAKE_CONNECTIONS_TEST_OAUTH_SCOPE": "session:role:PUBLIC",
        "SNOWFLAKE_CONNECTIONS_TEST_OAUTH_SECURITY_FEATURES": "Feature1 Feature2",
        "SNOWFLAKE_CONNECTIONS_TEST_CLIENT_STORE_TEMPORARY_CREDENTIAL": "True",
    },
    clear=True,
)
@mock.patch("snowflake.connector.connect")
def test_oauth_from_env_variables(mock_connect, runner, config_file):

    config = dedent(
        """\
    default_connection_name = "test"
    
    [connections.test]
    """
    )

    with config_file(config) as config_file:
        r = runner.invoke_with_config_file(config_file, ["sql", "-q", "select 1"])
        print(r.output)

    mock_connect.assert_called_once_with(
        application="SNOWCLI.SQL",
        application_name="snowcli",
        authenticator="OAUTH_AUTHORIZATION_CODE",
        oauth_client_id="client_id",
        oauth_client_secret="secret",
        oauth_authorization_url="https://localhost:8000/authorize",
        oauth_redirect_uri="http://localhost:8001/snowflake/oauth-redirect",
        oauth_scope="session:role:PUBLIC",
        oauth_security_features="Feature1 Feature2",
        client_store_temporary_credential="True",
    )


@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_AUTHENTICATOR": "OAUTH_AUTHORIZATION_CODE",
        "SNOWFLAKE_OAUTH_CLIENT_ID": "client_id",
        "SNOWFLAKE_OAUTH_CLIENT_SECRET": "secret",
        "SNOWFLAKE_OAUTH_AUTHORIZATION_URL": "https://localhost:8000/authorize",
        "SNOWFLAKE_OAUTH_REDIRECT_URI": "http://localhost:8001/snowflake/oauth-redirect",
        "SNOWFLAKE_OAUTH_SCOPE": "session:role:PUBLIC",
        "SNOWFLAKE_OAUTH_SECURITY_FEATURES": "Feature1 Feature2",
        "SNOWFLAKE_CLIENT_STORE_TEMPORARY_CREDENTIAL": "True",
    },
    clear=True,
)
@mock.patch("snowflake.connector.connect")
def test_oauth_from_env_variables_and_temporary_connection(mock_connect, runner):
    runner.invoke(["sql", "-q", "select 1", "-x"])

    mock_connect.assert_called_once_with(
        application="SNOWCLI.SQL",
        application_name="snowcli",
        authenticator="OAUTH_AUTHORIZATION_CODE",
        oauth_client_id="client_id",
        oauth_client_secret="secret",
        oauth_authorization_url="https://localhost:8000/authorize",
        oauth_redirect_uri="http://localhost:8001/snowflake/oauth-redirect",
        oauth_scope="session:role:PUBLIC",
        oauth_security_features="Feature1 Feature2",
        client_store_temporary_credential="True",
    )
