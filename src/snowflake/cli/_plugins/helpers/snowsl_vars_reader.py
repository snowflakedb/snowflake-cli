from __future__ import annotations

EnvVars = dict[str, str]

DicoveredVars = list[dict[str, str]]
UnusedVars = list[dict[str, str]]
Summary = str

CheckResult = tuple[DicoveredVars, UnusedVars, Summary]

KNOWN_SNOWSQL_ENV_VARS = {
    "SNOWSQL_ACCOUNT": {
        "Found": "SNOWSQL_ACCOUNT",
        "Suggested": "SNOWFLAKE_ACCOUNT",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_PWD": {
        "Found": "SNOWSQL_PASSWORD",
        "Suggested": "SNOWFLAKE_PASSWORD",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_USER": {
        "Found": "SNOWSQL_USER",
        "Suggested": "SNOWFLAKE_USER",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_REGION": {
        "Found": "SNOWSQL_REGION",
        "Suggested": "SNOWFLAKE_REGION",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_ROLE": {
        "Found": "SNOWSQL_ROLE",
        "Suggested": "SNOWFLAKE_ROLE",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_WAREHOUSE": {
        "Found": "SNOWSQL_WAREHOUSE",
        "Suggested": "SNOWFLAKE_WAREHOUSE",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_DATABASE": {
        "Found": "SNOWSQL_DATABASE",
        "Suggested": "SNOWFLAKE_DATABASE",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_SCHEMA": {
        "found": "SNOWSQL_SCHEMA",
        "Suggested": "SNOWFLAKE_SCHEMA",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_HOST": {
        "Found": "SNOWSQL_HOST",
        "Suggested": "SNOWFLAKE_HOST",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_PORT": {
        "Found": "SNOWSQL_PORT",
        "Suggested": "SNOWFLAKE_PORT",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_PROTOCOL": {
        "Found": "SNOWSQL_PROTOCOL",
        "Suggested": "SNOWFLAKE_PROTOCOL",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections#use-environment-variables-for-snowflake-credentials",
    },
    "SNOWSQL_PROXY_HOST": {
        "Found": "SNOWSQL_PROXY_HOST",
        "Suggested": "PROXY_HOST",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-cli#use-a-proxy-server",
    },
    "SNOWSQL_PROXY_PORT": {
        "Found": "SNOWSQL_PROXY_HOST",
        "Suggested": "PROXY_HOST",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-cli#use-a-proxy-server",
    },
    "SNOWSQL_PROXY_USER": {
        "Found": "SNOWSQL_PROXY_PORT",
        "Suggested": "PROXY_PORT",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-cli#use-a-proxy-server",
    },
    "SNOWSQL_PROXY_PWD": {
        "Found": "SNOWSQL_PROXY_PWD",
        "Suggested": "PROXY_PWD",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-cli#use-a-proxy-server",
    },
    "SNOWSQL_PRIVATE_KEY_PASSPHRASE": {
        "Found": "SNOWSQL_PRIVATE_KEY_PASSPHRASE",
        "Suggested": "PRIVATE_KEY_PASSPHRASE",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-connections",
    },
    "EXIT_ON_ERROR": {
        "Found": "EXIT_ON_ERROR",
        "Suggested": "SNOWFLAKE_ENHANCED_EXIT_CODES",
        "Additional info": "https://docs.snowflake.com/en/developer-guide/snowflake-cli/sql/execute-sql",
    },
}


def check_env_vars(variables: EnvVars) -> CheckResult:
    """Checks passed dict objects for possible SnowSQL variables.

    Returns tuple of
    - sequence of variables that can be adjusted
    - sequence of variables that have no corresponding variables
    - a summary messages
    """
    discovered: DicoveredVars = []
    unused: UnusedVars = []

    prefix_matched = (e for e in variables if e.lower().startswith("snowsql"))

    for var in prefix_matched:
        if suggestion := KNOWN_SNOWSQL_ENV_VARS.get(var, None):
            discovered.append(suggestion)
        else:
            unused.append(
                {
                    "Found": var,
                    "Suggested": "n/a",
                    "Additional info": "Unused variable",
                }
            )

    discovered_cnt = len(discovered)
    unused_cnt = len(unused)

    summary: Summary = (
        f"Found {discovered_cnt + unused_cnt} SnowSQL environment variables,"
        f" {discovered_cnt} with replacements, {unused_cnt} unused."
    )

    return discovered, unused, summary
