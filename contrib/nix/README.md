This is a Nix flake that allows installing [snowcli](https://github.com/Snowflake-Labs/snowcli) on Linux and Mac systems

# Running

Use the default package from this flake:

```shell
nix run github:Snowflake-Labs/snowcli?dir=contrib/nix -- <PARAMS_FOR_SNOWCLI>
```

# Setup

If not using a system-wide connection file:

1. Create a connection file: `touch connections.toml`
2. Run `nix run github:Snowflake-Labs/snowcli?dir=contrib/nix -- --config-file ./connections.toml connection add`
3. Run `nix run github:Snowflake-Labs/snowcli?dir=contrib/nix -- --config-file ./connections.toml sql -c "<CONNECTION_NAME_FROM_PREVIOUS_STEP>" -q "SELECT CURRENT_ACCOUNT()"` to test

# Limitations

Arrow results are not supported resulting in an error:

```
    Failed to import ArrowResult. No Apache Arrow result set format can be used. ImportError: No module named 'snowflake.connector.arrow_iterator'
```

Statements that return bigger results may produce an error.
