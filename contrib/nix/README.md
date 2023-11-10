This is a Nix flake that allows installing [snowcli](https://github.com/Snowflake-Labs/snowcli) on Linux and Mac systems

# Running

Use the default package from this flake:

```shell
nix run github:Snowflake-Labs/snowcli?dir=contrib/nix -- <PARAMS_FOR_SNOWCLI>
```

# Setup
## Home manager

Flake provides a home manager module that can be used to manage the snowcli settings.

To use it, import in the home manager user configuration:
```nix
{
    inputs.snowcli.url = "github:Snowflake-Labs/snowcli?dir=contrib/nix";
    # <...>
    outputs = { ... }@inputs: {
        # Where the home-manager configuration of a user is:
        home-manager.users.username = {
          imports = [
            inputs.snowcli.homeManagerModules.default
            {
              programs.snowcli.enable = true;
              programs.snowcli.settings.connections.dev = {
                account = "account_identifier";
                # <the rest of the settings>
              };
            }
          ];
        };
        # <the rest of the config>
    };
}

```

## Standalone
If not using a system-wide connection file:

1. Create a connection file: `touch connections.toml`
2. Run `nix run github:Snowflake-Labs/snowcli?dir=contrib/nix -- --config-file ./connections.toml connection add`
3. Run `nix run github:Snowflake-Labs/snowcli?dir=contrib/nix -- --config-file ./connections.toml sql -c "<CONNECTION_NAME_FROM_PREVIOUS_STEP>" -q "SELECT CURRENT_ACCOUNT()"` to test

# Overlay

Flake provides an overlay with two packages:

* `snowcli-stable` tracks the latest released version
* `snowcli` tracks the latest commit of the `snowcli` repository
