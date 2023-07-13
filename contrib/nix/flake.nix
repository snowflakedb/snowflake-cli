{
  description = "Flake that provides snowcli";
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    src = {
      url = "github:Snowflake-Labs/snowcli";
      flake = false;
    };
    # Effectively a nightly build
    snowflake-connector-python-src = {
      url = "github:snowflakedb/snowflake-connector-python";
      flake = false;
    };
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      flake = { };
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      perSystem = { config, self', inputs', pkgs, system, ... }:
        let
          inherit (inputs'.nixpkgs.legacyPackages) python3;
        in
        {
          # Per-system attributes can be defined here. The self' and inputs'
          # module parameters provide easy access to attributes of the same
          # system.

          formatter = pkgs.nixpkgs-fmt;
          packages = rec {
            default = snowcli;
            snowflake-connector-python-nightly-ish = python3.pkgs.snowflake-connector-python.overrideAttrs (
              finalAttrs: previousAttrs: {
                src = inputs.snowflake-connector-python-src;
                propagatedBuildInputs = previousAttrs.propagatedBuildInputs ++
                  (builtins.attrValues { inherit (python3.pkgs) sortedcontainers packaging platformdirs tomlkit; });
              }
            );
            snowcli = python3.pkgs.buildPythonApplication {
              pname = "snowflake-cli-labs";
              version = "1.0.0";
              format = "pyproject";
              inherit (inputs) src;

              patches = [ ./pyproject.patch ];

              # NOTE: for debugging purposes, to see effect of patch application
              # postPatch = ''
              #   cat pyproject.toml
              # '';

              nativeBuildInputs = with python3.pkgs; [
                hatch-vcs
                hatchling
              ];
              propagatedBuildInputs = with python3.pkgs; [
                coverage
                jinja2
                rich
                requests
                requirements-parser
                tomlkit
                typer
                chardet # needed by snowflake-connector-python
              ] ++ [ snowflake-connector-python-nightly-ish ];
              meta = {
                mainProgram = "snow";
                description = "Snowflake CLI";
                homepage = "https://github.com/Snowflake-Labs/snowcli";
                license = nixpkgs.lib.licenses.asl20;
              };
            };
          };
        };
    };
}
