{
  description = "Flake that provides snowcli";
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    src-stable = {
      url = "github:Snowflake-Labs/snowcli?ref=v1.1.1-rc3"; # Pins to last stable version tag by hand
      flake = false;
    };
    src-live = {
      url = "github:Snowflake-Labs/snowcli"; # Tracks live version
      flake = false;
    };
    snowflake-connector-python-src = {
      url = "github:snowflakedb/snowflake-connector-python?ref=v3.2.0"; # Unpin if needed
      flake = false;
    };
    snowflake-connector-python-src-live = {
      url = "github:snowflakedb/snowflake-connector-python?ref=v3.3.1"; # Unpin if needed
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
          packages =
            let
              mkSnowflakeConnectoPython = src: python3.pkgs.snowflake-connector-python.overrideAttrs
                (
                  finalAttrs: previousAttrs: {
                    inherit src;
                    propagatedBuildInputs = previousAttrs.propagatedBuildInputs ++
                      (builtins.attrValues { inherit (python3.pkgs) sortedcontainers packaging platformdirs tomlkit keyring; });
                  }
                );
              mkSnowCli = { version, snowflakeConnectorPkg, patchFile, src }: python3.pkgs.buildPythonApplication {
                pname = "snowflake-cli-labs";
                inherit version src;
                format = "pyproject";

                patches = [ patchFile ];

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
                  strictyaml
                  tomlkit
                  typer
                  chardet # needed by snowflake-connector-python
                  urllib3
                  gitpython
                  pluggy
                  pyyaml
                ] ++ [ snowflakeConnectorPkg ];
                meta = {
                  mainProgram = "snow";
                  description = "Snowflake CLI";
                  homepage = "https://github.com/Snowflake-Labs/snowcli";
                  license = nixpkgs.lib.licenses.asl20;
                };
              };
            in
            rec {
              default = snowcli;
              # Pinned version
              flake-snowflake-connector-python = mkSnowflakeConnectoPython inputs.snowflake-connector-python-src;
              # Live version
              flake-snowflake-connector-python-live = mkSnowflakeConnectoPython inputs.snowflake-connector-python-src-live;
              # This is the live version
              snowcli = mkSnowCli {
                version = "2.0.0-dev";
                snowflakeConnectorPkg = flake-snowflake-connector-python-live;
                patchFile = ./patches/pyprojectLive.patch;
                src = inputs.src-live;
              };
              # This is last released version
              snowcli-stable = mkSnowCli {
                version = "1.1.1";
                snowflakeConnectorPkg = flake-snowflake-connector-python;
                patchFile = ./patches/pyproject.patch;
                src = inputs.src-stable;
              };
            };
        };
    };
}
