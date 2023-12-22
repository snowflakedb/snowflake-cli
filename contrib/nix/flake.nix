{
  description = "Flake that provides snowcli";
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    src-stable = {
      url = "github:Snowflake-Labs/snowcli?ref=v1.2.3"; # Pins to last stable version tag by hand
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
      url = "github:snowflakedb/snowflake-connector-python?ref=v3.6.0"; # Unpin if needed
      flake = false;
    };
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } (
      { flake-parts-lib, ... }:
      {
        imports = [
          inputs.flake-parts.flakeModules.easyOverlay
        ];
        flake = {
          homeManagerModules.default = flake-parts-lib.importApply ./homeManagerModules { localFlake = self; };
        };
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
                  ]
                  ++
                  builtins.attrValues { inherit (pkgs) installShellFiles; }
                  ;
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
                  postInstall = ''
                    # NOTE: according to strace typer tries to read $HOME to generate completions. This breaks Nix building sandbox.
                    # The completions are thus built by hand through "snow --show-completion"
                    # This is somewhat brittle
                    installShellCompletion --cmd snow \
                    --zsh <(cat <<'END_HEREDOC'
                    #compdef snow

                    _snow_completion() {
                      eval $(env _TYPER_COMPLETE_ARGS="''${words[1,$CURRENT]}" _SNOW_COMPLETE=complete_zsh snow)
                    }

                    compdef _snow_completion snow
                    END_HEREDOC
                    ) \
                    --bash <(cat <<'END_HEREDOC'
                    _snow_completion() {
                        local IFS=$'
                    '
                        COMPREPLY=( $( env COMP_WORDS="''${COMP_WORDS[*]}" \
                                       COMP_CWORD=$COMP_CWORD \
                                       _SNOW_COMPLETE=complete_bash $1 ) )
                        return 0
                    }

                    complete -o default -F _snow_completion snow
                    END_HEREDOC
                    ) \
                    --fish <(cat <<'END_HEREDOC'
                    complete --command snow --no-files --arguments "(env _SNOW_COMPLETE=complete_fish _TYPER_COMPLETE_FISH_ACTION=get-args _TYPER_COMPLETE_ARGS=(commandline -cp) snow)" --condition "env _SNOW_COMPLETE=complete_fish _TYPER_COMPLETE_FISH_ACTION=is-args _TYPER_COMPLETE_ARGS=(commandline -cp) snow"
                    END_HEREDOC
                    )
                  '';
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
            overlayAttrs = {
              inherit (config.packages) snowcli snowcli-stable;
            };
          };
      }
    );
}
