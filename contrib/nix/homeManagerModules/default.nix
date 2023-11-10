# Home manager module for snowcli
{ localFlake }:
{ config, pkgs, lib, ... }:
let
  inherit (lib) mkOption mkEnableOption types mkIf;
  inherit (pkgs.stdenv.hostPlatform) system isDarwin;

  cfg = config.programs.snowcli;
  selfPkgs' = localFlake.packages.${system};
  settingsFormat = pkgs.formats.toml { };

  configFile = (if isDarwin then "Library/Application Support" else config.xdg.configHome) + "/snowflake/config.toml"; # Decide where the config is kept
in
{
  options.programs.snowcli = {
    enable = mkEnableOption "Snowcli";
    package = mkOption {
      type = types.package;
      default = selfPkgs'.default;
    };
    settings = lib.mkOption {
      type = types.submodule {
        freeformType = settingsFormat.type;
      };
      example = {
        connections.dev = {
          account = "account_identifier";
          user = "username";
          database = "some_database";
          authenticator = "externalbrowser";
        };
      };
      description = ''
        Snowcli configuration.

        See <link xlink:href="https://github.com/Snowflake-Labs/snowcli"/>
        for more information.
      '';
    };
    # TODO: authentication and config as RFC42 toml
  };
  config = mkIf cfg.enable {
    home.packages = [ cfg.package ];
    home.file.${configFile}.text = builtins.readFile (settingsFormat.generate "config.toml" cfg.settings);
  };
}
