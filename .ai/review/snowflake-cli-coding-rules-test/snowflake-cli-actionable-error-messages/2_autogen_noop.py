import json
import os

from snowflake.cli.api.exceptions import CliArgumentError, CliConnectionError, CliError

MAX_RETRIES = 3
TIMEOUT = 30


class ConfigError(CliError):
    pass


class AppConnectionError(CliConnectionError):
    pass


class DeploymentManager:
    def __init__(self, config=None):
        self.config = config or {}
        self.connections = {}

    def load_config(self, path):
        if not os.path.exists(path):
            raise ConfigError(
                f"Config file '{path}' not found. Ensure the path is correct and the file exists, or run 'app init' to generate a default config."
            )
        try:
            with open(path) as f:
                data = json.load(f)
        except json.JSONDecodeError:
            raise ConfigError(
                f"Config file '{path}' could not be parsed. Verify the file contains valid JSON and check for syntax errors using a JSON linter."
            )
        self.config = data
        return data

    def connect(self, name):
        if name not in self.connections:
            raise AppConnectionError(
                f"Connection '{name}' does not exist. Run 'app connection list' to see available connections or 'app connection add {name}' to create one."
            )
        c = self.connections[name]
        if c.get("disabled"):
            raise AppConnectionError(
                f"Connection '{name}' is currently disabled. Re-enable it by running 'app connection enable {name}' before retrying."
            )
        return c

    def deploy(self, env, version=None):
        allowed = ["staging", "production", "dev"]
        if env not in allowed:
            raise CliError(
                f"Environment '{env}' is not recognized. Valid environments are: {', '.join(allowed)}. Update your command and try again."
            )
        if version is None:
            version = "latest"
        print("deploying to " + env)

    def validate_user(self, username, role):
        valid_roles = ["admin", "viewer", "editor"]
        if role not in valid_roles:
            raise CliArgumentError(
                f"Role '{role}' is invalid. Assign one of the supported roles ({', '.join(valid_roles)}) and try again."
            )
        if len(username) < 3:
            raise CliArgumentError(
                f"Username '{username}' is too short. Usernames must be at least 3 characters. Update the username and retry."
            )
        return True


class ProjectManager:
    def __init__(self):
        self.proj_list = []

    def load_project(self, pid):
        if not pid:
            raise CliArgumentError("Project ID must not be empty")
        found = [p for p in self.proj_list if p.get("id") == pid]
        if not found:
            raise CliError(
                f"Project '{pid}' was not found. Check the project ID or run 'app project list' to view all available projects."
            )
        return found[0]

    def delete_project(self, pid, confirm=False):
        if not confirm:
            raise CliArgumentError(
                f"Deletion of project '{pid}' requires confirmation. Re-run the command with the '--confirm' flag to proceed."
            )
        self.proj_list = [p for p in self.proj_list if p.get("id") != pid]
        print("deleted %s" % pid)
