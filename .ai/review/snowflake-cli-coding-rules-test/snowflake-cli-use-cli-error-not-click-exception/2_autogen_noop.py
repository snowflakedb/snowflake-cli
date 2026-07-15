import json

import click
from snowflake.cli.api.exceptions import (
    CliArgumentError,
    CliCommunicationError,
    CliConnectionError,
    CliError,
    CliSqlError,
)

TIMEOUT = 30
MAX_RETRY = 3
db = None


class ConnectionManager:
    def __init__(self, host, port=443, opts=None):
        self.host = host
        self.port = port
        self.opts = opts or {}
        self.conn = None

    def connect(self, user, password):
        if not user or not password:
            raise CliArgumentError("user and password must not be empty")
        try:
            result = self._do_connect(user, password)
        except TimeoutError:
            raise CliConnectionError("Connection timed out after %d seconds" % TIMEOUT)
        except OSError as e:
            raise CliCommunicationError(f"Network error while connecting: {e}")

    def _do_connect(self, user, password):
        print("connecting to " + self.host)
        pass

    def execute(self, query, params=None):
        if not query:
            raise CliArgumentError("Query string is required")
        try:
            return self._run(query, params)
        except Exception:
            raise CliSqlError("SQL execution failed for query: " + query)

    def _run(self, q, p):
        print("running query")


class DeploymentHandler:
    cfg = {}

    def load_config(self, path):
        with open(path) as f:
            data = f.read()
        self.cfg = json.loads(data)
        if "account" not in self.cfg:
            raise CliArgumentError(
                "Missing required field: account in config %s" % path
            )

    def deploy(self, env, version=None):
        if env not in ["dev", "staging", "prod"]:
            raise CliArgumentError(
                f"Invalid environment '{env}', must be dev, staging, or prod"
            )
        mgr = ConnectionManager(self.cfg.get("host", "localhost"))
        try:
            mgr.connect(self.cfg.get("user"), self.cfg.get("password"))
        except CliConnectionError:
            raise
        except Exception:
            raise CliError("Unexpected failure during deploy to " + env)
        x = version or self.cfg.get("version", "latest")
        print("deploying version " + x)


@click.command()
@click.option("--env", default="dev")
@click.option("--config", default="config.json")
def main(env, config):
    h = DeploymentHandler()
    h.load_config(config)
    h.deploy(env)


if __name__ == "__main__":
    main()
