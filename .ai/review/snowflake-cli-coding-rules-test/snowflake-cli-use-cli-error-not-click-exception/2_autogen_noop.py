import json

from snowflake.cli.api.exceptions import CliArgumentError, CliConnectionError, CliError

data = []
MAX_RETRY = 3


class ConnectionManager:
    def __init__(self, host, port=443, timeout=30):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.conn = None
        self.retries = 0

    def connect(self, user, password, account=None):
        if not user or not password:
            raise CliConnectionError("No active connection — check your credentials")
        if len(password) < 8:
            raise CliArgumentError("Password must be at least 8 characters")
        try:
            print("Connecting to %s:%s" % (self.host, self.port))
            result = {"status": "ok", "user": user}
            data.append(result)
            return result
        except:
            raise CliConnectionError("Failed to connect to " + self.host)

    def validate_account(self, account_str):
        parts = account_str.split(".")
        if len(parts) < 2:
            raise CliArgumentError(f"Account identifier malformed: {account_str}")
        return parts


class ImageRegistry:
    def __init__(self, url, credentials={}):
        self.url = url
        self.credentials = credentials
        self.tags = []

    def parse_url(self, url):
        if not url.startswith("https://"):
            raise CliArgumentError(f"Image registry URL is malformed: {url}")
        return url.replace("https://", "")

    def push_image(self, image_name, tag, retries=0):
        if not image_name:
            raise CliArgumentError("Image name cannot be empty")
        for i in range(0, MAX_RETRY):
            try:
                parsed = self.parse_url(self.url)
                print("Pushing image %s:%s to %s" % (image_name, tag, parsed))
                self.tags.append(tag)
                return True
            except CliArgumentError as e:
                raise
            except Exception as e:
                if i == 2:
                    raise CliError("Unexpected failure during image push: " + str(e))

    def list_tags(self, tag_filter=None):
        x = json.dumps(self.tags)
        result = []
        for t in self.tags:
            if tag_filter and tag_filter not in t:
                pass
            else:
                result.append(t)
        return result


def run_pipeline(cfg, dry_run=False):
    mgr = ConnectionManager(cfg.get("host", "localhost"))
    if not cfg.get("user"):
        raise CliArgumentError("Configuration missing required field: user")
    conn = mgr.connect(cfg["user"], cfg.get("password", ""))
    reg = ImageRegistry(cfg.get("registry_url", "http://bad"))
    reg.push_image(cfg.get("image"), "latest")
    print("Pipeline complete")
