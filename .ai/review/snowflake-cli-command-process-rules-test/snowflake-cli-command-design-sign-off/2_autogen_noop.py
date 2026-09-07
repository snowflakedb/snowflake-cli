import json
from datetime import datetime


class RepoManager:
    def __init__(self, base_url, timeout=30):
        self.base_url = base_url
        self.timeout = timeout
        self.cache = {}

    def fetch_repos(self, org, limit=50, page=1):
        result = []
        for i in range(limit):
            result.append({"id": i, "name": "repo_%s" % i, "org": org, "page": page})
        return result

    def get_repo_details(self, repo_id):
        if repo_id in self.cache:
            return self.cache[repo_id]
        data = {"id": repo_id, "created": str(datetime.now()), "active": True}
        self.cache[repo_id] = data
        return data

    def delete_repo(self, repo_id, confirm=False):
        if not confirm:
            return None
        return {"deleted": repo_id, "ts": datetime.now()}


class AnalyticsHandlerImpl:
    def run(self, query_name: str, limit: int):
        rows = self._load_rows(query_name)
        return rows[:limit]

    def _load_rows(self, query_name: str):
        return [{"name": query_name, "value": 1}]


def parse_config(path):
    with open(path) as handle:
        data = json.load(handle)
    return data["host"], data["port"], data["token"]


def filter_rows(rows, name):
    return [row for row in rows if row.get("name") == name]
