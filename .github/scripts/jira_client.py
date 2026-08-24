#!/usr/bin/env python3
"""
Shared JIRA REST API client used by the daily automated-ticket workflows
(CVE scanning, Python version staleness, ...). Mimics what the gajira-create
action does but in a loop, with dedup/reopen support.
"""

import os
import re
import sys
from typing import Any, Dict, Optional, Union

import requests

REQUEST_TIMEOUT = 30

# JQL has no parameterized-query API, so values interpolated into a JQL string
# must be restricted to this safe character set to prevent JQL injection.
_JQL_SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _validate_jql_token(value: str, field_name: str) -> str:
    if not _JQL_SAFE_TOKEN_RE.match(value):
        raise ValueError(f"Unsafe value for JQL {field_name}: {value!r}")
    return value


def _sanitize_exception(e: Exception) -> str:
    """Return a safe-to-print label for an exception.

    ``requests`` exceptions can embed the request URL (and, transitively,
    query params) in their message. Only the exception type is surfaced so
    credentials or other sensitive request details never reach logs.
    """
    return type(e).__name__


class JiraClient:
    """Client for interacting with JIRA REST API."""

    def __init__(self, base_url: str, email: str, api_token: str):
        if not base_url.lower().startswith("https://"):
            raise ValueError(
                f"JIRA_BASE_URL must use https://, got scheme of: {base_url!r}"
            )
        self.base_url = base_url.rstrip("/")
        self.auth = (email, api_token)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

    @classmethod
    def from_env(cls) -> "JiraClient":
        """Build a JiraClient from the JIRA_BASE_URL / JIRA_USER_EMAIL / JIRA_API_TOKEN env vars."""
        base_url = os.environ.get("JIRA_BASE_URL")
        email = os.environ.get("JIRA_USER_EMAIL")
        api_token = os.environ.get("JIRA_API_TOKEN")

        if not all([base_url, email, api_token]):
            print(
                "❌ Missing required environment variables: JIRA_BASE_URL, JIRA_USER_EMAIL, JIRA_API_TOKEN"
            )
            sys.exit(1)

        assert base_url is not None
        assert email is not None
        assert api_token is not None
        try:
            return cls(base_url, email, api_token)
        except ValueError as e:
            print(f"❌ {e}")
            sys.exit(1)

    def search_existing_ticket(self, label: str, parent_key: str) -> Optional[str]:
        """Search for an existing automated JIRA ticket by label and parent key."""
        label = label.lower()
        _validate_jql_token(label, "label")
        _validate_jql_token(parent_key, "parent_key")

        # Build JQL query - search by label (don't quote labels in JQL)
        jql = f"project = SNOW AND parent = {parent_key} AND labels = {label} AND labels = automated"

        # Search for existing ticket using v3 API
        # Note: requests library will automatically URL-encode the params
        url = f"{self.base_url}/rest/api/3/search/jql"
        params: Dict[str, Union[str, int]] = {"jql": jql, "maxResults": 1}

        try:
            response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)  # type: ignore[arg-type]
            if response.status_code == 200:
                data = response.json()
                issues = data.get("issues", [])
                if len(issues) > 0:
                    issue_key = issues[0].get("key")
                    if issue_key:
                        return issue_key
                    # If key not in response, fetch it using the ID
                    issue_id = issues[0].get("id")
                    if issue_id:
                        key_response = self.session.get(
                            f"{self.base_url}/rest/api/2/issue/{issue_id}",
                            params={"fields": "key"},
                            timeout=REQUEST_TIMEOUT,
                        )
                        return key_response.json().get("key")
            else:
                print(
                    f"    ⚠️  Search failed (HTTP {response.status_code})",
                    file=sys.stderr,
                )
        except Exception as e:
            print(f"    ⚠️  Search error: {_sanitize_exception(e)}", file=sys.stderr)

        return None

    def get_issue_status(self, issue_key: str) -> Optional[str]:
        """Get the status of a JIRA issue."""
        try:
            url = f"{self.base_url}/rest/api/2/issue/{issue_key}"
            params = {"fields": "status"}
            response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response.json()["fields"]["status"]["name"]
        except Exception as e:
            print(
                f"    ⚠️  Error getting status: {_sanitize_exception(e)}",
                file=sys.stderr,
            )
        return None

    def reopen_issue(self, issue_key: str) -> bool:
        """Reopen a closed JIRA issue."""
        try:
            # Get available transitions
            url = f"{self.base_url}/rest/api/2/issue/{issue_key}/transitions"
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                return False

            transitions = response.json().get("transitions", [])

            # Find transition to reopen (TODO, Open, Reopened, etc.)
            reopen_transition = None
            for trans in transitions:
                name = trans.get("name", "")
                if any(
                    keyword in name.lower() for keyword in ["todo", "open", "reopen"]
                ):
                    reopen_transition = trans.get("id")
                    break

            if reopen_transition:
                payload = {"transition": {"id": reopen_transition}}
                response = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
                return response.status_code in [200, 204]
            else:
                print("    ⚠️  Could not find transition to reopen")
                return False
        except Exception as e:
            print(
                f"    ⚠️  Error reopening issue: {_sanitize_exception(e)}",
                file=sys.stderr,
            )
            return False

    def add_comment(self, issue_key: str, comment: str) -> bool:
        """Add a comment to a JIRA issue."""
        try:
            url = f"{self.base_url}/rest/api/2/issue/{issue_key}/comment"
            payload = {"body": comment}
            response = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            return response.status_code in [200, 201]
        except Exception as e:
            print(
                f"    ⚠️  Error adding comment: {_sanitize_exception(e)}",
                file=sys.stderr,
            )
            return False

    def create_issue(self, issue_data: Dict[str, Any]) -> Optional[str]:
        """Create a new JIRA issue."""
        try:
            url = f"{self.base_url}/rest/api/2/issue"
            response = self.session.post(url, json=issue_data, timeout=REQUEST_TIMEOUT)
            if response.status_code == 201:
                return response.json().get("key")
            else:
                print(f"❌ Failed to create ticket (HTTP {response.status_code})")
                return None
        except Exception as e:
            print(f"❌ Error creating issue: {_sanitize_exception(e)}", file=sys.stderr)
            return None

    def transition_to_todo(self, issue_key: str) -> bool:
        """Transition a newly created issue to TODO status."""
        try:
            url = f"{self.base_url}/rest/api/2/issue/{issue_key}/transitions"
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200:
                print(
                    f"    ⚠️  Failed to get transitions (HTTP {response.status_code})",
                    file=sys.stderr,
                )
                return False

            transitions = response.json().get("transitions", [])

            todo_transition = None
            for trans in transitions:
                name = trans.get("name", "")
                if (
                    "todo" in name.lower()
                    or "to do" in name.lower()
                    or "to-do" in name.lower()
                ):
                    todo_transition = trans.get("id")
                    break

            if todo_transition:
                payload = {"transition": {"id": todo_transition}}
                response = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
                return response.status_code in [200, 204]
            else:
                print("    ⚠️  Could not find TODO transition", file=sys.stderr)
                return False
        except Exception as e:
            print(
                f"    ⚠️  Error transitioning to TODO: {_sanitize_exception(e)}",
                file=sys.stderr,
            )
            return False
