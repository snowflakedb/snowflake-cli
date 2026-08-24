#!/usr/bin/env python3
"""
This script reads CVEs from JSON and creates JIRA tickets.
It mimics what the gajira-create action does but in a loop.
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, Optional, Union

import requests


class JiraClient:
    """Client for interacting with JIRA REST API."""

    def __init__(self, base_url: str, email: str, api_token: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (email, api_token)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Content-Type": "application/json"})

    def search_existing_ticket(self, cve_id: str, parent_key: str) -> Optional[str]:
        """Search for existing JIRA ticket by CVE ID and parent key."""
        # Convert CVE ID to lowercase for label search
        cve_label = cve_id.lower()

        # Build JQL query - search by label (don't quote labels in JQL)
        jql = f"project = SNOW AND parent = {parent_key} AND labels = {cve_label} AND labels = automated"

        # Search for existing ticket using v3 API
        # Note: requests library will automatically URL-encode the params
        url = f"{self.base_url}/rest/api/3/search/jql"
        params: Dict[str, Union[str, int]] = {"jql": jql, "maxResults": 1}

        try:
            response = self.session.get(url, params=params)  # type: ignore[arg-type]
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
                        )
                        return key_response.json().get("key")
            else:
                print(f"    ⚠️  Search failed: {response.text}", file=sys.stderr)
        except Exception as e:
            print(f"    ⚠️  Search error: {e}", file=sys.stderr)

        return None

    def get_issue_status(self, issue_key: str) -> Optional[str]:
        """Get the status of a JIRA issue."""
        try:
            url = f"{self.base_url}/rest/api/2/issue/{issue_key}"
            params = {"fields": "status"}
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                return response.json()["fields"]["status"]["name"]
        except Exception as e:
            print(f"    ⚠️  Error getting status: {e}", file=sys.stderr)
        return None

    def reopen_issue(self, issue_key: str) -> bool:
        """Reopen a closed JIRA issue."""
        try:
            # Get available transitions
            url = f"{self.base_url}/rest/api/2/issue/{issue_key}/transitions"
            response = self.session.get(url)
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
                response = self.session.post(url, json=payload)
                return response.status_code in [200, 204]
            else:
                print("    ⚠️  Could not find transition to reopen")
                return False
        except Exception as e:
            print(f"    ⚠️  Error reopening issue: {e}", file=sys.stderr)
            return False

    def add_comment(self, issue_key: str, comment: str) -> bool:
        """Add a comment to a JIRA issue."""
        try:
            url = f"{self.base_url}/rest/api/2/issue/{issue_key}/comment"
            payload = {"body": comment}
            response = self.session.post(url, json=payload)
            return response.status_code in [200, 201]
        except Exception as e:
            print(f"    ⚠️  Error adding comment: {e}", file=sys.stderr)
            return False

    def create_issue(self, issue_data: Dict[str, Any]) -> Optional[str]:
        """Create a new JIRA issue."""
        try:
            url = f"{self.base_url}/rest/api/2/issue"
            response = self.session.post(url, json=issue_data)
            if response.status_code == 201:
                return response.json().get("key")
            else:
                print(f"❌ Failed to create ticket (HTTP {response.status_code})")
                try:
                    print(f"Response: {json.dumps(response.json(), indent=2)}")
                except:
                    print(f"Response: {response.text}")
                return None
        except Exception as e:
            print(f"❌ Error creating issue: {e}", file=sys.stderr)
            return None

    def transition_to_todo(self, issue_key: str) -> bool:
        """Transition a newly created issue to TODO status."""
        try:
            url = f"{self.base_url}/rest/api/2/issue/{issue_key}/transitions"
            response = self.session.get(url)
            if response.status_code != 200:
                print(
                    f"    ⚠️  Failed to get transitions: {response.text}",
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
                response = self.session.post(url, json=payload)
                return response.status_code in [200, 204]
            else:
                print("    ⚠️  Could not find TODO transition", file=sys.stderr)
                return False
        except Exception as e:
            print(f"    ⚠️  Error transitioning to TODO: {e}", file=sys.stderr)
            return False


def process_cves(cves_file: str, parent_key: str, workflow_url: str):
    """Process CVEs and create/update JIRA tickets."""

    # Check if CVE file exists
    if not os.path.exists(cves_file):
        print(f"❌ CVE file not found: {cves_file}")
        sys.exit(1)

    # Load CVEs from file
    with open(cves_file, "r") as f:
        cves = json.load(f)

    cve_count = len(cves)
    print(f"📋 Processing {cve_count} CVE(s) from {cves_file}")

    # Get JIRA credentials from environment
    jira_base_url = os.environ.get("JIRA_BASE_URL")
    jira_user_email = os.environ.get("JIRA_USER_EMAIL")
    jira_api_token = os.environ.get("JIRA_API_TOKEN")

    if not all([jira_base_url, jira_user_email, jira_api_token]):
        print(
            "❌ Missing required environment variables: JIRA_BASE_URL, JIRA_USER_EMAIL, JIRA_API_TOKEN"
        )
        sys.exit(1)

    # Initialize JIRA client (assert to satisfy type checker - we've validated above)
    assert jira_base_url is not None
    assert jira_user_email is not None
    assert jira_api_token is not None
    jira = JiraClient(jira_base_url, jira_user_email, jira_api_token)

    created = 0
    updated = 0
    failed = 0

    # Process each CVE
    for i, cve in enumerate(cves):
        cve_id = cve.get("cve_id")
        title = cve.get("title")
        severity = cve.get("severity")
        package = cve.get("package")
        description = cve.get("description")

        print()
        print(f"🔒 Processing CVE {i+1}/{cve_count}: {cve_id}")

        # Check for existing ticket
        print("  🔍 Searching for existing ticket...")
        existing_key = jira.search_existing_ticket(cve_id, parent_key)

        if existing_key:
            print(f"  📌 Found existing ticket: {existing_key}")

            # Get ticket status
            status = jira.get_issue_status(existing_key)
            if status:
                print(f"  📊 Current status: {status}")

                # Reopen if closed
                if any(
                    keyword in status.lower()
                    for keyword in ["done", "closed", "resolved"]
                ):
                    print("  🔓 Reopening closed ticket...")
                    if jira.reopen_issue(existing_key):
                        print("  ✅ Ticket reopened")
                    else:
                        print("  ⚠️  Failed to reopen ticket")

            # Add comment
            print("  💬 Adding comment about CVE still present...")
            comment = f"""🔄 CVE Still Present in Latest Scan

CVE ID: {cve_id}
Workflow Run: {workflow_url}

This vulnerability is still present in the latest dependency scan. Please prioritize remediation."""

            if jira.add_comment(existing_key, comment):
                print("  ✅ Comment added")
            else:
                print("  ⚠️  Failed to add comment")

            updated += 1
            continue

        print("  ✨ No existing ticket found, creating new one...")

        # Create JIRA ticket summary
        summary = f"🔒 {cve_id}: {title}"

        # Create JIRA ticket description
        issue_desc = f"""**Security Vulnerability Detected**

**CVE ID:** {cve_id}
**Severity:** {severity.upper()}
**Affected Package:** {package}
**Workflow Run:** {workflow_url}

**Description:**
{description}

**Recommended Actions:**
1. Review the CVE details and assess impact
2. Check for available patches or updates
3. Update the affected package to a secure version
4. Re-run CVE scan to verify fix

_This ticket was automatically created by the Daily CVE Check workflow._"""

        # Create CVE label (lowercase)
        cve_label = cve_id.lower()

        # Create JIRA ticket payload
        payload = {
            "fields": {
                "project": {"key": "SNOW"},
                "issuetype": {"name": "Bug"},
                "summary": summary,
                "description": issue_desc,
                "parent": {"key": parent_key},
                "labels": ["dp-snowcli", "security", "cve", "automated", cve_label],
                "components": [{"id": "18653"}],
                "customfield_11401": {"id": "14723"},
            }
        }

        # Create the issue
        issue_key = jira.create_issue(payload)
        if issue_key:
            print(f"✅ Created JIRA ticket: {issue_key} for {cve_id}")
            if jira.transition_to_todo(issue_key):
                print(f"  ✅ Transitioned to TODO status")
            else:
                print(f"  ⚠️  Could not transition to TODO (may already be in TODO)")
            created += 1
        else:
            failed += 1

    # Print summary
    print()
    print("✨ Summary:")
    print(f"  - Created: {created} new ticket(s)")
    print(f"  - Updated: {updated} existing ticket(s)")
    print(f"  - Failed: {failed} ticket(s)")
    print(f"  - Total: {cve_count} CVE(s)")

    if failed > 0:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Create JIRA tickets from CVE scan results"
    )
    parser.add_argument(
        "cves_file",
        nargs="?",
        default="cves.json",
        help="Path to CVEs JSON file (default: cves.json)",
    )
    parser.add_argument(
        "parent_key",
        nargs="?",
        default="SNOW-2380150",
        help="JIRA parent ticket key (default: SNOW-2380150)",
    )
    parser.add_argument(
        "workflow_url", nargs="?", default="", help="GitHub workflow run URL"
    )

    args = parser.parse_args()
    process_cves(args.cves_file, args.parent_key, args.workflow_url)


if __name__ == "__main__":
    main()
