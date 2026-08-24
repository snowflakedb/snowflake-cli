#!/usr/bin/env python3
"""
Create (or reopen) a JIRA ticket when a newer Python patch is available
than the one pinned in scripts/packaging/build_python.sh.
"""

import argparse
import sys

from jira_client import JiraClient


def process_update(
    current_version: str, latest_version: str, parent_key: str, workflow_url: str
):
    python_label = f"python-{latest_version.replace('.', '-')}"

    jira = JiraClient.from_env()

    print(f"🔍 Searching for existing ticket for {latest_version}...")
    existing_key = jira.search_existing_ticket(python_label, parent_key)

    if existing_key:
        print(f"📌 Found existing ticket: {existing_key}")

        status = jira.get_issue_status(existing_key)
        if status:
            print(f"📊 Current status: {status}")

            if any(
                keyword in status.lower() for keyword in ["done", "closed", "resolved"]
            ):
                print("🔓 Reopening closed ticket...")
                if jira.reopen_issue(existing_key):
                    print("✅ Ticket reopened")
                    comment = f"""🔄 Python Patch Still Not Bundled

Latest available: {latest_version}
Workflow Run: {workflow_url}

This ticket was reopened because a newer Python patch is still not bundled. Please prioritize updating PYTHON_VERSION in scripts/packaging/build_python.sh."""
                    if jira.add_comment(existing_key, comment):
                        print("✅ Comment added")
                    else:
                        print("⚠️  Failed to add comment")
                else:
                    print("⚠️  Failed to reopen ticket")
            else:
                print("ℹ️  Ticket is already open, skipping (no daily comment spam)")
        return

    print("✨ No existing ticket found, creating new one...")

    summary = (
        f"🐍 New Python patch available: {latest_version} (current: {current_version})"
    )

    issue_desc = f"""**Newer Python Patch Available**

**Currently Bundled:** {current_version}
**Latest Published:** {latest_version}
**Release Page:** https://www.python.org/downloads/release/python-{latest_version.replace('.', '')}/
**Workflow Run:** {workflow_url}

**Recommended Actions:**
1. Review the release notes for {latest_version} (security fixes, bugfixes)
2. Update PYTHON_VERSION in scripts/packaging/build_python.sh to {latest_version}
3. Rebuild and verify the bundled Python still passes packaging tests

_This ticket was automatically created by the Daily Python Version Check workflow._"""

    payload = {
        "fields": {
            "project": {"key": "SNOW"},
            "issuetype": {"name": "Bug"},
            "summary": summary,
            "description": issue_desc,
            "parent": {"key": parent_key},
            "labels": ["dp-snowcli", "python-version", "automated", python_label],
            "components": [{"id": "18653"}],
            "customfield_11401": {"id": "14723"},
        }
    }

    issue_key = jira.create_issue(payload)
    if issue_key:
        print(f"✅ Created JIRA ticket: {issue_key} for {latest_version}")
        if jira.transition_to_todo(issue_key):
            print("✅ Transitioned to TODO status")
        else:
            print("⚠️  Could not transition to TODO (may already be in TODO)")
    else:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Create a JIRA ticket when a newer Python patch is available"
    )
    parser.add_argument("current_version", help="Currently pinned Python version")
    parser.add_argument("latest_version", help="Latest published Python version")
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
    process_update(
        args.current_version, args.latest_version, args.parent_key, args.workflow_url
    )


if __name__ == "__main__":
    main()
