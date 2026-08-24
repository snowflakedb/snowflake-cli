#!/usr/bin/env python3
"""
This script reads CVEs from JSON and creates JIRA tickets.
It mimics what the gajira-create action does but in a loop.
"""

import argparse
import json
import os
import sys

from jira_client import JiraClient


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

    # Initialize JIRA client from environment
    jira = JiraClient.from_env()

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

        # Create CVE label (lowercase)
        cve_label = cve_id.lower()

        # Check for existing ticket
        print("  🔍 Searching for existing ticket...")
        existing_key = jira.search_existing_ticket(cve_label, parent_key)

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
