#!/usr/bin/env python3
"""
Parse Snyk SARIF output and extract CVE information.
Outputs CVE data as JSON for GitHub Actions to process.
"""

import json
import os
import sys
from typing import Any


def parse_sarif_file(sarif_path: str) -> list[dict[str, Any]]:
    """Parse SARIF file and extract CVE information."""
    with open(sarif_path, "r") as f:
        sarif_data = json.load(f)

    cves = []

    for run in sarif_data.get("runs", []):
        for result in run.get("results", []):
            rule_id = result.get("ruleId", "")

            # Skip if no rule ID
            if not rule_id:
                continue

            # Get rule details
            rule = None
            for r in run.get("tool", {}).get("driver", {}).get("rules", []):
                if r.get("id") == rule_id:
                    rule = r
                    break

            if not rule:
                continue

            # Extract CVE/CWE identifiers
            cve_ids = []
            properties = rule.get("properties", {})
            cves_list = properties.get("cve", [])
            if isinstance(cves_list, list):
                cve_ids.extend(cves_list)

            # If no CVE, use the Snyk ID
            if not cve_ids:
                cve_ids = [rule_id]

            # Skip license issues
            if rule_id.startswith("snyk:lic:"):
                continue

            # Get affected package
            locations = result.get("locations", [])
            package = "Unknown"
            if locations:
                logical_location = (
                    locations[0]
                    .get("physicalLocation", {})
                    .get("artifactLocation", {})
                    .get("uri", "")
                )
                if logical_location:
                    package = logical_location.split("/")[-1]

            # Extract from message if package not found
            message = result.get("message", {}).get("text", "")
            if "in " in message and package == "Unknown":
                parts = message.split("in ")
                if len(parts) > 1:
                    package = parts[1].split()[0]

            severity = properties.get("security-severity", "medium")
            if isinstance(severity, str) and severity.replace(".", "").isdigit():
                # Convert CVSS score to severity
                score = float(severity)
                if score >= 9.0:
                    severity = "critical"
                elif score >= 7.0:
                    severity = "high"
                elif score >= 4.0:
                    severity = "medium"
                else:
                    severity = "low"

            for cve_id in cve_ids:
                cves.append(
                    {
                        "cve_id": cve_id,
                        "title": rule.get("shortDescription", {}).get(
                            "text", "Security Vulnerability"
                        ),
                        "description": message,
                        "severity": severity,
                        "package": package,
                    }
                )

    return cves


def main():
    sarif_path = os.environ.get("SARIF_FILE", "snyk-cve.sarif")
    output_file = os.environ.get("CVE_OUTPUT_FILE", "cves.json")

    if not os.path.exists(sarif_path):
        print(f"❌ SARIF file not found: {sarif_path}")
        sys.exit(1)

    print(f"📊 Parsing SARIF file: {sarif_path}")
    cves = parse_sarif_file(sarif_path)

    print(f"🔍 Found {len(cves)} CVE(s)")

    # Write to output file
    with open(output_file, "w") as f:
        json.dump(cves, f, indent=2)

    print(f"✅ Wrote {len(cves)} CVE(s) to {output_file}")

    # Also output for GitHub Actions
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"cve_count={len(cves)}\n")
            if cves:
                f.write("has_cves=true\n")
            else:
                f.write("has_cves=false\n")


if __name__ == "__main__":
    main()
