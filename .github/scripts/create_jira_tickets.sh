#!/bin/bash
set -e

# This script reads CVEs from JSON and creates JIRA tickets using gajira-create action
# It mimics what the action does but in a loop

CVES_FILE="${1:-cves.json}"
PARENT_KEY="${2:-SNOW-2380150}"
WORKFLOW_URL="${3:-}"

if [ ! -f "$CVES_FILE" ]; then
    echo "❌ CVE file not found: $CVES_FILE"
    exit 1
fi

# Count CVEs
CVE_COUNT=$(jq length "$CVES_FILE")
echo "📋 Processing $CVE_COUNT CVE(s) from $CVES_FILE"

CREATED=0
FAILED=0
UPDATED=0

# Function to search for existing JIRA ticket
search_existing_ticket() {
    local cve_id="$1"
    local parent_key="$2"

    # Convert CVE ID to lowercase for label search
    local cve_label=$(echo "$cve_id" | tr '[:upper:]' '[:lower:]')

    # Build JQL query - search by label (don't quote labels in JQL)
    local jql="project = SNOW AND parent = ${parent_key} AND labels = ${cve_label} AND labels = automated"

    # URL encode the JQL
    local encoded_jql=$(echo "$jql" | jq -sRr @uri)

    # Search for existing ticket using v3 API
    local search_response=$(curl -s -w "\n%{http_code}" \
        -X GET \
        -H "Content-Type: application/json" \
        -u "$JIRA_USER_EMAIL:$JIRA_API_TOKEN" \
        "$JIRA_BASE_URL/rest/api/3/search/jql?jql=$encoded_jql&maxResults=1")

    local http_code=$(echo "$search_response" | tail -n1)
    local response_body=$(echo "$search_response" | sed '$d')

    if [ "$http_code" = "200" ]; then
        # v3 API doesn't have .total field, check issues array length instead
        local issues_count=$(echo "$response_body" | jq '.issues | length')
        if [ "$issues_count" -gt 0 ]; then
            # Get the issue key from first result
            local issue_key=$(echo "$response_body" | jq -r '.issues[0].key // empty')
            if [ -n "$issue_key" ]; then
                echo "$issue_key"
                return 0
            else
                # If key not in response, fetch it using the ID
                local issue_id=$(echo "$response_body" | jq -r '.issues[0].id')
                local key_response=$(curl -s \
                    -X GET \
                    -H "Content-Type: application/json" \
                    -u "$JIRA_USER_EMAIL:$JIRA_API_TOKEN" \
                    "$JIRA_BASE_URL/rest/api/2/issue/$issue_id?fields=key")
                issue_key=$(echo "$key_response" | jq -r '.key')
                echo "$issue_key"
                return 0
            fi
        fi
    else
        echo "    ⚠️  Search failed: $response_body" >&2
    fi

    # No existing ticket found
    return 1
}

# Process each CVE
for i in $(seq 0 $(($CVE_COUNT - 1))); do
    CVE_ID=$(jq -r ".[$i].cve_id" "$CVES_FILE")
    TITLE=$(jq -r ".[$i].title" "$CVES_FILE")
    SEVERITY=$(jq -r ".[$i].severity" "$CVES_FILE")
    PACKAGE=$(jq -r ".[$i].package" "$CVES_FILE")
    DESCRIPTION=$(jq -r ".[$i].description" "$CVES_FILE")

    echo ""
    echo "🔒 Processing CVE $((i+1))/$CVE_COUNT: $CVE_ID"

    # Check for existing ticket
    echo "  🔍 Searching for existing ticket..."
    if EXISTING_KEY=$(search_existing_ticket "$CVE_ID" "$PARENT_KEY"); then
        echo "  📌 Found existing ticket: $EXISTING_KEY"

        # Get ticket status
        STATUS_RESPONSE=$(curl -s \
            -X GET \
            -H "Content-Type: application/json" \
            -u "$JIRA_USER_EMAIL:$JIRA_API_TOKEN" \
            "$JIRA_BASE_URL/rest/api/2/issue/$EXISTING_KEY?fields=status")

        STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.fields.status.name')
        echo "  📊 Current status: $STATUS"

        # Reopen if closed
        if echo "$STATUS" | grep -qi -E "done|closed|resolved"; then
            echo "  🔓 Reopening closed ticket..."
            # Get available transitions
            TRANS_RESPONSE=$(curl -s \
                -X GET \
                -H "Content-Type: application/json" \
                -u "$JIRA_USER_EMAIL:$JIRA_API_TOKEN" \
                "$JIRA_BASE_URL/rest/api/2/issue/$EXISTING_KEY/transitions")

            # Find transition to reopen (TODO, Open, Reopened, etc.)
            TRANS_ID=$(echo "$TRANS_RESPONSE" | jq -r '.transitions[] | select(.name | test("TODO|Open|Reopen"; "i")) | .id' | head -1)

            if [ -n "$TRANS_ID" ]; then
                curl -s -X POST \
                    -H "Content-Type: application/json" \
                    -u "$JIRA_USER_EMAIL:$JIRA_API_TOKEN" \
                    "$JIRA_BASE_URL/rest/api/2/issue/$EXISTING_KEY/transitions" \
                    -d "{\"transition\":{\"id\":\"$TRANS_ID\"}}" > /dev/null
                echo "  ✅ Ticket reopened"
            else
                echo "  ⚠️  Could not find transition to reopen"
            fi
        fi

        # Add comment
        echo "  💬 Adding comment about CVE still present..."
        COMMENT="🔄 CVE Still Present in Latest Scan

CVE ID: $CVE_ID
Workflow Run: $WORKFLOW_URL

This vulnerability is still present in the latest dependency scan. Please prioritize remediation."
        curl -s -X POST \
            -H "Content-Type: application/json" \
            -u "$JIRA_USER_EMAIL:$JIRA_API_TOKEN" \
            "$JIRA_BASE_URL/rest/api/2/issue/$EXISTING_KEY/comment" \
            -d "{\"body\":$(echo "$COMMENT" | jq -Rs .)}" > /dev/null
        echo "  ✅ Comment added"

        UPDATED=$((UPDATED + 1))
        continue
    fi
    echo "  ✨ No existing ticket found, creating new one..."

    # Create JIRA ticket summary
    SUMMARY="🔒 $CVE_ID: $TITLE"

    # Create JIRA ticket description
    ISSUE_DESC="**Security Vulnerability Detected**

**CVE ID:** $CVE_ID
**Severity:** ${SEVERITY^^}
**Affected Package:** $PACKAGE
**Workflow Run:** $WORKFLOW_URL

**Description:**
$DESCRIPTION

**Recommended Actions:**
1. Review the CVE details and assess impact
2. Check for available patches or updates
3. Update the affected package to a secure version
4. Re-run CVE scan to verify fix

_This ticket was automatically created by the Daily CVE Check workflow._"

    # Create temp file for the description (jq needs proper JSON escaping)
    DESC_FILE=$(mktemp)
    echo "$ISSUE_DESC" > "$DESC_FILE"
    DESC_JSON=$(jq -Rs . "$DESC_FILE")
    rm "$DESC_FILE"

    # Create CVE label (lowercase)
    CVE_LABEL=$(echo "$CVE_ID" | tr '[:upper:]' '[:lower:]')

    # Create JIRA ticket using REST API (same as gajira-create does)
    PAYLOAD=$(jq -n \
        --arg summary "$SUMMARY" \
        --arg description "$ISSUE_DESC" \
        --arg parent "$PARENT_KEY" \
        --arg cve_label "$CVE_LABEL" \
        '{
            fields: {
                project: {key: "SNOW"},
                issuetype: {name: "Bug"},
                summary: $summary,
                description: $description,
                parent: {key: $parent},
                labels: ["dp-snowcli", "security", "cve", "automated", $cve_label],
                components: [{id: "18653"}],
                customfield_11401: {id: "14723"}
            }
        }')

    # Make the API call
    RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -u "$JIRA_USER_EMAIL:$JIRA_API_TOKEN" \
        "$JIRA_BASE_URL/rest/api/2/issue" \
        -d "$PAYLOAD")

    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')

    if [ "$HTTP_CODE" = "201" ]; then
        ISSUE_KEY=$(echo "$RESPONSE_BODY" | jq -r '.key')
        echo "✅ Created JIRA ticket: $ISSUE_KEY for $CVE_ID"
        CREATED=$((CREATED + 1))
    else
        echo "❌ Failed to create ticket for $CVE_ID (HTTP $HTTP_CODE)"
        echo "Response: $RESPONSE_BODY" | jq '.' 2>/dev/null || echo "$RESPONSE_BODY"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "✨ Summary:"
echo "  - Created: $CREATED new ticket(s)"
echo "  - Updated: $UPDATED existing ticket(s)"
echo "  - Failed: $FAILED ticket(s)"
echo "  - Total: $CVE_COUNT CVE(s)"

if [ $FAILED -gt 0 ]; then
    exit 1
fi
