#!/usr/bin/env python3
import os
import sys
import csv
import argparse
import base64
import calendar
import requests

"""
Builds an export with the EXACT headers requested:

  Issue Type, Issue key, Issue id, Summary, Assignee, Assignee Id,
  Custom field (Reopen Count), Custom field (Reopen log )

It searches for issues that match:
  status CHANGED TO "Reopen" DURING (start, end)
  AND "Reopen log [Short text]" IS NOT EMPTY

It resolves the custom field IDs by display name ("Reopen Count", "Reopen log")
so you don't need to hardcode customfield_xxxxx.

Env (set by GitHub Actions secrets mapping):
  JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN

Usage:
  python scripts/export_jira.py --month 2025-08 --out export.csv
"""

HEADERS = [
    "Issue Type",
    "Issue key",
    "Issue id",
    "Summary",
    "Assignee",
    "Assignee Id",
    "Custom field (Reopen Count)",
    "Custom field (Reopen log )",   # note: space before ')' preserved as requested
]

CUSTOM_FIELD_NAMES = {
    "reopen_count": "Reopen Count",
    "reopen_log": "Reopen log",  # we will match case-insensitive and ignore bracket suffixes
}

def month_to_range(month: str):
    try:
        y, m = month.split("-")
        y = int(y); m = int(m)
        last_day = calendar.monthrange(y, m)[1]
        start = f"{y:04d}-{m:02d}-01"
        end = f"{y:04d}-{m:02d}-{last_day:02d}"
        return start, end
    except Exception:
        print("Bad month format. Use YYYY-MM (e.g., 2025-08)", file=sys.stderr)
        sys.exit(2)

def auth_headers(base64_basic: str):
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": base64_basic
    }

def get_field_map(base_url, auth_b64):
    """Return list of fields (id, name) and a helper to find customfield IDs by display name."""
    url = f"{base_url}/rest/api/3/field"
    r = requests.get(url, headers=auth_headers(auth_b64), timeout=60)
    if not r.ok:
        print(f"Failed to read fields: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    fields = r.json()
    # Build search structures
    by_name = {}
    for f in fields:
        # Jira returns {id:"customfield_12345", name:"Reopen Count", ...}
        name = (f.get("name") or "").strip()
        fid = f.get("id")
        if name and fid:
            by_name.setdefault(name.lower(), []).append(fid)
    return fields, by_name

def find_customfield_id(display_name: str, by_name: dict):
    """Fuzzy find a custom field id by display name (case-insensitive, tolerate bracket suffix)."""
    norm = display_name.lower().strip()
    # exact
    if norm in by_name:
        return by_name[norm][0]
    # fuzzy: strip anything in brackets and extra spaces
    stripped = norm.split("[", 1)[0].strip()
    if stripped in by_name:
        return by_name[stripped][0]
    # last resort: scan keys that start with stripped
    for k in by_name.keys():
        if k.startswith(stripped):
            return by_name[k][0]
    return None

def search_issues(base_url, auth_b64, jql, start_at, fields):
    url = f"{base_url}/rest/api/3/search"
    payload = {
        "jql": jql,
        "startAt": start_at,
        "maxResults": 100,
        "fieldsByKeys": True,
        "fields": fields,
    }
    r = requests.post(url, json=payload, headers=auth_headers(auth_b64), timeout=90)
    if not r.ok:
        print(f"Jira search failed: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    return r.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="YYYY-MM")
    ap.add_argument("--out", default="export.csv")
    args = ap.parse_args()

    base_url = os.environ.get("JIRA_BASE_URL", "").rstrip("/")
    email = os.environ.get("JIRA_EMAIL", "")
    token = os.environ.get("JIRA_API_TOKEN", "")

    if not base_url or not email or not token:
        print("Missing JIRA_BASE_URL / JIRA_EMAIL / JIRA_API_TOKEN", file=sys.stderr)
        sys.exit(1)

    start, end = month_to_range(args.month)

    jql = (
        f'status CHANGED TO "Reopen" DURING ("{start}", "{end}") '
        f'AND "Reopen log [Short text]" IS NOT EMPTY'
    )

    # Basic auth
    auth_b64 = "Basic " + base64.b64encode(f"{email}:{token}".encode()).decode()

    # Resolve custom field IDs by display name
    _, by_name = get_field_map(base_url, auth_b64)
    reopen_count_id = find_customfield_id(CUSTOM_FIELD_NAMES["reopen_count"], by_name)
    reopen_log_id   = find_customfield_id(CUSTOM_FIELD_NAMES["reopen_log"], by_name)

    if not reopen_count_id or not reopen_log_id:
        print("ERROR: Could not find custom field IDs.", file=sys.stderr)
        print(f"  Wanted names: '{CUSTOM_FIELD_NAMES['reopen_count']}', '{CUSTOM_FIELD_NAMES['reopen_log']}'", file=sys.stderr)
        print("  Make sure these names match Jira’s field display names.", file=sys.stderr)
        sys.exit(1)

    # Ask only for the columns we need
    fields = [
        "issuetype", "key", "id", "summary", "assignee",
        reopen_count_id, reopen_log_id
    ]

    rows = []
    start_at = 0
    while True:
        data = search_issues(base_url, auth_b64, jql, start_at, fields)
        for issue in data.get("issues", []):
            f = issue.get("fields", {}) or {}
            issuetype = (f.get("issuetype") or {}).get("name", "")
            key = issue.get("key", "")
            iid = issue.get("id", "")
            summary = f.get("summary", "") or ""
            assignee = f.get("assignee") or {}
            assignee_name = assignee.get("displayName", "") or ""
            assignee_id = assignee.get("accountId", "") or ""

            reopen_count_val = f.get(reopen_count_id, "")
            reopen_log_val   = f.get(reopen_log_id, "")

            # Normalize types
            if reopen_count_val is None:
                reopen_count_val = ""
            if isinstance(reopen_log_val, list):
                # in case it's a multi-line field returning list; join with '; '
                reopen_log_val = "; ".join([str(x) for x in reopen_log_val])

            rows.append([
                issuetype,
                key,
                iid,
                summary,
                assignee_name,
                assignee_id,
                reopen_count_val,
                reopen_log_val,
            ])

        total = data.get("total", 0)
        fetched = start_at + data.get("maxResults", 0)
        if fetched >= total:
            break
        start_at = fetched

    # Write with EXACT header names/order
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADERS)
        w.writerows(rows)

    print(f"Wrote {args.out} with {len(rows)} rows for {args.month}")

if __name__ == "__main__":
    main()
