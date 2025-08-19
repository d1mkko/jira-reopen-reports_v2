#!/usr/bin/env python3
import os
import sys
import csv
import argparse
import base64
import calendar
import requests

"""
Exports exact headers:

  Issue key, Issue Type, Issue id, Summary, Assignee, Assignee Id,
  Custom field (Reopen Count), Custom field (Reopen log )

Filters issues by:
  status CHANGED TO "Reopen" DURING (start, end)
  AND "Reopen log [Short text]" IS NOT EMPTY

Resolves custom field IDs by display name (so you don't hardcode IDs).

Env (mapped via workflow):
  JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN

Usage:
  python scripts/export_jira.py --month 2025-09 --out export.csv
"""

HEADERS = [
    "Issue key",
    "Issue Type",  # after Issue key
    "Issue id",
    "Summary",
    "Assignee",
    "Assignee Id",
    "Custom field (Reopen Count)",
    "Custom field (Reopen log )",
]

# Adjust display names if your site uses slightly different labels
CUSTOM_FIELD_NAMES = {
    "reopen_count": "Reopen Count",
    "reopen_log":   "Reopen log",  # e.g., Jira UI might show "Reopen log [Short text]"
}

def month_to_range(month: str):
    try:
        y, m = month.split("-")
        y = int(y); m = int(m)
        last_day = calendar.monthrange(y, m)[1]
        return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last_day:02d}"
    except Exception:
        print("Bad month format. Use YYYY-MM (e.g., 2025-09)", file=sys.stderr)
        sys.exit(2)

def _auth_headers(b64_basic: str):
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": b64_basic,
    }

def get_field_map(base_url, auth_header):
    """Fetch all fields and index by display name (lowercased)."""
    url = f"{base_url}/rest/api/3/field"
    r = requests.get(url, headers=_auth_headers(auth_header), timeout=60)
    if not r.ok:
        print(f"Failed to read fields: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    by_name = {}
    for f in r.json():
        name = (f.get("name") or "").strip().lower()
        fid = f.get("id")
        if name and fid:
            by_name.setdefault(name, []).append(fid)
    return by_name

def resolve_cf_id(display_name: str, by_name: dict):
    """Fuzzy resolve a customfield id by display name (case-insensitive, tolerates bracket suffixes)."""
    norm = display_name.lower().strip()
    if norm in by_name:
        return by_name[norm][0]
    stripped = norm.split("[", 1)[0].strip()
    if stripped in by_name:
        return by_name[stripped][0]
    for k in by_name.keys():
        if k.startswith(stripped):
            return by_name[k][0]
    return None

def jira_search(base_url, auth_header, jql, start_at, fields):
    """
    Try the newer enhanced search endpoint first; if it's removed/unavailable, fall back.
    Logs which path worked.
    """
    headers = _auth_headers(auth_header)
    payload = {
        "jql": jql,
        "startAt": start_at,
        "maxResults": 100,
        "fieldsByKeys": True,
        "fields": fields,
    }

    candidates = [
        "/rest/api/3/issue/search",  # enhanced path seen on newer sites
        "/rest/api/3/search",        # legacy path (still works on many)
    ]

    last = None
    for path in candidates:
        url = f"{base_url}{path}"
        r = requests.post(url, json=payload, headers=headers, timeout=90)
        last = r
        if r.status_code == 410:
            # Removed on this site; try next candidate
            print(f"[export] {path} returned 410 GONE, trying fallback…")
            continue
        if r.ok:
            print(f"[export] search OK via {path} (startAt={start_at})")
            return r.json()
        else:
            # Non-410 error (e.g., 400 bad JQL) — fail fast with details
            print(f"Jira search failed via {path}: {r.status_code} {r.text}", file=sys.stderr)
            sys.exit(1)

    if last is not None and last.status_code == 410:
        print("All search endpoints returned 410 GONE. Jira requires the enhanced search API on this site.", file=sys.stderr)
    else:
        print("Jira search failed and no fallback succeeded.", file=sys.stderr)
    sys.exit(1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True, help="YYYY-MM")
    ap.add_argument("--out", default="export.csv")
    args = ap.parse_args()

    base_url = os.environ.get("JIRA_BASE_URL", "").rstrip("/")
    email    = os.environ.get("JIRA_EMAIL", "")
    token    = os.environ.get("JIRA_API_TOKEN", "")
    if not base_url or not email or not token:
        print("Missing JIRA_BASE_URL / JIRA_EMAIL / JIRA_API_TOKEN", file=sys.stderr)
        sys.exit(1)

    start, end = month_to_range(args.month)
    jql = (
        f'status CHANGED TO "Reopen" DURING ("{start}", "{end}") '
        f'AND "Reopen log [Short text]" IS NOT EMPTY'
    )

    auth_b64 = "Basic " + base64.b64encode(f"{email}:{token}".encode()).decode()

    # Resolve custom field IDs
    by_name = get_field_map(base_url, auth_b64)
    cf_reopen_count = resolve_cf_id(CUSTOM_FIELD_NAMES["reopen_count"], by_name)
    cf_reopen_log   = resolve_cf_id(CUSTOM_FIELD_NAMES["reopen_log"], by_name)
    if not cf_reopen_count or not cf_reopen_log:
        print("ERROR: Could not resolve custom field IDs for 'Reopen Count' / 'Reopen log'.", file=sys.stderr)
        print("       Check display names in Jira or adjust CUSTOM_FIELD_NAMES.", file=sys.stderr)
        sys.exit(1)

    # Ask only for the fields we need (include issuetype!)
    fields = ["issuetype", "key", "id", "summary", "assignee", cf_reopen_count, cf_reopen_log]

    rows = []
    start_at = 0
    while True:
        data = jira_search(base_url, auth_b64, jql, start_at, fields)
        for issue in data.get("issues", []):
            f = issue.get("fields") or {}
            iss_type = (f.get("issuetype") or {}).get("name", "") or ""
            assignee = f.get("assignee") or {}
            assignee_name = assignee.get("displayName", "") or ""
            assignee_id   = assignee.get("accountId", "") or ""
            reopen_count_val = f.get(cf_reopen_count, "")
            reopen_log_val   = f.get(cf_reopen_log, "")
            if reopen_log_val is None: reopen_log_val = ""
            if isinstance(reopen_log_val, list): reopen_log_val = "; ".join(map(str, reopen_log_val))

            rows.append([
                issue.get("key", ""),
                iss_type,                           # Issue Type after Issue key
                issue.get("id", ""),
                f.get("summary", "") or "",
                assignee_name,
                assignee_id,
                "" if reopen_count_val is None else reopen_count_val,
                reopen_log_val,
            ])

        total = data.get("total", 0)
        fetched = start_at + data.get("maxResults", 0)
        if fetched >= total:
            break
        start_at = fetched

    with open(args.out, "w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(HEADERS)
        writer.writerows(rows)

    print(f"Wrote {args.out} with {len(rows)} rows for {args.month}")

if __name__ == "__main__":
    main()
