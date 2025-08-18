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

Resolves custom field IDs by display name, so you don't hardcode IDs.
"""

HEADERS = [
    "Issue key",
    "Issue Type",  # after Issue key (per your latest ask)
    "Issue id",
    "Summary",
    "Assignee",
    "Assignee Id",
    "Custom field (Reopen Count)",
    "Custom field (Reopen log )",
]

CUSTOM_FIELD_NAMES = {
    "reopen_count": "Reopen Count",
    "reopen_log":   "Reopen log",  # adjust if your display name differs slightly
}

def month_to_range(month: str):
    try:
        y, m = month.split("-")
        y = int(y); m = int(m)
        last_day = calendar.monthrange(y, m)[1]
        return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last_day:02d}"
    except Exception:
        print("Bad month format. Use YYYY-MM (e.g., 2025-08)", file=sys.stderr)
        sys.exit(2)

def headers_auth(b64):  # tiny helper
    return {"Accept":"application/json","Content-Type":"application/json","Authorization":b64}

def get_field_map(base_url, auth_header):
    url = f"{base_url}/rest/api/3/field"
    r = requests.get(url, headers=headers_auth(auth_header), timeout=60)
    if not r.ok:
        print(f"Failed to read fields: {r.status_code} {r.text}", file=sys.stderr); sys.exit(1)
    by_name = {}
    for f in r.json():
        name = (f.get("name") or "").strip().lower()
        fid = f.get("id")
        if name and fid:
            by_name.setdefault(name, []).append(fid)
    return by_name

def resolve_cf_id(display_name: str, by_name: dict):
    norm = display_name.lower().strip()
    if norm in by_name: return by_name[norm][0]
    # try stripping bracket suffixes like "Reopen log [Short text]"
    stripped = norm.split("[",1)[0].strip()
    if stripped in by_name: return by_name[stripped][0]
    for k in by_name:
        if k.startswith(stripped): return by_name[k][0]
    return None

def jira_search(base_url, auth_header, jql, start_at, fields):
    url = f"{base_url}/rest/api/3/search"
    payload = {
        "jql": jql, "startAt": start_at, "maxResults": 100,
        "fieldsByKeys": True, "fields": fields
    }
    r = requests.post(url, json=payload, headers=headers_auth(auth_header), timeout=90)
    if not r.ok:
        print(f"Jira search failed: {r.status_code} {r.text}", file=sys.stderr); sys.exit(1)
    return r.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True)
    ap.add_argument("--out", default="export.csv")
    args = ap.parse_args()

    base_url = os.environ.get("JIRA_BASE_URL","").rstrip("/")
    email    = os.environ.get("JIRA_EMAIL","")
    token    = os.environ.get("JIRA_API_TOKEN","")
    if not base_url or not email or not token:
        print("Missing JIRA_BASE_URL / JIRA_EMAIL / JIRA_API_TOKEN", file=sys.stderr); sys.exit(1)

    start, end = month_to_range(args.month)
    jql = (f'status CHANGED TO "Reopen" DURING ("{start}", "{end}") '
           f'AND "Reopen log [Short text]" IS NOT EMPTY')

    auth_b64 = "Basic " + base64.b64encode(f"{email}:{token}".encode()).decode()

    # resolve customfield ids by name
    by_name = get_field_map(base_url, auth_b64)
    cf_reopen_count = resolve_cf_id(CUSTOM_FIELD_NAMES["reopen_count"], by_name)
    cf_reopen_log   = resolve_cf_id(CUSTOM_FIELD_NAMES["reopen_log"], by_name)
    if not cf_reopen_count or not cf_reopen_log:
        print("ERROR: could not resolve custom field IDs for Reopen Count / Reopen log", file=sys.stderr)
        sys.exit(1)

    # ask only what we need, including issuetype for the new column
    fields = ["issuetype", "key", "id", "summary", "assignee", cf_reopen_count, cf_reopen_log]

    rows = []
    start_at = 0
    while True:
        data = jira_search(base_url, auth_b64, jql, start_at, fields)
        for issue in data.get("issues", []):
            f = issue.get("fields") or {}
            issuetype_name = (f.get("issuetype") or {}).get("name","")
            assignee = f.get("assignee") or {}
            assignee_name = assignee.get("displayName","") or ""
            assignee_id   = assignee.get("accountId","") or ""
            reopen_count_val = f.get(cf_reopen_count, "")
            reopen_log_val   = f.get(cf_reopen_log, "")
            if reopen_log_val is None: reopen_log_val = ""
            if isinstance(reopen_log_val, list): reopen_log_val = "; ".join(map(str, reopen_log_val))

            rows.append([
                issue.get("key",""),
                issuetype_name,                # <-- Issue Type after Issue key
                issue.get("id",""),
                f.get("summary","") or "",
                assignee_name,
                assignee_id,
                reopen_count_val if reopen_count_val is not None else "",
                reopen_log_val,
            ])

        total = data.get("total",0); fetched = start_at + data.get("maxResults",0)
        if fetched >= total: break
        start_at = fetched

    with open(args.out, "w", newline="", encoding="utf-8") as fp:
        w = csv.writer(fp); w.writerow(HEADERS); w.writerows(rows)

    print(f"Wrote {args.out} with {len(rows)} rows for {args.month}")

if __name__ == "__main__":
    main()
