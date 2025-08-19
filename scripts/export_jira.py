#!/usr/bin/env python3
import os
import sys
import csv
import argparse
import base64
import calendar
import requests

"""
Exports exact headers (in this order):

  Issue key, Issue Type, Issue id, Summary, Assignee, Assignee Id,
  Custom field (Reopen Count), Custom field (Reopen log )

Resolves customfield IDs by:
  1) Direct ID env (REOPEN_COUNT_FIELD_ID / REOPEN_LOG_FIELD_ID)
  2) Display name env (REOPEN_COUNT_FIELD_NAME / REOPEN_LOG_FIELD_NAME)
  3) Fuzzy match against /rest/api/3/field (case-insensitive, strips bracket suffixes)

If resolution fails, prints top 'reopen' candidates to help configure names.

Also uses enhanced search endpoint first, falls back if needed.
"""

HEADERS = [
    "Issue key",
    "Issue Type",
    "Issue id",
    "Summary",
    "Assignee",
    "Assignee Id",
    "Custom field (Reopen Count)",
    "Custom field (Reopen log )",
]

# Default display names (can be overridden via env)
DEFAULT_REOPEN_COUNT_NAME = "Reopen Count"
DEFAULT_REOPEN_LOG_NAME   = "Reopen log"  # sometimes appears as "Reopen log [Short text]"

def month_to_range(month: str):
    import re
    if not re.match(r"^\d{4}-\d{2}$", month or ""):
        print("Bad month format. Use YYYY-MM (e.g., 2025-09)", file=sys.stderr)
        sys.exit(2)
    y, m = map(int, month.split("-"))
    last = calendar.monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last:02d}"

def _auth_headers(b64_basic: str):
    return {"Accept":"application/json","Content-Type":"application/json","Authorization":b64_basic}

def fetch_all_fields(base_url, auth_header):
    url = f"{base_url}/rest/api/3/field"
    r = requests.get(url, headers=_auth_headers(auth_header), timeout=60)
    if not r.ok:
        print(f"Failed to read fields: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    return r.json()

def build_field_indexes(fields_json):
    by_name = {}
    by_id = {}
    for f in fields_json:
        fid = f.get("id")
        name = (f.get("name") or "").strip()
        if fid:
            by_id[fid] = f
        if name:
            by_name.setdefault(name.lower(), []).append(f)
    return by_name, by_id

def normalize_name(s: str) -> str:
    # Lowercase, strip, remove bracket suffix like " [Short text]"
    s = (s or "").strip().lower()
    if "[" in s:
        s = s.split("[", 1)[0].strip()
    return s

def resolve_cf_id(base_url, auth_header, wanted_name, wanted_id=None, all_fields=None):
    """
    Return (field_id, debug_info). If wanted_id provided, just validate it exists.
    """
    fields = all_fields or fetch_all_fields(base_url, auth_header)
    by_name, by_id = build_field_indexes(fields)

    # If ID given, validate and return
    if wanted_id:
        if wanted_id in by_id:
            return wanted_id, f"resolved by explicit ID: {wanted_id}"
        else:
            # sometimes Jira returns 'customfield_12345' but site has a different prefix
            matches = [f for f in by_id.keys() if f.endswith(wanted_id)]
            if matches:
                return matches[0], f"resolved by suffix match to ID: {matches[0]}"
            return None, f"explicit ID '{wanted_id}' not found"

    # Try exact name, fuzzy variants
    norm = normalize_name(wanted_name)
    # 1) exact (lowercased)
    if norm in by_name:
        return by_name[norm][0]["id"], f"resolved by exact name: '{wanted_name}'"

    # 2) scan for startswith norm
    for key, lst in by_name.items():
        if normalize_name(key) == norm:
            return lst[0]["id"], f"resolved by normalized name: '{wanted_name}' -> '{key}'"
        if key.startswith(norm):
            return lst[0]["id"], f"resolved by prefix match: '{wanted_name}' -> '{key}'"

    # 3) scan for contains norm (last resort)
    for key, lst in by_name.items():
        if norm and norm in key:
            return lst[0]["id"], f"resolved by contains match: '{wanted_name}' -> '{key}'"

    # Not found; return helpful suggestions
    candidates = []
    for f in fields:
        nm = (f.get("name") or "").lower()
        if "reopen" in nm:
            candidates.append(f"{f.get('id')} :: {f.get('name')}")
    debug = "not found. Reopen-like fields:\n  - " + "\n  - ".join(candidates) if candidates else "not found. No 'reopen' fields visible."
    return None, debug

def jira_search(base_url, auth_header, jql, start_at, fields):
    headers = _auth_headers(auth_header)
    payload = {
        "jql": jql,
        "startAt": start_at,
        "maxResults": 100,
        "fieldsByKeys": True,
        "fields": fields,
    }
    candidates = [
        "/rest/api/3/issue/search",  # enhanced (some rollouts)
        "/rest/api/3/search",        # legacy
    ]
    last = None
    for path in candidates:
        url = f"{base_url}{path}"
        r = requests.post(url, json=payload, headers=headers, timeout=90)
        last = r
        if r.status_code == 410:
            print(f"[export] {path} returned 410 GONE; trying next…")
            continue
        if r.ok:
            print(f"[export] search OK via {path} (startAt={start_at})")
            return r.json()
        else:
            # fail fast for non-410 errors to help debugging
            print(f"Jira search failed via {path}: {r.status_code} {r.text}", file=sys.stderr)
            sys.exit(1)
    if last is not None and last.status_code == 410:
        print("All search endpoints returned 410 GONE. Enhanced search required.", file=sys.stderr)
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

    # Optional overrides:
    cf_count_id_env = os.environ.get("REOPEN_COUNT_FIELD_ID")      # ex: customfield_12345
    cf_log_id_env   = os.environ.get("REOPEN_LOG_FIELD_ID")
    cf_count_name   = os.environ.get("REOPEN_COUNT_FIELD_NAME", DEFAULT_REOPEN_COUNT_NAME)
    cf_log_name     = os.environ.get("REOPEN_LOG_FIELD_NAME",   DEFAULT_REOPEN_LOG_NAME)

    if not base_url or not email or not token:
        print("Missing JIRA_BASE_URL / JIRA_EMAIL / JIRA_API_TOKEN env vars.", file=sys.stderr)
        sys.exit(1)

    start, end = month_to_range(args.month)
    jql = (
        f'status CHANGED TO "Reopen" DURING ("{start}", "{end}") '
        f'AND "Reopen log [Short text]" IS NOT EMPTY'
    )

    auth_b64 = "Basic " + base64.b64encode(f"{email}:{token}".encode()).decode()

    # Resolve custom field IDs with robust strategy
    all_fields = fetch_all_fields(base_url, auth_b64)
    cf_reopen_count, info1 = resolve_cf_id(base_url, auth_b64, cf_count_name, cf_count_id_env, all_fields=all_fields)
    cf_reopen_log,   info2 = resolve_cf_id(base_url, auth_b64, cf_log_name,   cf_log_id_env,   all_fields=all_fields)

    print(f"[export] Reopen Count resolution: {info1}")
    print(f"[export] Reopen Log   resolution: {info2}")

    if not cf_reopen_count or not cf_reopen_log:
        # Help user by listing likely matches
        print("ERROR: Could not resolve custom field IDs for Reopen Count / Reopen log.", file=sys.stderr)
        print("       You can set either the *names* or the *IDs* via env:", file=sys.stderr)
        print("         REOPEN_COUNT_FIELD_NAME / REOPEN_LOG_FIELD_NAME", file=sys.stderr)
        print("         REOPEN_COUNT_FIELD_ID   / REOPEN_LOG_FIELD_ID", file=sys.stderr)
        # Print candidates with 'reopen' in name:
        candidates = []
        for f in all_fields:
            name = (f.get("name") or "").lower()
            if "reopen" in name:
                candidates.append(f"{f.get('id')} :: {f.get('name')}")
        if candidates:
            print("       Reopen-like fields found:", file=sys.stderr)
            for c in candidates:
                print(f"         - {c}", file=sys.stderr)
        sys.exit(1)

    # Request only required fields (include issuetype for the new column)
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
                iss_type,
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
        w = csv.writer(fp)
        w.writerow(HEADERS)
        w.writerows(rows)

    print(f"Wrote {args.out} with {len(rows)} rows for {args.month}")

if __name__ == "__main__":
    main()
