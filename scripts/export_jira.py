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

Resolves customfield IDs by display name (with optional override via env).

Env (mapped via workflow):
  JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN
  [optional overrides]
  REOPEN_COUNT_FIELD_ID, REOPEN_LOG_FIELD_ID
  REOPEN_COUNT_FIELD_NAME, REOPEN_LOG_FIELD_NAME
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

# Defaults (can be overridden via env)
DEFAULT_REOPEN_COUNT_NAME = "Reopen Count"
DEFAULT_REOPEN_LOG_NAME   = "Reopen log"

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
    s = (s or "").strip().lower()
    if "[" in s:
        s = s.split("[", 1)[0].strip()
    return s

def resolve_cf_id(base_url, auth_header, wanted_name, wanted_id=None, all_fields=None):
    fields = all_fields or fetch_all_fields(base_url, auth_header)
    by_name, by_id = build_field_indexes(fields)

    if wanted_id:
        if wanted_id in by_id:
            return wanted_id, f"resolved by explicit ID: {wanted_id}"
        matches = [fid for fid in by_id if fid.endswith(wanted_id)]
        if matches:
            return matches[0], f"resolved by suffix match ID: {matches[0]}"
        return None, f"explicit ID '{wanted_id}' not found"

    norm = normalize_name(wanted_name)
    if norm in by_name:
        return by_name[norm][0]["id"], f"resolved by exact name: '{wanted_name}'"

    for key, lst in by_name.items():
        if normalize_name(key) == norm:
            return lst[0]["id"], f"resolved by normalized name: '{wanted_name}' -> '{key}'"
        if key.startswith(norm):
            return lst[0]["id"], f"resolved by prefix match: '{wanted_name}' -> '{key}'"

    for key, lst in by_name.items():
        if norm and norm in key:
            return lst[0]["id"], f"resolved by contains match: '{wanted_name}' -> '{key}'"

    candidates = []
    for f in fields:
        nm = (f.get("name") or "").lower()
        if "reopen" in nm:
            candidates.append(f"{f.get('id')} :: {f.get('name')}")
    debug = "not found. Reopen-like fields:\n  - " + "\n  - ".join(candidates) if candidates else "not found. No 'reopen' fields visible."
    return None, debug

def jira_search(base_url, auth_header, jql, start_at, fields):
    """
    Try multiple endpoints. Only fail after all candidates fail.
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
        "/rest/api/3/search",        # legacy (most sites support this)
        "/rest/api/3/issue/search",  # some sites expose this "enhanced" path
    ]

    errors = []
    for path in candidates:
        url = f"{base_url}{path}"
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=90)
        except Exception as e:
            errors.append((path, f"exception {type(e).__name__}: {e}"))
            continue

        if r.ok:
            print(f"[export] search OK via {path} (startAt={start_at})")
            return r.json()

        # Record the failure and try next candidate
        errors.append((path, f"{r.status_code} {r.text[:300]}"))
        print(f"[export] {path} failed ({r.status_code}); trying next…")

    # If we reach here, all candidates failed
    print("Jira search failed on all endpoints:", file=sys.stderr)
    for p, err in errors:
        print(f"  - {p}: {err}", file=sys.stderr)
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

    # Optional overrides from env
    cf_count_id_env = os.environ.get("REOPEN_COUNT_FIELD_ID")
    cf_log_id_env   = os.environ.get("REOPEN_LOG_FIELD_ID")
    cf_count_name   = os.environ.get("REOPEN_COUNT_FIELD_NAME", DEFAULT_REOPEN_COUNT_NAME)
    cf_log_name     = os.environ.get("REOPEN_LOG_FIELD_NAME",   DEFAULT_REOPEN_LOG_NAME)

    start, end = month_to_range(args.month)
    jql = (
        f'status CHANGED TO "Reopen" DURING ("{start}", "{end}") '
        f'AND "Reopen log [Short text]" IS NOT EMPTY'
    )

    auth_b64 = "Basic " + base64.b64encode(f"{email}:{token}".encode()).decode()

    # Resolve custom field IDs
    all_fields = fetch_all_fields(base_url, auth_b64)
    cf_reopen_count, info1 = resolve_cf_id(base_url, auth_b64, cf_count_name, cf_count_id_env, all_fields=all_fields)
    cf_reopen_log,   info2 = resolve_cf_id(base_url, auth_b64, cf_log_name,   cf_log_id_env,   all_fields=all_fields)
    print(f"[export] Reopen Count resolution: {info1}")
    print(f"[export] Reopen Log   resolution: {info2}")

    if not cf_reopen_count or not cf_reopen_log:
        print("ERROR: Could not resolve custom field IDs for Reopen Count / Reopen log.", file=sys.stderr)
        print("       Configure env overrides via Secrets if needed:", file=sys.stderr)
        print("       REOPEN_COUNT_FIELD_NAME / REOPEN_LOG_FIELD_NAME or REOPEN_COUNT_FIELD_ID / REOPEN_LOG_FIELD_ID", file=sys.stderr)
        sys.exit(1)

    # Request only fields we need (include issuetype!)
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
