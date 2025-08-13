#!/usr/bin/env python3
import os, sys, csv, argparse, base64, calendar, requests

def month_to_range(month: str):
    try:
        y, m = month.split("-"); y = int(y); m = int(m)
        last = calendar.monthrange(y, m)[1]
        return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last:02d}"
    except Exception:
        print("Bad month format. Use YYYY-MM (e.g., 2025-08)", file=sys.stderr); sys.exit(2)

def jira_search(base_url, auth_header, jql, start_at, max_results=100, expand="changelog"):
    url = f"{base_url}/rest/api/3/search"
    headers = {"Accept":"application/json","Content-Type":"application/json","Authorization":auth_header}
    payload = {"jql": jql, "startAt": start_at, "maxResults": max_results, "expand":[expand], "fields":["id"]}
    r = requests.post(url, json=payload, headers=headers, timeout=60)
    if not r.ok:
        print(f"Jira search failed: {r.status_code} {r.text}", file=sys.stderr); sys.exit(1)
    return r.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", required=True)
    ap.add_argument("--out", default="export.csv")
    args = ap.parse_args()

    base_url = os.environ.get("JIRA_BASE_URL","").rstrip("/")
    email = os.environ.get("JIRA_EMAIL","")
    token = os.environ.get("JIRA_API_TOKEN","")
    if not base_url or not email or not token:
        print("Missing JIRA_BASE_URL/JIRA_EMAIL/JIRA_API_TOKEN", file=sys.stderr); sys.exit(1)

    start, end = month_to_range(args.month)
    jql = (f'status CHANGED TO "Reopen" DURING ("{start}", "{end}") '
           f'AND "Reopen log [Short text]" IS NOT EMPTY')
    auth = "Basic " + base64.b64encode(f"{email}:{token}".encode()).decode()

    header = ["issueKey","transitionAt","authorAccountId","authorName","from","to"]
    rows = []
    start_at = 0
    while True:
        data = jira_search(base_url, auth, jql, start_at)
        for issue in data.get("issues", []):
            key = issue.get("key")
            histories = (issue.get("changelog") or {}).get("histories", [])
            for h in histories:
                created = h.get("created") or ""
                day = created[:10]
                if day < start or day > end: continue
                for it in h.get("items", []):
                    if it.get("field") == "status" and (it.get("toString") or "").lower() == "reopen":
                        rows.append([ key, created,
                                      (h.get("author") or {}).get("accountId",""),
                                      (h.get("author") or {}).get("displayName",""),
                                      it.get("fromString","") or "", it.get("toString","") or "" ])
        total = data.get("total", 0)
        fetched = start_at + data.get("maxResults", 0)
        if fetched >= total: break
        start_at = fetched

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    print(f"Wrote {args.out} with {len(rows)} rows for {args.month}")

if __name__ == "__main__":
    main()
