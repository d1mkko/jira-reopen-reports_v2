import os
import re
import pandas as pd

DATE_RE = re.compile(r'(\d{4}-\d{2}-\d{2})')            # YYYY-MM-DD
ASSIGNEE_RE = re.compile(r'Assignee:\s*(.*?)(?:\n|$)')  # name after 'Assignee:'

def _extract_reopen_events(row):
    """
    Parse 'Custom field (Reopen log )' and yield tuples:
      (Issue key, Issue Type, Summary, Assignee, Date)
    """
    issue_key  = row.get('Issue key', '')
    issue_type = row.get('Issue Type', '')  # <-- now taken from export.csv
    summary    = row.get('Summary', '')
    text       = str(row.get('Custom field (Reopen log )', '') or '')

    events = []
    for line in text.splitlines():
        dmatch = DATE_RE.search(line)
        if not dmatch:
            continue
        date_str = dmatch.group(1)
        amatch = ASSIGNEE_RE.search(line)
        assignee_name = (amatch.group(1).strip() if amatch else (row.get('Assignee','') or ''))
        events.append((issue_key, issue_type, summary, assignee_name, date_str))
    return events

def process(input_csv_path, out_user_csv_path, out_ticket_csv_path):
    """
    Reads export.csv, filters by env MONTH (YYYY-MM),
    writes:
      - reopens_by_user.csv (Assignee, Reopens Count)
      - reopens_by_ticket.csv (Issue key, Issue Type, Summary, Reopens Count, Assignee)
    """
    month = os.environ.get("MONTH", "").strip()
    if not month or not re.match(r'^\d{4}-\d{2}$', month):
        raise ValueError("MONTH env var must be set to YYYY-MM (provided by workflow).")

    df = pd.read_csv(input_csv_path)
    if 'Custom field (Reopen log )' not in df.columns:
        raise ValueError("Expected column 'Custom field (Reopen log )' not found in export.")

    df['Custom field (Reopen log )'] = df['Custom field (Reopen log )'].fillna('')
    if 'Assignee' in df.columns:
        df['Assignee'] = df['Assignee'].fillna('')

    # Collect reopen events with Issue Type carried through
    all_events = []
    for _, row in df.iterrows():
        all_events.extend(_extract_reopen_events(row))

    # No events? write empty CSVs with headers and return
    if not all_events:
        pd.DataFrame(columns=['Assignee', 'Reopens Count']).to_csv(out_user_csv_path, index=False)
        pd.DataFrame(columns=['Issue key','Issue Type','Summary','Reopens Count','Assignee']).to_csv(out_ticket_csv_path, index=False)
        return

    events_df = pd.DataFrame(all_events, columns=['Issue key','Issue Type','Summary','Assignee','Date'])
    events_df['Date'] = pd.to_datetime(events_df['Date'], errors='coerce')
    events_df['Month'] = events_df['Date'].dt.to_period('M').astype(str)
    events_df = events_df[events_df['Month'] == month]

    # --- By user ---
    by_user = (
        events_df.groupby('Assignee')
        .size().reset_index(name='Reopens Count')
        .sort_values('Reopens Count', ascending=False)
    )
    by_user.to_csv(out_user_csv_path, index=False)

    # --- By ticket (with Issue Type) ---
    by_ticket = (
        events_df
        .groupby(['Issue key', 'Issue Type', 'Summary', 'Assignee'])
        .size().reset_index(name='Reopens Count')
        # sort primarily by Assignee (A→Z), then Issue key
        .sort_values(['Assignee', 'Issue key'], ascending=[True, True])
    )
    # Column order exactly as requested
    by_ticket = by_ticket[['Issue key', 'Issue Type', 'Summary', 'Reopens Count', 'Assignee']]
    by_ticket.to_csv(out_ticket_csv_path, index=False)
