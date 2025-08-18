import os
import re
import pandas as pd

DATE_RE = re.compile(r'(\d{4}-\d{2}-\d{2})')  # capture YYYY-MM-DD
ASSIGNEE_RE = re.compile(r'Assignee:\s*(.*?)(?:\n|$)')  # capture name after 'Assignee:'

def _extract_reopen_events(row):
    """
    From one export row, parse the 'Custom field (Reopen log )' text and yield
    (issue_key, summary, assignee_name, date_str) tuples.
    """
    issue_key = row.get('Issue key', '')
    summary = row.get('Summary', '')
    text = str(row.get('Custom field (Reopen log )', '') or '')

    events = []
    # We assume each date belongs to the following 'Assignee: ...' on the same/next line.
    # We'll scan by lines for robustness.
    for block in text.splitlines():
        dmatch = DATE_RE.search(block)
        if not dmatch:
            continue
        date_str = dmatch.group(1)
        amatch = ASSIGNEE_RE.search(block)
        assignee_name = (amatch.group(1).strip() if amatch else (row.get('Assignee', '') or ''))
        events.append((issue_key, summary, assignee_name, date_str))
    return events

def process(input_csv_path, out_user_csv_path, out_ticket_csv_path):
    """
    Read export.csv, parse Reopen log lines, filter by env MONTH (YYYY-MM),
    and write:
      - reopens_by_user.csv (Assignee, Reopens Count)
      - reopens_by_ticket.csv (Issue key, Summary, Reopens Count)
    """
    month = os.environ.get("MONTH", "").strip()
    if not month or not re.match(r'^\d{4}-\d{2}$', month):
        raise ValueError("MONTH env var must be set to YYYY-MM (provided by workflow).")

    df = pd.read_csv(input_csv_path)
    if 'Custom field (Reopen log )' not in df.columns:
        raise ValueError("Expected column 'Custom field (Reopen log )' not found in export.")

    df['Custom field (Reopen log )'] = df['Custom field (Reopen log )'].fillna('')
    df['Assignee'] = df.get('Assignee', '').fillna('')

    # Collect reopen events (Issue key, Summary, Assignee, Date)
    all_events = []
    for _, row in df.iterrows():
        all_events.extend(_extract_reopen_events(row))

    if not all_events:
        # no events; write empty reports with headers
        pd.DataFrame(columns=['Assignee', 'Reopens Count']).to_csv(out_user_csv_path, index=False)
        pd.DataFrame(columns=['Issue key', 'Summary', 'Reopens Count']).to_csv(out_ticket_csv_path, index=False)
        return

    events_df = pd.DataFrame(all_events, columns=['Issue key', 'Summary', 'Assignee', 'Date'])
    # Filter to target month
    events_df['Date'] = pd.to_datetime(events_df['Date'], errors='coerce')
    events_df['Month'] = events_df['Date'].dt.to_period('M').astype(str)
    events_df = events_df[events_df['Month'] == month]

    # By user
    by_user = (
        events_df.groupby('Assignee')
        .size().reset_index(name='Reopens Count')
        .sort_values('Reopens Count', ascending=False)
    )
    by_user.to_csv(out_user_csv_path, index=False)

    # By ticket (count of events per Issue key)
    # By ticket + assignee (count events per Issue key & Assignee)
    by_ticket = (
        events_df
        .groupby(['Issue key', 'Issue Type', 'Summary', 'Assignee'])
        .size()
        .reset_index(name='Reopens Count')
        .sort_values(['Assignee', 'Issue key'], ascending=[True, True])
    )

    # Reorder columns exactly as required
    by_ticket = by_ticket[['Issue key', 'Issue Type', 'Summary', 'Reopens Count', 'Assignee']]

    by_ticket.to_csv(out_ticket_csv_path, index=False)
