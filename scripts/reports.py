import pandas as pd
from io import StringIO
import re

# --- USER CONFIGURATION ---
# Define the month for which you need the report in 'YYYY-MM' format.
REPORT_MONTH = '2025-09'

# You will need to replace 'your_jira_export.csv' with the actual
# name of your file.
try:
    df = pd.read_csv('Jira.csv')
except FileNotFoundError:
    print("Error: The file 'Jira.csv' was not found.")
    print("Please make sure the file is in the same directory as this script.")
    exit()

# The 'Reopen log' column might contain NaN values or be empty, so we fill them
# with an empty string to avoid errors during string processing.
df['Custom field (Reopen log )'] = df['Custom field (Reopen log )'].fillna('')

# --- 1. Calculate Reopens Per Assignee for the specified month ---
# This section calculates the number of reopens that occurred for each assignee
# within the month defined by REPORT_MONTH.

# Initialize a list to store the reopen events
reopen_data = []

# Iterate through each row of the DataFrame
for index, row in df.iterrows():
    # Use a regular expression to find all log entries within the cell.
    for match in re.finditer(r'(\d{4}-\d{2}-\d{2}).*?Assignee: (.*?)(\n|$)', row['Custom field (Reopen log )']):
        date_str = match.group(1)
        assignee_name = match.group(2).strip()
        
        # Append the extracted issue key, assignee, and date to our list
        reopen_data.append({
            'Issue key': row['Issue key'],
            'Assignee': assignee_name,
            'Date': pd.to_datetime(date_str)
        })

# Create a new DataFrame from the collected reopen events
reopens_df = pd.DataFrame(reopen_data)

# Extract the month from the 'Date' column and format it as a period ('YYYY-MM')
reopens_df['Month'] = reopens_df['Date'].dt.to_period('M')

# Filter the DataFrame to include only reopen events from the specified month.
filtered_reopens = reopens_df[reopens_df['Month'] == REPORT_MONTH]

# Group by 'Assignee' and count the occurrences for the filtered month.
reopens_per_assignee = filtered_reopens.groupby(['Assignee']).size().reset_index(name='Reopens Count')

# Save the result to a new CSV file
output_filename_monthly = f'reopens_per_assignee_{REPORT_MONTH}.csv'
reopens_per_assignee.to_csv(output_filename_monthly, index=False)
print(f"Reopen stats for '{REPORT_MONTH}' have been saved to '{output_filename_monthly}'")


# --- 2. Calculate Reopens Per Ticket for the specified month ---
# This section calculates the number of reopens per ticket that occurred
# in the month defined by REPORT_MONTH.

# Group the filtered reopen data by 'Issue key' and 'Assignee' and count
# the number of reopen events for each ticket.
reopens_per_ticket = filtered_reopens.groupby(['Issue key', 'Assignee']).size().reset_index(name='Reopens Count')

# Save the per-ticket result to a new CSV file
output_filename_ticket = f'reopens_per_ticket_{REPORT_MONTH}.csv'
reopens_per_ticket.to_csv(output_filename_ticket, index=False)
print(f"Per-ticket reopen stats for '{REPORT_MONTH}' have been saved to '{output_filename_ticket}'")
