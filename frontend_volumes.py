#!/usr/bin/env python3
"""
Fetch top 10 frontend daily volumes from Dune and send a beautiful report to Slack.
Shows client name, volumes with actual dates, and percent change.
"""

import os
import requests
from datetime import datetime, timezone, timedelta
from dune_client.client import DuneClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DUNE_API_KEY = os.getenv("DUNE_API_KEY_FRONTEND")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_FRONTEND")
QUERY_ID = 6928394


def fetch_dune_data():
    """Fetch latest results from Dune query."""
    print(f"Fetching data from Dune query {QUERY_ID}...")
    dune = DuneClient(DUNE_API_KEY)

    try:
        query_result = dune.get_latest_result(QUERY_ID)
    except Exception as e:
        print(f"No cached results found, executing query... ({e})")
        from dune_client.query import QueryBase
        query = QueryBase(query_id=QUERY_ID)
        query_result = dune.run_query(query)

    rows = query_result.result.rows
    print(f"Fetched {len(rows)} rows")
    return rows


def format_volume(value):
    """Format volume as $X.XXm or $X.XXk."""
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}m"
    elif value >= 1_000:
        return f"${value / 1_000:.2f}k"
    else:
        return f"${value:.2f}"


def get_change_indicator(pct_change):
    """Get simple arrow indicator based on percent change."""
    if pct_change >= 0:
        return "▲"
    else:
        return "▼"


def build_slack_message(data):
    """Build a clean, simple Slack Block Kit message."""

    # Calculate dates
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    day_before = today - timedelta(days=2)

    yesterday_str = yesterday.strftime("%b %d")  # e.g., "Mar 30"
    day_before_str = day_before.strftime("%b %d")  # e.g., "Mar 29"

    # Build the table rows
    table_lines = []
    table_lines.append(f"`{'#':>2}  {'Frontend':<14} {yesterday_str:>10} {day_before_str:>10}   Change`")
    table_lines.append("`" + "─" * 52 + "`")

    for idx, row in enumerate(data[:10], 1):
        client_name = row.get('client_name', 'Unknown')
        yesterday_vol = row.get('yesterday_volume', 0)
        day_before_vol = row.get('day_before_volume', 0)
        pct_change = row.get('pct_change', 0)

        # Format values with fixed widths
        name_display = client_name[:14].ljust(14)
        vol1 = format_volume(yesterday_vol).rjust(10)
        vol2 = format_volume(day_before_vol).rjust(10)

        arrow = get_change_indicator(pct_change)
        change_val = f"{arrow} {abs(pct_change):.1f}%"

        # Use code block for alignment, then colored change outside
        row_base = f"`{idx:>2}  {name_display} {vol1} {vol2}`"

        # Add colored change indicator
        if pct_change >= 0:
            change_str = f"  :small_green_dot: {change_val}"
        else:
            change_str = f"  :small_red_triangle_down: {change_val}"

        table_lines.append(row_base + change_str)

    table_text = "\n".join(table_lines)

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Top 10 Frontend Volumes*\n_{yesterday.strftime('%A, %B %d, %Y')}_"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": table_text
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "via Dune Analytics"
                }
            ]
        }
    ]

    return blocks


def send_to_slack(data):
    """Send the formatted report to Slack."""
    blocks = build_slack_message(data)

    payload = {
        "text": "Top 10 Frontend Daily Volumes Report",
        "blocks": blocks
    }

    response = requests.post(SLACK_WEBHOOK_URL, json=payload)

    if response.status_code == 200:
        print("Report sent to Slack successfully!")
        return True
    else:
        print(f"Failed to send to Slack: {response.status_code} - {response.text}")
        return False


def main():
    """Main function to fetch data and send report."""
    print(f"Starting frontend volumes report at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # Fetch data from Dune
    data = fetch_dune_data()

    if not data:
        print("No data fetched from Dune. Exiting.")
        return

    # Print data preview
    print(f"\nTop 10 Frontends by Volume:")
    for idx, row in enumerate(data[:10], 1):
        print(f"  {idx}. {row.get('client_name', 'Unknown')}: {format_volume(row.get('yesterday_volume', 0))}")

    # Send to Slack
    send_to_slack(data)

    print("\nJob completed!")


if __name__ == "__main__":
    main()
