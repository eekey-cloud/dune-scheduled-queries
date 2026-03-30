#!/usr/bin/env python3
"""
Fetch top 10 frontend daily volumes from Dune and send a beautiful report to Slack.
Shows client name, yesterday's volume, day before volume, and percent change.
"""

import os
import requests
from datetime import datetime, timezone
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


def get_change_emoji(pct_change):
    """Get emoji based on percent change."""
    if pct_change >= 20:
        return "🚀"
    elif pct_change >= 10:
        return "📈"
    elif pct_change >= 0:
        return "✅"
    elif pct_change >= -10:
        return "📉"
    elif pct_change >= -20:
        return "⚠️"
    else:
        return "🔻"


def get_rank_emoji(rank):
    """Get medal emoji for top 3."""
    if rank == 1:
        return "🥇"
    elif rank == 2:
        return "🥈"
    elif rank == 3:
        return "🥉"
    else:
        return f"{rank}."


def build_slack_message(data):
    """Build a beautiful Slack Block Kit message."""

    # Header
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📊 Top 10 Frontend Daily Volumes",
                "emoji": True
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"🕐 Report generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                }
            ]
        },
        {
            "type": "divider"
        }
    ]

    # Calculate totals for summary
    total_yesterday = sum(row.get('yesterday_volume', 0) for row in data)
    total_day_before = sum(row.get('day_before_volume', 0) for row in data)
    total_pct_change = ((total_yesterday - total_day_before) / total_day_before * 100) if total_day_before > 0 else 0

    # Summary section
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*📈 Market Summary*\n"
                f"Total Volume (Top 10): *{format_volume(total_yesterday)}*\n"
                f"vs Previous Day: *{format_volume(total_day_before)}* ({total_pct_change:+.2f}%)"
            )
        }
    })

    blocks.append({"type": "divider"})

    # Column headers
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*Rank  │  Frontend  │  Yesterday  │  Day Before  │  Change*"
        }
    })

    # Build rows for each client
    for idx, row in enumerate(data[:10], 1):
        client_name = row.get('client_name', 'Unknown')
        yesterday_vol = row.get('yesterday_volume', 0)
        day_before_vol = row.get('day_before_volume', 0)
        pct_change = row.get('pct_change', 0)

        rank_emoji = get_rank_emoji(idx)
        change_emoji = get_change_emoji(pct_change)

        # Format the change with color indicator
        if pct_change >= 0:
            change_str = f"+{pct_change:.2f}%"
        else:
            change_str = f"{pct_change:.2f}%"

        row_text = (
            f"{rank_emoji}  *{client_name}*\n"
            f"      └─ {format_volume(yesterday_vol)}  ←  {format_volume(day_before_vol)}  │  {change_emoji} {change_str}"
        )

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": row_text
            }
        })

    # Footer
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "📡 Data source: Dune Analytics  •  🔄 Updates daily at 4:00 AM UTC"
            }
        ]
    })

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
