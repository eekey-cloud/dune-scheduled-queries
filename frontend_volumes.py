#!/usr/bin/env python3
"""
Fetch top 10 frontend daily volumes & transactions from Dune and send a beautiful report to Slack.
Shows client name, volumes, transactions with actual dates, and percent change.
Also shows app share in volume from a separate Dune query.
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
SHARE_QUERY_ID = 7377109


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


def fetch_share_data():
    """Fetch latest results from Dune share query."""
    print(f"Fetching share data from Dune query {SHARE_QUERY_ID}...")
    dune = DuneClient(DUNE_API_KEY)

    try:
        query_result = dune.get_latest_result(SHARE_QUERY_ID)
    except Exception as e:
        print(f"No cached results found for share query, executing... ({e})")
        from dune_client.query import QueryBase
        query = QueryBase(query_id=SHARE_QUERY_ID)
        query_result = dune.run_query(query)

    rows = query_result.result.rows
    print(f"Fetched {len(rows)} share rows")

    # Build a lookup dict keyed by client_name
    share_lookup = {}
    for row in rows:
        name = row.get('client_name', '')
        share_lookup[name] = {
            'dflow_volume_pct': row.get('dflow_volume_pct', 0),
            'okx_volume_pct': row.get('okx_volume_pct', 0),
            'jupiter_volume_pct': row.get('jupiter_volume_pct', 0),
            'dflow_share_pct_change': row.get('dflow_share_pct_change', 0),
        }
    return share_lookup


def format_volume(value):
    """Format volume as $X.XXm or $X.XXk."""
    value = float(value)
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}m"
    elif value >= 1_000:
        return f"${value / 1_000:.2f}k"
    else:
        return f"${value:.2f}"


def format_txns(value):
    """Format transaction count as X.XXk or raw number."""
    value = float(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}m"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}k"
    else:
        return f"{int(value)}"


def format_share(value):
    """Format share percentage as X.XX%."""
    value = float(value)
    return f"{value:.2f}%"


def format_change(pct_change):
    """Return emoji + formatted percent change string."""
    pct_change = float(pct_change)
    if pct_change >= 0:
        return f"  📈 `+{abs(pct_change):.1f}%`"
    else:
        return f"  📉 `-{abs(pct_change):.1f}%`"


def build_slack_message(data, share_lookup):
    """Build a clean, simple Slack Block Kit message with volume, txn, and share sections."""

    # Calculate dates
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    day_before = today - timedelta(days=2)

    yesterday_str = yesterday.strftime("%b %d")
    day_before_str = day_before.strftime("%b %d")

    # ── Volume table ──
    vol_lines = []
    vol_lines.append(f"`{'#':>2}  {'Frontend':<14} {yesterday_str:>10} {day_before_str:>10}   Change`")
    vol_lines.append("`" + "─" * 52 + "`")

    for idx, row in enumerate(data[:10], 1):
        client_name = row.get('client_name', 'Unknown')
        yesterday_vol = row.get('yesterday_volume', 0)
        day_before_vol = row.get('day_before_volume', 0)
        vol_pct = row.get('volume_pct_change', row.get('pct_change', 0))

        name_display = client_name[:14].ljust(14)
        vol1 = format_volume(yesterday_vol).rjust(10)
        vol2 = format_volume(day_before_vol).rjust(10)
        row_base = f"`{idx:>2}  {name_display} {vol1} {vol2}`"

        vol_lines.append(row_base + format_change(vol_pct))

    vol_text = "\n".join(vol_lines)

    # ── Transactions table ──
    txn_lines = []
    txn_lines.append(f"`{'#':>2}  {'Frontend':<14} {yesterday_str:>10} {day_before_str:>10}   Change`")
    txn_lines.append("`" + "─" * 52 + "`")

    for idx, row in enumerate(data[:10], 1):
        client_name = row.get('client_name', 'Unknown')
        yesterday_txns = row.get('yesterday_txns', 0)
        day_before_txns = row.get('day_before_txns', 0)
        txn_pct = row.get('txns_pct_change', 0)

        name_display = client_name[:14].ljust(14)
        txn1 = format_txns(yesterday_txns).rjust(10)
        txn2 = format_txns(day_before_txns).rjust(10)
        row_base = f"`{idx:>2}  {name_display} {txn1} {txn2}`"

        txn_lines.append(row_base + format_change(txn_pct))

    txn_text = "\n".join(txn_lines)

    # ── Volume Share table ──
    share_lines = []
    share_lines.append(f"`{'#':>2}  {'Frontend':<14} {'DFlow':>8} {'OKX':>8} {'Jupiter':>8}   DFlow Δ`")
    share_lines.append("`" + "─" * 56 + "`")

    for idx, row in enumerate(data[:10], 1):
        client_name = row.get('client_name', 'Unknown')
        share_info = share_lookup.get(client_name, {})

        dflow_pct = share_info.get('dflow_volume_pct', 0)
        okx_pct = share_info.get('okx_volume_pct', 0)
        jupiter_pct = share_info.get('jupiter_volume_pct', 0)
        dflow_change = share_info.get('dflow_share_pct_change', 0)

        name_display = client_name[:14].ljust(14)
        d = format_share(dflow_pct).rjust(8)
        o = format_share(okx_pct).rjust(8)
        j = format_share(jupiter_pct).rjust(8)
        row_base = f"`{idx:>2}  {name_display} {d} {o} {j}`"

        share_lines.append(row_base + format_change(dflow_change))

    share_text = "\n".join(share_lines)

    # ── Assemble blocks ──
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Top 10 Frontend Volumes, Transactions & Market Share*\n_{yesterday.strftime('%A, %B %d, %Y')}_"
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "💰 *Volume*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": vol_text
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "🔁 *Transactions*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": txn_text
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "🥧 *Volume Market Share*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": share_text
            }
        },
        {"type": "divider"},
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


def send_to_slack(data, share_lookup):
    """Send the formatted report to Slack."""
    blocks = build_slack_message(data, share_lookup)

    payload = {
        "text": "Top 10 Frontend Daily Volumes, Transactions & Market Share Report",
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

    # Fetch share data from Dune
    share_lookup = fetch_share_data()

    # Print data preview
    print(f"\nTop 10 Frontends by Volume:")
    for idx, row in enumerate(data[:10], 1):
        vol = format_volume(row.get('yesterday_volume', 0))
        txns = format_txns(row.get('yesterday_txns', 0))
        client_name = row.get('client_name', 'Unknown')
        share_info = share_lookup.get(client_name, {})
        share = format_share(share_info.get('dflow_volume_pct', 0))
        print(f"  {idx}. {client_name}: {vol} | {txns} txns | {share} DFlow share")

    # Send to Slack
    send_to_slack(data, share_lookup)

    print("\nJob completed!")


if __name__ == "__main__":
    main()
