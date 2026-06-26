#!/usr/bin/env python3
"""
Fetch top 10 frontend daily volumes & transactions from Dune, 
fetch Quotes from Grafana/Loki, and send a beautiful report to Slack.
Shows client name, volumes, transactions, trades per quote, and percent change.
"""

import os
import requests
from datetime import datetime, timezone, timedelta
from dune_client.client import DuneClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Config: Dune & Slack ---
DUNE_API_KEY = os.getenv("DUNE_API_KEY_FRONTEND")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_FRONTEND")
QUERY_ID = 6928394
SHARE_QUERY_ID = 7377109
FEE_QUERY_ID = 7426532

# --- Config: Grafana ---
GRAFANA_URL = os.getenv("GRAFANA_URL", "https://dflow.grafana.net")
GRAFANA_TOKEN = os.getenv("GRAFANA_TOKEN")
GRAFANA_DATASOURCE_UID = os.getenv("GRAFANA_DATASOURCE_UID", "grafanacloud-logs")


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
            'titan_volume_pct': row.get('titan_volume_pct', 0),
            'dflow_volume_share_change': row.get('dflow_volume_share_change', 0),
            'dflow_txn_pct': row.get('dflow_txn_pct', 0),
            'okx_txn_pct': row.get('okx_txn_pct', 0),
            'jupiter_txn_pct': row.get('jupiter_txn_pct', 0),
            'titan_txn_pct': row.get('titan_txn_pct', 0),
            'dflow_txn_share_change': row.get('dflow_txn_share_change', 0),
        }
    return share_lookup


def fetch_fee_data():
    """Fetch latest results from Dune fee payers query."""
    print(f"Fetching fee data from Dune query {FEE_QUERY_ID}...")
    dune = DuneClient(DUNE_API_KEY)

    try:
        query_result = dune.get_latest_result(FEE_QUERY_ID)
    except Exception as e:
        print(f"No cached results found for fee query, executing... ({e})")
        from dune_client.query import QueryBase
        query = QueryBase(query_id=FEE_QUERY_ID)
        query_result = dune.run_query(query)

    rows = query_result.result.rows
    print(f"Fetched {len(rows)} fee rows")
    return rows


def fetch_grafana_quotes():
    """Fetch Quotes count from Grafana/Loki for yesterday."""
    print("Fetching Quotes data from Grafana/Loki...")
    if not GRAFANA_TOKEN:
        print("Warning: GRAFANA_TOKEN missing from environment. Skipping Quotes.")
        return {}

    app_mapping = {
        "227": "Kamino",
        "154": "Fomo",
        "350": "Coinbase",
        "104": "Solflare",
        "120": "Phantom",
        "798": "Tessera"
    }

    query_regional = r"""
    sum by (app_id) (
      count_over_time(
        {ecs_cluster="regional-prod"}
        |~ "(?i)\"app_id\":\"(227|154|350|104|798)\""
        |~ "(?i)finished processing request"
        | regexp `(?i)"app_id":"(?P<app_id>227|154|350|104|798)"`
        [24h]
      )
    )
    """

    query_phantom = r"""
    sum by (app_id) (
      count_over_time(
        {ecs_task_arn=~".*us-east-1.*", container_name=~"(haze-aggregator-api|haze-aggregator-api-b)"}
        |~ "(?i)\"app_id\":\"120\""
        != "/health-check"
        | regexp `(?i)"app_id":"(?P<app_id>120)"`
        [24h]
      )
    )
    """

    # Evaluate at today's exact midnight UTC (to capture full 24h of yesterday)
    now_utc = datetime.now(timezone.utc)
    today_midnight_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    eval_time = today_midnight_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    def run_loki_query(query_string):
        url = f"{GRAFANA_URL}/api/datasources/proxy/uid/{GRAFANA_DATASOURCE_UID}/loki/api/v1/query"
        try:
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {GRAFANA_TOKEN}"},
                params={"query": query_string, "time": eval_time},
            )
            if response.status_code == 200:
                return response.json().get("data", {}).get("result", [])
            else:
                print(f"Grafana error {response.status_code}: {response.text}")
        except Exception as e:
            print(f"Failed to query Grafana: {e}")
        return []

    all_results = []
    all_results.extend(run_loki_query(query_regional))
    all_results.extend(run_loki_query(query_phantom))

    quotes_data = {}
    for series in all_results:
        metric = series.get("metric", {})
        app_id = metric.get("app_id", "unknown")
        app_name = app_mapping.get(app_id, f"App {app_id}")
        total_quotes = float(series["value"][1])
        quotes_data[app_name] = total_quotes

    return quotes_data


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
    """Format transaction count as X.XXm, X.XXk, or raw number."""
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


def build_slack_message(data, share_lookup, fee_data, quotes_data):
    """Build a clean, simple Slack Block Kit message with volume, txn, share, fee, and quote sections."""

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

    # ── Trades per Quote table (Inner Join & 6 Decimal Ratio) ──
    quotes_lookup = {k.lower(): v for k, v in quotes_data.items()}
    
    tq_lines = []
    tq_lines.append(f"`{'#':>2}  {'Frontend':<14} {'Quotes':>9} {'Trades':>9} {'Ratio':>10}`")
    tq_lines.append("`" + "─" * 49 + "`")

    idx = 1
    for row in data:
        client_name = row.get('client_name', 'Unknown')
        
        # INNER JOIN: Skip if the app name is not present in Grafana Quotes output
        if client_name.lower() not in quotes_lookup:
            continue
            
        txns = float(row.get('yesterday_txns', 0) or 0)
        quotes = quotes_lookup[client_name.lower()]

        name_display = client_name[:14].ljust(14)
        q_fmt = format_txns(quotes).rjust(9) if quotes > 0 else "N/A".rjust(9)
        t_fmt = format_txns(txns).rjust(9)

        if quotes > 0:
            ratio = txns / quotes
            ratio_fmt = f"{ratio:.6f}".rjust(10)
        else:
            ratio_fmt = "-".rjust(10)

        tq_lines.append(f"`{idx:>2}  {name_display} {q_fmt} {t_fmt} {ratio_fmt}`")
        idx += 1
        
    tq_text = "\n".join(tq_lines)

    # ── Volume Share table ──
    share_lines = []
    share_lines.append(f"`{'#':>2}  {'Frontend':<14} {'DFlow':>7} {'OKX':>7} {'Jupiter':>7} {'Titan':>7}   DFlow Δ`")
    share_lines.append("`" + "─" * 56 + "`")

    for idx, (client_name, share_info) in enumerate(share_lookup.items(), 1):
        dflow_pct = share_info.get('dflow_volume_pct', 0)
        okx_pct = share_info.get('okx_volume_pct', 0)
        jupiter_pct = share_info.get('jupiter_volume_pct', 0)
        titan_pct = share_info.get('titan_volume_pct', 0)
        dflow_change = share_info.get('dflow_volume_share_change', 0)

        name_display = client_name[:14].ljust(14)
        d = format_share(dflow_pct).rjust(7)
        o = format_share(okx_pct).rjust(7)
        j = format_share(jupiter_pct).rjust(7)
        t = format_share(titan_pct).rjust(7)
        row_base = f"`{idx:>2}  {name_display} {d} {o} {j} {t}`"

        share_lines.append(row_base + format_change(dflow_change))

    share_text = "\n".join(share_lines)

    # ── Txn Share table ──
    txn_share_lines = []
    txn_share_lines.append(f"`{'#':>2}  {'Frontend':<14} {'DFlow':>7} {'OKX':>7} {'Jupiter':>7} {'Titan':>7}   DFlow Δ`")
    txn_share_lines.append("`" + "─" * 56 + "`")

    for idx, (client_name, share_info) in enumerate(share_lookup.items(), 1):
        dflow_pct = share_info.get('dflow_txn_pct', 0)
        okx_pct = share_info.get('okx_txn_pct', 0)
        jupiter_pct = share_info.get('jupiter_txn_pct', 0)
        titan_pct = share_info.get('titan_txn_pct', 0)
        dflow_change = share_info.get('dflow_txn_share_change', 0)

        name_display = client_name[:14].ljust(14)
        d = format_share(dflow_pct).rjust(7)
        o = format_share(okx_pct).rjust(7)
        j = format_share(jupiter_pct).rjust(7)
        t = format_share(titan_pct).rjust(7)
        row_base = f"`{idx:>2}  {name_display} {d} {o} {j} {t}`"

        txn_share_lines.append(row_base + format_change(dflow_change))

    txn_share_text = "\n".join(txn_share_lines)

    # ── Fee Payers table ──
    fee_lines = []
    fee_lines.append(f"`{'#':>2}  {'Frontend':<14} {yesterday_str:>10} {day_before_str:>10}   Change`")
    fee_lines.append("`" + "─" * 52 + "`")

    for idx, row in enumerate(fee_data[:10], 1):
        client_name = row.get('client_name', 'Unknown')
        yesterday_signers = row.get('yesterday_signers', 0) or 0
        day_before_signers = row.get('day_before_signers', 0) or 0
        signers_pct = row.get('signers_pct_change') or 0

        name_display = client_name[:14].ljust(14)
        s1 = format_txns(yesterday_signers).rjust(10)
        s2 = format_txns(day_before_signers).rjust(10)
        row_base = f"`{idx:>2}  {name_display} {s1} {s2}`"

        fee_lines.append(row_base + format_change(signers_pct))

    fee_text = "\n".join(fee_lines)

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
                "text": "📊 *Trades per Quote*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": tq_text
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
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "🔢 *Transaction Market Share*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": txn_share_text
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "👥 *Fee Payers*"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": fee_text
            }
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "via Dune Analytics & Grafana Loki"
                }
            ]
        }
    ]

    return blocks


def send_to_slack(data, share_lookup, fee_data, quotes_data):
    """Send the formatted report to Slack."""
    blocks = build_slack_message(data, share_lookup, fee_data, quotes_data)

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

    # Fetch fee data from Dune
    fee_data = fetch_fee_data()

    # Fetch Quotes from Grafana
    quotes_data = fetch_grafana_quotes()

    # Print data preview
    print(f"\nTop 10 Frontends by Volume:")
    for idx, row in enumerate(data[:10], 1):
        vol = format_volume(row.get('yesterday_volume', 0))
        txns = format_txns(row.get('yesterday_txns', 0))
        client_name = row.get('client_name', 'Unknown')
        print(f"  {idx}. {client_name}: {vol} | {txns} txns")

    print(f"\nTrades per Quote:")
    quotes_lookup = {k.lower(): v for k, v in quotes_data.items()}
    
    idx = 1
    for row in data:
        client_name = row.get('client_name', 'Unknown')
        
        # INNER JOIN for console output as well
        if client_name.lower() not in quotes_lookup:
            continue
            
        txns = float(row.get('yesterday_txns', 0) or 0)
        quotes = quotes_lookup[client_name.lower()]
        
        q_str = format_txns(quotes) if quotes > 0 else "N/A"
        ratio = f"{(txns/quotes):.6f}" if quotes > 0 else "N/A"
        print(f"  {idx}. {client_name}: {q_str} quotes | {format_txns(txns)} trades | Yield: {ratio}")
        idx += 1

    print(f"\nVolume Share by Client:")
    for idx, (client_name, share_info) in enumerate(share_lookup.items(), 1):
        dflow = format_share(share_info.get('dflow_volume_pct', 0))
        okx = format_share(share_info.get('okx_volume_pct', 0))
        jup = format_share(share_info.get('jupiter_volume_pct', 0))
        print(f"  {idx}. {client_name}: DFlow {dflow} | OKX {okx} | Jupiter {jup}")

    print(f"\nTxn Share by Client:")
    for idx, (client_name, share_info) in enumerate(share_lookup.items(), 1):
        dflow = format_share(share_info.get('dflow_txn_pct', 0))
        okx = format_share(share_info.get('okx_txn_pct', 0))
        jup = format_share(share_info.get('jupiter_txn_pct', 0))
        print(f"  {idx}. {client_name}: DFlow {dflow} | OKX {okx} | Jupiter {jup}")

    print(f"\nFee Payers by Client:")
    for idx, row in enumerate(fee_data[:10], 1):
        client_name = row.get('client_name', 'Unknown')
        ys = format_txns(row.get('yesterday_signers', 0) or 0)
        db = format_txns(row.get('day_before_signers', 0) or 0)
        print(f"  {idx}. {client_name}: {ys} (yesterday) | {db} (day before)")

    # Send to Slack
    send_to_slack(data, share_lookup, fee_data, quotes_data)

    print("\nJob completed!")


if __name__ == "__main__":
    main()
