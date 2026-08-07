#!/usr/bin/env python3
"""
Fetch DFlow Swap Orchestrator on-chain failure breakdown (Dune query 8251243)
and post a monospace table to Slack.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dotenv import load_dotenv

load_dotenv()

DUNE_API_KEY = os.getenv("DUNE_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_FAILURES")
QUERY_ID = 8251243

SOLSCAN = "https://solscan.io/tx/"


def fetch_dune_data(refresh=True):
    """Fetch results from Dune. refresh=True re-executes so the 24h window is live."""
    dune = DuneClient(DUNE_API_KEY)
    if refresh:
        print(f"Executing Dune query {QUERY_ID}...")
        result = dune.run_query(QueryBase(query_id=QUERY_ID))
    else:
        print(f"Fetching cached results for {QUERY_ID}...")
        result = dune.get_latest_result(QUERY_ID)

    df = pd.DataFrame(result.result.rows)
    print(f"Fetched {len(df)} rows")
    return df


def truncate(s, n):
    s = str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


def build_table(df):
    """Fixed-width table that survives Slack's monospace block."""
    W = {"scope": 16, "owner": 18, "error": 42, "n": 7, "pct": 6}

    header = (
        f"{'SCOPE':<{W['scope']}}"
        f"{'OWNER':<{W['owner']}}"
        f"{'ERROR TYPE':<{W['error']}}"
        f"{'FAILS':>{W['n']}}"
        f"{'PCT':>{W['pct']}}"
    )
    lines = [header, "-" * len(header)]

    for _, r in df.iterrows():
        lines.append(
            f"{truncate(r['scope'], W['scope'] - 1):<{W['scope']}}"
            f"{truncate(r['owner'], W['owner'] - 1):<{W['owner']}}"
            f"{truncate(r['error_type'], W['error'] - 1):<{W['error']}}"
            f"{int(r['failures']):>{W['n']},}"
            f"{r['pct']:>{W['pct']}.1f}"
        )

    total = int(df["failures"].sum())
    lines.append("-" * len(header))
    lines.append(f"{'TOTAL (top 10)':<{W['scope'] + W['owner'] + W['error']}}{total:>{W['n']},}")

    return "\n".join(lines)


def build_samples(df, k=3):
    """Link a few sample txs for the biggest buckets."""
    out = []
    for _, r in df.head(k).iterrows():
        label = truncate(f"{r['owner']} / {r['error_type']}", 60)
        out.append(f"• <{SOLSCAN}{r['sample_tx']}|{r['sample_tx'][:8]}…> — {label}")
    return "\n".join(out)


def send_to_slack(df):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = int(df["failures"].sum())

    venue = df[df["scope"].str.startswith("Venue")]["failures"].sum()
    nonvenue = df[df["scope"] == "Non-venue"]["failures"].sum()

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "DFlow Swap Failures — last 24h"},
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_{ts}_ · query `{QUERY_ID}`"}],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Total (top 10)*\n{total:,}"},
                {"type": "mrkdwn", "text": f"*Venue / Non-venue*\n{venue:,} / {nonvenue:,}"},
            ],
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": f"```\n{build_table(df)}\n```"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*Samples*\n{build_samples(df)}"}},
    ]

    payload = {"text": f"DFlow swap failures 24h — {total:,} in top 10 buckets", "blocks": blocks}
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)

    if resp.status_code == 200:
        print("Posted to Slack.")
    else:
        print(f"Slack failed: {resp.status_code} - {resp.text}")
    return resp.status_code == 200


def main():
    for name, val in (("DUNE_API_KEY", DUNE_API_KEY), ("SLACK_WEBHOOK_URL", SLACK_WEBHOOK_URL)):
        if not val:
            raise SystemExit(f"{name} is not set")

    df = fetch_dune_data()
    if df.empty:
        print("No rows — nothing to post.")
        return

    required = {"scope", "owner", "error_type", "failures", "pct", "sample_tx"}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing columns from Dune result: {missing}")

    df = df.sort_values("failures", ascending=False)
    print(df[["scope", "owner", "error_type", "failures"]].to_string(index=False))
    send_to_slack(df)


if __name__ == "__main__":
    main()
