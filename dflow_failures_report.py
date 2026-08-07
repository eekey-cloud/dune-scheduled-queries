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

# --- Config: Grafana ---
# The token may live under any of these names depending on which repo secret
# was created first; take whichever is actually populated.
_GRAFANA_TOKEN_KEYS = (
    "GRAFANA_TOKEN",
    "GRAFANA_API_TOKEN",
    "GRAFANA_API_KEY",
    "GRAFANA_CLOUD_TOKEN",
    "GRAFANA_SERVICE_ACCOUNT_TOKEN",
)


def _first_env(keys):
    """Return (value, name_it_came_from) for the first non-empty key."""
    for k in keys:
        v = (os.getenv(k) or "").strip()
        if v:
            return v, k
    return None, None


GRAFANA_TOKEN, GRAFANA_TOKEN_SRC = _first_env(_GRAFANA_TOKEN_KEYS)
# .strip() + `or` guards against an unset GH secret resolving to "" and
# clobbering the default.
GRAFANA_URL = (os.getenv("GRAFANA_URL") or "").strip() or "https://dflow.grafana.net"
GRAFANA_DATASOURCE_UID = (
    (os.getenv("GRAFANA_DATASOURCE_UID") or "").strip() or "grafanacloud-logs"
)

DFLOW_PROGRAM = "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH"

# program_id -> venue_name. Mirrors program_map in Dune query 8251243.
# Order matters only in that later entries win; there are no duplicate ids.
PROGRAM_MAP = [
    (DFLOW_PROGRAM, "DFlow Swap Orchestrator"),
    ("9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp", "HumidiFi"),
    ("BiSoNHVpsVZW2F7rx2eQ59yQwKxzU5NvBcmKshCSUypi", "BisonFi"),
    ("2DNbzPochEcyCcWMbL4d9S3u9QqQEj5bbe6cSZFvKsbh", "BisonFi Predictions"),
    ("JanusXpm3gsW3c9ErNoUgHppL8dGLvZKB7uekkJEYFP", "JanusFi"),
    ("orafZ4BdfzikRRg498P23vG3EdEyMR7bYoYcD2zcwiD", "JanusFi"),
    ("AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45", "Nexus"),
    ("fastC7gqs2WUXgcyNna2BZAe9mte4zcTGprv3mv18N3", "Nexus"),
    ("Tri3NG4HkZ6DddYPKoX2ehgkqFtDuej9Aspw5BmvSo4", "Triangle"),
    ("TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH", "Tessera-V"),
    ("kdexv89r17wFQN1MY3auCX7QgWFyshWAji2LsLRVUQU", "KDEX"),
    ("HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ", "KDEX"),
    ("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "Raydium AMM"),
    ("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "Raydium CLMM"),
    ("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", "Raydium CPMM"),
    ("LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj", "Raydium LaunchLab"),
    ("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "Meteora DLMM"),
    ("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB", "Meteora DAMM v1"),
    ("24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi", "Meteora DAMM v1"),
    ("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG", "Meteora DAMM v2"),
    ("dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN", "Meteora DBC"),
    ("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "Pump.fun"),
    ("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA", "Pump.fun AMM"),
    ("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ", "Pump.fun"),
    ("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "Whirlpools"),
    ("PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY", "Phoenix"),
    ("MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms", "Manifest"),
    ("stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq", "Sanctum StakeDex"),
    ("5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx", "Sanctum Infinity"),
    ("SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe", "SolFi"),
    ("SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF", "SolFi v2"),
    ("E2uCGJ4TtYyKPGaK57UMfbs9sgaumwDEZF1aAY6fF3mS", "SolFi v2"),
    ("gatorLx9aC1e5ZWAXscv5QRKiLXnLPLXjftVc81h1Hr", "GatorSwap"),
    ("GAMMA7meSFWaBXF25oSUgmGRwaW6sCMFLmBNiMSdbHVT", "Gamma"),
    ("ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA", "AlphaQ"),
    ("obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y", "Obric v2"),
    ("Minimox7jqQmMpF6Z34DTNwE9iJyNkruzvvYQRaHpAP", "Obric v2"),
    ("REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2", "Byreal CLMM"),
    ("REALKVQcx2GMAyY46MUN3aTXG9xy1pXKRwPEC2swRpd", "Byreal Prop AMM"),
    ("3ZfZY9sRNNaFUY4w6a12DU8KArgWjHgi6KTz83Y4RN7o", "Byreal Prop AMM"),
    ("5pXzd9UiWrVxATCYWmgo5EbfxzXqHYhfSKGdCPXPz7vK", "Doppler CPMM"),
    ("4carc9eePfE7jKUXdCAYMhcPf4awEFpZPrz1sTykdss1", "Doppler Launch"),
    ("4pU2NUiPd3WFCw8vTbvyF3RSARhjMqoUejWi7eMJWp3U", "Doppler"),
    ("GGMAUGEEPuUxXz7uMVmcZxgtifhVusZUjB42VBRRSg5T", "Doppler"),
    ("4cPvEYosU3g7h4kA95XKbDhLxeJ8QQUf5rdx3K257ws2", "Doppler"),
    ("BeyqffXEVgLpM3fQ1zjk8YnZzQN9sMVrCKtNKwSxNATr", "Doppler"),
    ("9b1NvVimFW4aTh3fPFXXWG3dnYnBqiL4915G947N4Yhd", "Doppler"),
    ("SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr", "Saros AMM"),
    ("1qbkdrr3z4ryLA7pZykqxvxWPoeifcVKo6ZG9CfkvVE", "Saros DLMM"),
    ("swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ", "Stabble Stable Swap"),
    ("swapFpHZwjELNnjvThjajtiVmkz3yPQEHjLtka2fwHW", "Stabble Weighted Swap"),
    ("vrTGoBuy5rYSxAfV3jaRJWHH6nN9WK4NRExGxsk1bCJ", "Vertigo"),
    ("FLUXwEJnjZrURq3ZLBAQFzvR76bpcXscZkiZkiFHNhhR", "FluxSwap DAMM v2"),
    ("FLiNTXPwppyoJabCoxc2uiiRygAHpmMXajiDXo2Ub1z", "Superis"),
    ("ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY", "ZeroFi"),
    ("HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o", "Heaven"),
    ("SCoRcH8c2dpjvcJD6FiPbCSQyQgu3PcUAWj2Xxx3mqn", "Scorch"),
    ("ojh19ojaKduoJZuaJADhcVGp4xt1TcdAvZmpVsCorch", "Scorch"),
    ("FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq", "MetaDAO"),
    ("HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq", "PancakeSwap"),
    ("DRVSpZ2YUYYKgZP8XtLhAGtT1zYSCKzeHfb4DgRnrgqD", "Deriverse"),
    ("ghosty4ZU1Qk1HN7Ymz4pZ15QfspzJZgSYFkdKN6ZLK", "Ghost"),
]


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


# ---------------------------------------------------------------- Grafana --

# Number of buckets pulled for each simulation section. Both queries and all
# table/section labels derive from this, so they can't drift apart.
SIM_TOPK = 10

# Reduced-input retries are a normal part of the quote path, not a failure
# worth reporting. Regex form (not a plain != ) so it also matches
# `"is_reduced_input": true` if the serializer ever emits a space.
REDUCED_INPUT_FILTER = '!~ `"is_reduced_input":\\s*true`'


def build_sim_by_log_query(topk=SIM_TOPK):
    """Query 1: simulation failures grouped by the raw program log line."""
    return f"""sort_desc(topk({topk}, sum by (program_log) (
  count_over_time(
    {{container_name="haze-aggregator-api"}} |= "Failed to simulate transaction"
    {REDUCED_INPUT_FILTER}
    | regexp `.*Program log: (?P<program_log>[^\\"]*)`
    | program_log != ""
    [24h])
)))"""


def build_sim_by_venue_query(topk=SIM_TOPK):
    """Query 2: simulation failures by (venue_name, err_code).

    Built as a nested label_replace chain rather than pasted literal, so the
    venue map stays in one place (PROGRAM_MAP) shared with the Dune query.
    Innermost wrapper is the identity fallback — any unmapped program keeps its
    address as venue_name — and each specific mapping wraps outward, so a
    matching program id overwrites the fallback.
    """
    inner = (
        '  count_over_time(\n'
        '    {container_name="haze-aggregator-api"} |= "Failed to simulate transaction"\n'
        f'    {REDUCED_INPUT_FILTER}\n'
        '    | regexp `Program (?P<failing_program>[1-9A-HJ-NP-Za-km-z]{32,44}) failed:`\n'
        '    | regexp `err: InstructionError\\(\\d+, Custom\\((?P<err_code>\\d+)\\)\\)`\n'
        '    | failing_program != ""\n'
        f'    | failing_program != "{DFLOW_PROGRAM}"\n'
        '    [24h])'
    )

    tails = ['  "venue_name", "$1", "failing_program", "(.+)")']
    for program_id, venue_name in PROGRAM_MAP:
        tails.append(f'  "venue_name", "{venue_name}", "failing_program", "{program_id}")')

    head = "label_replace(\n" * len(tails)
    body = inner + ",\n" + ",\n".join(tails)

    return (
        f"sort_desc(topk({topk}, sum by (venue_name, err_code) (\n"
        f"{head}{body}\n)))"
    )


def run_loki_query(query_string, eval_time, label=""):
    """Instant query through Grafana's datasource proxy. Window comes from [24h]."""
    url = (
        f"{GRAFANA_URL}/api/datasources/proxy/uid/{GRAFANA_DATASOURCE_UID}"
        "/loki/api/v1/query"
    )
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {GRAFANA_TOKEN}"},
            params={"query": query_string, "time": eval_time},
            timeout=120,
        )
        if resp.status_code == 200:
            payload = resp.json()
            result = payload.get("data", {}).get("result", [])
            if not result:
                print(f"  [{label}] HTTP 200 but empty result set. "
                      f"resultType={payload.get('data', {}).get('resultType')}")
            return result
        print(f"  [{label}] Grafana error {resp.status_code}: {resp.text[:800]}")
    except Exception as e:
        print(f"  [{label}] Failed to query Grafana: {type(e).__name__}: {e}")
    return []


def fetch_simulation_failures():
    """Return (by_log, by_venue) as lists of dicts, already sorted desc.

    Missing token or a failed request degrades to empty lists — the report
    still posts with the executed-failures section.
    """
    if not GRAFANA_TOKEN:
        seen = [k for k in _GRAFANA_TOKEN_KEYS if k in os.environ]
        print("Warning: no Grafana token found. Skipping simulation sections.")
        print(f"  tried: {', '.join(_GRAFANA_TOKEN_KEYS)}")
        print(f"  present but empty: {seen or 'none'}")
        print("  -> add the token as a repo secret and pass it in the workflow env block.")
        return [], []

    print(f"Grafana: url={GRAFANA_URL} uid={GRAFANA_DATASOURCE_UID} "
          f"token=${GRAFANA_TOKEN_SRC} ***{GRAFANA_TOKEN[-4:]} (len {len(GRAFANA_TOKEN)})")

    eval_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("Fetching simulation failures by log line...")
    raw_log = run_loki_query(build_sim_by_log_query(), eval_time, "by_log")
    by_log = [
        {
            "program_log": s.get("metric", {}).get("program_log", "(none)"),
            "count": float(s["value"][1]),
        }
        for s in raw_log
    ]

    print("Fetching simulation failures by venue and error code...")
    raw_venue = run_loki_query(build_sim_by_venue_query(), eval_time, "by_venue")
    by_venue = [
        {
            "venue_name": s.get("metric", {}).get("venue_name", "unknown"),
            "err_code": s.get("metric", {}).get("err_code", "-"),
            "count": float(s["value"][1]),
        }
        for s in raw_venue
    ]

    # sort_desc is applied server-side, but JSON ordering is not contractual.
    by_log.sort(key=lambda r: r["count"], reverse=True)
    by_venue.sort(key=lambda r: r["count"], reverse=True)

    print(f"Fetched {len(by_log)} log buckets, {len(by_venue)} venue buckets")
    return by_log, by_venue


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


def build_sim_log_table(rows):
    """Simulation failures by program log line."""
    W = {"log": 62, "n": 10, "pct": 7}
    total = sum(r["count"] for r in rows) or 1

    header = f"{'PROGRAM LOG':<{W['log']}}{'COUNT':>{W['n']}}{'PCT':>{W['pct']}}"
    lines = [header, "-" * len(header)]

    for r in rows:
        log = truncate(r["program_log"].strip(), W["log"] - 1)
        lines.append(
            f"{log:<{W['log']}}{int(r['count']):>{W['n']},}"
            f"{100.0 * r['count'] / total:>{W['pct']}.1f}"
        )

    lines.append("-" * len(header))
    lines.append(f"{f'TOTAL (top {SIM_TOPK})':<{W['log']}}{int(total):>{W['n']},}")
    return "\n".join(lines)


def build_sim_venue_table(rows):
    """Simulation failures by venue and custom error code."""
    W = {"venue": 26, "code": 10, "n": 10, "pct": 7}
    total = sum(r["count"] for r in rows) or 1

    header = (
        f"{'VENUE':<{W['venue']}}{'ERR CODE':>{W['code']}}"
        f"{'COUNT':>{W['n']}}{'PCT':>{W['pct']}}"
    )
    lines = [header, "-" * len(header)]

    for r in rows:
        lines.append(
            f"{truncate(r['venue_name'], W['venue'] - 1):<{W['venue']}}"
            f"{truncate(r['err_code'], W['code'] - 1):>{W['code']}}"
            f"{int(r['count']):>{W['n']},}"
            f"{100.0 * r['count'] / total:>{W['pct']}.1f}"
        )

    lines.append("-" * len(header))
    lines.append(f"{f'TOTAL (top {SIM_TOPK})':<{W['venue'] + W['code']}}{int(total):>{W['n']},}")
    return "\n".join(lines)


def build_samples(df, k=3):
    """Link a few sample txs for the biggest buckets."""
    out = []
    for _, r in df.head(k).iterrows():
        label = truncate(f"{r['owner']} / {r['error_type']}", 60)
        out.append(f"• <{SOLSCAN}{r['sample_tx']}|{r['sample_tx'][:8]}…> — {label}")
    return "\n".join(out)


def send_to_slack(df, sim_by_log, sim_by_venue):
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
            "elements": [{"type": "mrkdwn", "text": f"_{ts}_ · Dune `{QUERY_ID}` + Grafana Loki"}],
        },
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "⛓️ *Executed Failures* (on-chain, top 10)"}},
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

    # ── Simulation failures by program log ──
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"🧪 *Simulation Failures by Program Log* (top {SIM_TOPK})"},
    })
    if sim_by_log:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```\n{build_sim_log_table(sim_by_log)}\n```"},
        })
    else:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_no data — Grafana unavailable or zero matches_"}],
        })

    # ── Simulation failures by venue + err code ──
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"🏛️ *Simulation Failures by Venue* (top {SIM_TOPK})"},
    })
    if sim_by_venue:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"```\n{build_sim_venue_table(sim_by_venue)}\n```"},
        })
    else:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_no data — Grafana unavailable or zero matches_"}],
        })

    payload = {"text": f"DFlow swap failures 24h — {total:,} executed in top 10 buckets", "blocks": blocks}
    resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)

    if resp.status_code == 200:
        print("Posted to Slack.")
    else:
        print(f"Slack failed: {resp.status_code} - {resp.text}")
    return resp.status_code == 200


def diagnose():
    """Walk the query outward one stage at a time to find where it empties out."""
    if not GRAFANA_TOKEN:
        raise SystemExit(
            "No Grafana token in env. Tried: " + ", ".join(_GRAFANA_TOKEN_KEYS)
        )

    eval_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"eval_time={eval_time} url={GRAFANA_URL} uid={GRAFANA_DATASOURCE_UID}")

    stages = [
        ("1. container only",
         'sum(count_over_time({container_name="haze-aggregator-api"}[24h]))'),
        ("2. + line filter",
         'sum(count_over_time({container_name="haze-aggregator-api"} '
         '|= "Failed to simulate transaction" [24h]))'),
        ("3. + program_log regexp",
         'sum(count_over_time({container_name="haze-aggregator-api"} '
         '|= "Failed to simulate transaction" '
         '| regexp `.*Program log: (?P<program_log>[^\\"]*)` '
         '| program_log != "" [24h]))'),
        ("4. + failing_program regexp",
         'sum(count_over_time({container_name="haze-aggregator-api"} '
         '|= "Failed to simulate transaction" '
         '| regexp `Program (?P<failing_program>[1-9A-HJ-NP-Za-km-z]{32,44}) failed:` '
         '| failing_program != "" [24h]))'),
        ("5. + err_code regexp",
         'sum(count_over_time({container_name="haze-aggregator-api"} '
         '|= "Failed to simulate transaction" '
         '| regexp `Program (?P<failing_program>[1-9A-HJ-NP-Za-km-z]{32,44}) failed:` '
         '| regexp `err: InstructionError\\(\\d+, Custom\\((?P<err_code>\\d+)\\)\\)` '
         '| failing_program != "" [24h]))'),
        ("6. full by_log query", SIM_BY_LOG_QUERY),
        ("8. full by_venue query", build_sim_by_venue_query()),
    ]

    for name, q in stages:
        res = run_loki_query(q, eval_time, name)
        if res:
            vals = [float(s["value"][1]) for s in res]
            print(f"{name}: {len(res)} series, total {sum(vals):,.0f}")
        else:
            print(f"{name}: EMPTY")

    print("\nOne raw log line for reference:")
    url = (f"{GRAFANA_URL}/api/datasources/proxy/uid/{GRAFANA_DATASOURCE_UID}"
           "/loki/api/v1/query_range")
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {GRAFANA_TOKEN}"},
            params={
                "query": '{container_name="haze-aggregator-api"} '
                         '|= "Failed to simulate transaction"',
                "limit": 1,
                "start": int((datetime.now(timezone.utc).timestamp() - 86400) * 1e9),
                "end": int(datetime.now(timezone.utc).timestamp() * 1e9),
            },
            timeout=60,
        )
        streams = r.json().get("data", {}).get("result", [])
        if streams and streams[0].get("values"):
            print(streams[0]["values"][0][1][:1500])
        else:
            print("(no matching lines in the last 24h)")
    except Exception as e:
        print(f"raw fetch failed: {e}")


def main():
    for name, val in (("DUNE_API_KEY", DUNE_API_KEY), ("SLACK_WEBHOOK_FAILURES", SLACK_WEBHOOK_URL)):
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
    print("\nExecuted failures:")
    print(df[["scope", "owner", "error_type", "failures"]].to_string(index=False))

    sim_by_log, sim_by_venue = fetch_simulation_failures()

    if sim_by_log:
        print("\nSimulation failures by program log:")
        print(build_sim_log_table(sim_by_log))
    if sim_by_venue:
        print("\nSimulation failures by venue:")
        print(build_sim_venue_table(sim_by_venue))

    send_to_slack(df, sim_by_log, sim_by_venue)


if __name__ == "__main__":
    import sys
    if "--diag" in sys.argv:
        diagnose()
    else:
        main()
