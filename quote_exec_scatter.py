#!/usr/bin/env python3
"""
Quote vs exec spread -> Slack. Runs in GitHub Actions.

Repo path: quote_exec_scatter.py  (root)

Uploads the chart via Slack's own file API, so the summary text and the image
arrive in ONE call. There is no code path that posts text without a chart.

Env:
  DUNE_API_KEY       Dune API key
  SLACK_BOT_TOKEN    xoxb-... with files:write, bot invited to the channel
  SLACK_CHANNEL_ID   e.g. C0123456789

Any failure raises, so the Actions run goes red instead of quietly
delivering nothing.
"""

import io
import os
import sys
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import requests
from dune_client.client import DuneClient
from dune_client.query import QueryBase

QUERY_ID = 8286755
LINTHRESH = 1
FORCE_RUN = False
CHART = "quote_exec_combined.png"

REQUIRED = {"block_time", "venue", "tx_success", "wide_bps"}


def env(name, *alts):
    for n in (name, *alts):
        v = os.getenv(n)
        if v:
            return v
    sys.exit(f"Missing required env var: {name}")


def fetch():
    """Cached results first, execute if empty. A brand-new query has no cache."""
    dune = DuneClient(env("DUNE_API_KEYY"))
    rows = []

    if not FORCE_RUN:
        try:
            res = dune.get_latest_result(QUERY_ID)
            rows = res.result.rows if res and res.result else []
            print(f"cached: {len(rows)} rows")
        except Exception as e:
            print(f"get_latest_result failed: {e}")

    if not rows:
        print("executing query...")
        res = dune.run_query(QueryBase(query_id=QUERY_ID), ping_frequency=10)
        rows = res.result.rows if res and res.result else []
        print(f"fresh: {len(rows)} rows")

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"Query {QUERY_ID} returned 0 rows")

    missing = REQUIRED - set(df.columns)
    if missing:
        raise RuntimeError(
            f"Query {QUERY_ID} missing {sorted(missing)}; got {list(df.columns)}"
        )

    df["block_time"] = pd.to_datetime(df["block_time"])
    df["wide_bps"] = pd.to_numeric(df["wide_bps"], errors="coerce")
    for c in ("quote", "exec"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    bad = int(df["wide_bps"].isna().sum())
    if bad:
        print(f"dropping {bad} rows with unparseable wide_bps")
        df = df.dropna(subset=["wide_bps"])

    if df.empty:
        raise RuntimeError("all rows dropped after cleaning")
    return df


def build_chart(df):
    venues = sorted(df["venue"].unique())
    cmap = plt.get_cmap("tab10" if len(venues) <= 10 else "tab20")
    colors = {v: cmap(i % cmap.N) for i, v in enumerate(venues)}

    fig, ax = plt.subplots(figsize=(14, 7))
    for venue, vdata in df.groupby("venue"):
        for ok, marker, size in ((True, "o", 28), (False, "x", 44)):
            sub = vdata[vdata["tx_success"].astype(bool) == ok]
            if sub.empty:
                continue
            kw = dict(c=[colors[venue]], marker=marker, s=size, alpha=0.65)
            # edgecolors is ignored on unfilled markers and warns if passed
            kw.update({"edgecolors": "white", "linewidth": 0.4} if ok
                      else {"linewidth": 1.4})
            ax.scatter(sub["block_time"], sub["wide_bps"], **kw)

    ax.set_yscale("symlog", linthresh=LINTHRESH)
    ax.axhline(0, color="red", lw=1, alpha=0.5)
    ax.grid(True, which="both", ls="--", alpha=0.3)
    ax.set_xlabel("block_time (UTC)")
    ax.set_ylabel(f"wide_bps (symlog, linthresh={LINTHRESH})")
    ax.set_title(
        "Quote vs exec spread by venue\n"
        f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}"
    )
    handles = [
        plt.Line2D([], [], marker="o", ls="", color=colors[v],
                   label=f"{v} ({int((df.venue == v).sum())})")
        for v in venues
    ]
    ax.legend(handles=handles, loc="upper left", bbox_to_anchor=(1.02, 1),
              fontsize=9, title="venue (n)   o=ok  x=fail")
    plt.xticks(rotation=45, ha="right")
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    fig.savefig(CHART, dpi=140, bbox_inches="tight")   # artifact for the run
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def build_summary(df):
    stats = df.groupby("venue")["wide_bps"].agg(
        n="count",
        median="median",
        q95=lambda s: s.quantile(0.95),
        nuniq="nunique",
    )
    fails = df.groupby("venue")["tx_success"].apply(
        lambda s: int((~s.astype(bool)).sum())
    )
    stats["fail_pct"] = (100 * fails / stats["n"]).round(2)
    stats = stats.sort_values("n", ascending=False)

    lines = [
        "*Quote vs exec spread* — last 24h",
        f"_{datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}_",
        f"{len(df)} fills across {df['venue'].nunique()} venues  |  "
        f"range {df['wide_bps'].min():.2f} to {df['wide_bps'].max():.2f} bps",
        "",
        "```",
        f"{'venue':18s} {'n':>6s} {'median':>8s} {'q95':>8s} {'fail%':>7s} {'uniq':>6s}",
    ]
    for venue, r in stats.iterrows():
        lines.append(
            f"{venue[:18]:18s} {int(r['n']):6d} {r['median']:8.2f} "
            f"{r['q95']:8.2f} {r['fail_pct']:7.2f} {int(r['nuniq']):6d}"
        )
    lines.append("```")

    # A venue with a handful of distinct values is a fixed offset, not a
    # spread. Positive median means it delivered LESS than it quoted.
    flat = stats[(stats["nuniq"] <= 10) & (stats["n"] > 100)]
    for venue, r in flat.iterrows():
        direction = ("under-delivers vs quote" if r["median"] > 0
                     else "quotes conservatively")
        lines.append(
            f"• `{venue}` is effectively constant at {r['median']:.2f} bps "
            f"({int(r['nuniq'])} distinct values / {int(r['n'])} fills) — {direction}"
        )

    worst = stats["fail_pct"].idxmax()
    if stats.loc[worst, "fail_pct"] > 5:
        lines.append(
            f"• `{worst}` failure rate {stats.loc[worst, 'fail_pct']:.1f}% "
            f"— well above the rest"
        )

    return "\n".join(lines)[:3900]      # Slack initial_comment limit


def post_to_slack(image_bytes, comment):
    """One call delivers image + text. No path posts text alone."""
    token = env("SLACK_BOT_TOKEN")
    channel = env("SLACK_CHANNEL_ID")
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        headers=headers,
        data={"filename": CHART, "length": len(image_bytes)},
        timeout=30,
    )
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"getUploadURLExternal failed: {j}")
    upload_url, file_id = j["upload_url"], j["file_id"]

    r = requests.post(
        upload_url,
        files={"file": (CHART, image_bytes, "image/png")},
        timeout=120,
    )
    r.raise_for_status()

    r = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
        json={
            "files": [{"id": file_id, "title": "Quote vs exec spread"}],
            "channel_id": channel,
            "initial_comment": comment,
        },
        timeout=30,
    )
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"completeUploadExternal failed: {j}")
    print("posted to Slack")


def main():
    df = fetch()
    image = build_chart(df)
    comment = build_summary(df)
    print(comment)
    post_to_slack(image, comment)


if __name__ == "__main__":
    main()
