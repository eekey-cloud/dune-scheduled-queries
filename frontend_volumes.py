#!/usr/bin/env python3
"""
Daily top-10 frontend report for Slack, sent as one image.

1. Pulls 4 Dune queries: volume & txns, market share, fee payers, Phantom revenue
2. Draws one PNG: charts/frontend_report.png
3. Pushes the PNG to the CHART_BRANCH branch of this repo (the branch is replaced every run)
4. Posts it to Slack with the incoming webhook, using a raw.githubusercontent.com link

Slack webhooks can't take files, only image links, and Slack must open the link without
logging in. So the repo that holds the image has to be public. If this repo is private,
set CHART_REPO to a public repo (plus CHART_REPO_TOKEN that can push to it).

Env vars:
  DUNE_API_KEY_FRONTEND     Dune API key
  SLACK_WEBHOOK_FRONTEND    Slack incoming webhook URL
  GITHUB_TOKEN              given by GitHub Actions (pass it in the yml), used to push the image
  CHART_REPO, CHART_REPO_TOKEN   optional: push the image to a different (public) repo

pip install dune-client python-dotenv requests matplotlib numpy
Run locally without GITHUB_TOKEN and it only saves the PNG (nothing is posted).
"""

import math
import os
import shutil
import subprocess
import tempfile
import textwrap
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no screen on GitHub runners
import matplotlib.pyplot as plt
import numpy as np
import requests
from dotenv import load_dotenv
from dune_client.client import DuneClient
from matplotlib import font_manager
from matplotlib.colors import to_rgb
from matplotlib.patches import Circle, FancyBboxPatch, Wedge

load_dotenv()

# --- Config: Dune & Slack ---
DUNE_API_KEY = os.getenv("DUNE_API_KEY_FRONTEND")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_FRONTEND")
QUERY_ID = 6928394
SHARE_QUERY_ID = 7377109
FEE_QUERY_ID = 7426532
PHANTOM_REVENUE_QUERY_ID = 7856440

# --- Config: where the image lives (a branch of this repo, replaced every run) ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
CHART_REPO = os.getenv("CHART_REPO", "").strip() or os.getenv("GITHUB_REPOSITORY", "").strip()
CHART_TOKEN = os.getenv("CHART_REPO_TOKEN", "").strip() or GITHUB_TOKEN
CHART_BRANCH = "frontend-report-charts"
OUT = Path("charts/frontend_report.png")

# Router name -> column prefix in query 7377109 (<prefix>_volume_pct and <prefix>_txn_pct)
ROUTER_COLS = {"DFlow": "dflow", "OKX": "okx", "Jupiter": "jupiter", "Titan": "titan",
               "Coinbase program": "coinbase_program"}


# ============================== data ==============================
def fetch_rows(query_id, label):
    """Latest result of a Dune query (runs the query if there is no cached result)."""
    print(f"Fetching {label} from Dune query {query_id}...")
    dune = DuneClient(DUNE_API_KEY)
    try:
        result = dune.get_latest_result(query_id)
    except Exception as e:
        print(f"No cached results found, executing query... ({e})")
        from dune_client.query import QueryBase
        result = dune.run_query(QueryBase(query_id=query_id))
    rows = result.result.rows
    print(f"Fetched {len(rows)} {label} rows")
    return rows


def f0(x):
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def build_data(vol_rows, share_rows, fee_rows, rev_rows):
    today = datetime.now(timezone.utc).date()
    day, prev = today - timedelta(days=1), today - timedelta(days=2)

    frontends = {r.get("client_name", "Unknown"): (f0(r.get("yesterday_volume")), f0(r.get("day_before_volume")),
                                                   f0(r.get("yesterday_txns")), f0(r.get("day_before_txns")))
                 for r in vol_rows[:10]}
    market_share = {r.get("client_name", "Unknown"): {
        "vol": {name: f0(r.get(f"{col}_volume_pct")) for name, col in ROUTER_COLS.items()},
        "txns": {name: f0(r.get(f"{col}_txn_pct")) for name, col in ROUTER_COLS.items()},
        "vol_chg": f0(r.get("dflow_volume_share_change")),
        "txns_chg": f0(r.get("dflow_txn_share_change")),
    } for r in share_rows}
    fee_payers = {r.get("client_name", "Unknown"): (f0(r.get("yesterday_signers")), f0(r.get("day_before_signers")))
                  for r in fee_rows}
    rev = sorted(rev_rows, key=lambda r: str(r.get("block_date", "")))[-2:]
    rev = [(f0(r.get("all_phantom_revenue")), f0(r.get("phantom_pm_revenue"))) for r in rev]
    rev = [(0.0, 0.0)] * (2 - len(rev)) + rev  # older day first

    return {"day": day.strftime("%b %d"), "prev": prev.strftime("%b %d"), "long_day": day.strftime("%A, %B %d, %Y"),
            "frontends": frontends, "market_share": market_share, "fee_payers": fee_payers,
            "phantom_rev": {"prev": rev[0], "today": rev[1]}}


# ============================== look ==============================
DPI = 150
W, H = 18, 12  # inches; the page is drawn in inch coordinates

BG_DEEP, BG_GLOW, BG_TINT = "#070B16", "#17213F", "#1A1335"
CARD, EDGE, TRACK = "#0E1528", "#1F2B4A", "#1A2440"
TEXT, MUTED, FAINT = "#E9EDF7", "#95A1BD", "#5B6786"
UP, DOWN, ACCENT = "#5EE3A1", "#FF8098", "#8B7BFF"
FRONT = {"fomo": "#FF9E7D", "phantom": "#AB9DFF", "jupiter": "#5ED3B3", "coinbase": "#6FA8FF",
         "kamino": "#D9A877", "li.fi": "#F48FB8", "tessera": "#67D5EE", "solflare": "#FFD36E",
         "sunrise": "#B9E58A", "socketbungee": "#A3AEC6"}
SPARE = ["#7FB2FF", "#F6A6C9", "#9FE3C9", "#FFC48C", "#C3B4FF"]
ROUTER = {"DFlow": ACCENT, "OKX": "#C9D2E6", "Jupiter": "#5ED3B3", "Titan": "#F7B267",
          "Coinbase program": "#6FA8FF"}
PM_C, OTHER_C = "#FFB86B", "#AB9DFF"

FONT_DIR = Path(__file__).with_name("fonts")
FONT_URL = "https://raw.githubusercontent.com/google/fonts/main/ofl/barlow/Barlow-{}.ttf"


def load_font():
    """Use Barlow (downloaded once into ./fonts); fall back to DejaVu Sans if that fails."""
    FONT_DIR.mkdir(exist_ok=True)
    try:
        for style in ("Regular", "Medium", "SemiBold", "Bold"):
            f = FONT_DIR / f"Barlow-{style}.ttf"
            if not f.exists():
                urllib.request.urlretrieve(FONT_URL.format(style), f)
            font_manager.fontManager.addfont(str(f))
        return "Barlow"
    except Exception as e:
        print(f"Font download failed, using DejaVu Sans ({e})")
        return "DejaVu Sans"


def color(name):
    return FRONT.get(name.lower(), SPARE[sum(map(ord, name)) % len(SPARE)])


def usd(v, k_digits=1):
    if v >= 1e6:
        return f"${v / 1e6:.2f}m"
    if v >= 1e4:
        return f"${v / 1e3:.{k_digits}f}k"
    return f"${v:,.0f}"


def cnt(v):
    if v >= 1e6:
        return f"{v / 1e6:.2f}m"
    if v >= 1e3:
        return f"{v / 1e3:.1f}k"
    return f"{v:,.0f}"


def pct(a, b):
    return (a / b - 1) * 100 if b else None


def chg(p):
    if p is None:
        return "new"
    if p >= 900:  # big jumps read better as a multiple, e.g. 1,098×
        return f"{p / 100 + 1:,.0f}×"
    return ("+" if p >= 0 else "−") + f"{abs(p):.1f}%"


def signed_usd(v, k_digits=1):
    return ("+" if v >= 0 else "−") + usd(abs(v), k_digits)


class Page:
    def __init__(self, font):
        plt.rcParams.update({"font.family": [font, "DejaVu Sans"], "text.parse_math": False})
        self.fig = plt.figure(figsize=(W, H), dpi=DPI)
        self.ax = self.fig.add_axes([0, 0, 1, 1])
        self.ax.axis("off")
        self.r = self.fig.canvas.get_renderer()

    def text(self, x, y, s, size=11, c=TEXT, weight="regular", ha="left", va="baseline", z=6, **kw):
        return self.ax.text(x, y, s, fontsize=size, color=c, fontweight=weight, ha=ha, va=va, zorder=z, **kw)

    def width(self, t):
        return t.get_window_extent(self.r).width / DPI

    def pill(self, x, y, label, good, size=9.5, ha="left"):
        c = UP if good else DOWN
        t = self.text(x, y, label, size, c, "semibold", ha=ha, va="center",
                      bbox=dict(boxstyle="round,pad=0.3,rounding_size=0.75", fc=(*to_rgb(c), .14), ec="none"))
        return t, self.width(t) + 0.6 * size / 72

    def chg_pill(self, x, y, p, size=9.5, ha="left"):
        return self.pill(x, y, chg(p), (p or 0) >= 0, size, ha)

    def rbox(self, x, y, w, h, r, fc="none", ec="none", lw=1, z=1, alpha=1):
        p = FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={r}", fc=fc, ec=ec, lw=lw,
                           zorder=z, alpha=alpha)
        self.ax.add_patch(p)
        return p

    def bar(self, x0, x1, yc, h, c, z=3):
        """Rounded bar from x0 to x1 that fades in towards its tip."""
        lo, hi = min(x0, x1), max(x0, x1)
        if hi - lo < h:
            lo, hi = (hi - h, hi) if x1 < x0 else (lo, lo + h)
        clip = self.rbox(lo, yc - h / 2, hi - lo, h, h / 2, z=z)
        img = np.zeros((1, 256, 4))
        img[..., :3] = to_rgb(c)
        img[..., 3] = np.linspace(.12, 1, 256)[::-1] if x1 < x0 else np.linspace(.12, 1, 256)
        im = self.ax.imshow(img, extent=(lo, hi, yc - h / 2, yc + h / 2), aspect="auto",
                            interpolation="bilinear", zorder=z)
        im.set_clip_path(clip)

    def glow_dot(self, x, y, r, c, z=5):
        for k, a in ((3.2, .06), (2.2, .1), (1.5, .18)):
            self.ax.add_patch(Circle((x, y), r * k, fc=c, ec="none", alpha=a, zorder=z))
        self.ax.add_patch(Circle((x, y), r, fc=c, ec="none", zorder=z + 1))

    def orb(self, x, y, r, c, z=4):
        """Circle with a soft radial light and a faint halo."""
        for k, a in ((1.35, .05), (1.18, .09)):
            self.ax.add_patch(Circle((x, y), r * k, fc=c, ec="none", alpha=a, zorder=z))
        clip = Circle((x, y), r, fc="none", ec="none", zorder=z)
        self.ax.add_patch(clip)
        yy, xx = np.mgrid[-1:1:200j, -1:1:200j]
        d = np.clip(np.hypot(xx + .35, yy - .35) / 1.6, 0, 1)
        img = np.zeros((200, 200, 4))
        img[..., :3] = np.clip(np.array(to_rgb(c)) * (1 - .35 * d[..., None]) + .25 * (1 - d[..., None]), 0, 1)
        img[..., 3] = .95
        im = self.ax.imshow(img, extent=(x - r, x + r, y - r, y + r), origin="lower", aspect="auto", zorder=z + 1)
        im.set_clip_path(clip)

    def ring(self, x, y, r, width, parts, z=3, gap=1.5):
        """One donut ring. parts = [(value, colour)], drawn clockwise from 12 o'clock."""
        self.ax.add_patch(Wedge((x, y), r, 0, 360, width=width, fc=TRACK, ec="none", zorder=z - 1))
        tot, start = sum(v for v, _ in parts), 90
        for v, c in parts:
            if v <= 0 or tot <= 0:
                continue
            span = v / tot * 360
            self.ax.add_patch(Wedge((x, y), r, start - span + gap / 2, start - gap / 2, width=width, fc=c,
                                    ec="none", zorder=z))
            start -= span

    def background(self):
        yy, xx = np.mgrid[0:1:240j, 0:1:360j]
        glow = np.clip(1 - np.hypot(xx - .12, (yy - .98) * 1.3) / .95, 0, 1) ** 1.6
        tint = np.clip(1 - np.hypot(xx - .95, (yy - .05) * 1.2) / .8, 0, 1) ** 2
        deep = np.array(to_rgb(BG_DEEP))
        img = (deep + (np.array(to_rgb(BG_GLOW)) - deep) * glow[..., None]
               + (np.array(to_rgb(BG_TINT)) - deep) * tint[..., None])
        self.ax.imshow(np.clip(img, 0, 1), extent=(0, W, 0, H), origin="lower", aspect="auto", zorder=-10,
                       interpolation="bicubic")

    def card(self, x, y, w, h):
        self.rbox(x, y, w, h, .2, fc=CARD, ec=EDGE, lw=1, z=0, alpha=.88)

    def section_title(self, x, top, num, title, note, wrap):
        self.ax.add_patch(Circle((x + .45, top - .42), .16, fc=ACCENT, ec="none", zorder=5))
        self.text(x + .45, top - .425, str(num), 11, "#FFFFFF", "bold", ha="center", va="center")
        self.text(x + .72, top - .42, title, 17, TEXT, "semibold", va="center")
        self.text(x + .3, top - .74, textwrap.fill(note, wrap), 11.5, MUTED, va="top", linespacing=1.35)

    def no_data(self, x0, y0, w, h):
        self.text(x0 + w / 2, y0 + h / 2 - .4, "No data today", 13, FAINT, ha="center", va="center")

    def save(self, path):
        self.ax.set_xlim(0, W)
        self.ax.set_ylim(0, H)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.fig.savefig(path, dpi=DPI)
        plt.close(self.fig)
        from PIL import Image  # Pillow ships with matplotlib
        Image.open(path).convert("RGB").save(path, optimize=True)  # smaller file, no alpha channel
        return path


# ============================== page parts ==============================
def header(p, d):
    rows = list(d["frontends"].items())
    day = p.text(.35, 11.42, d["day"], 27, TEXT, "bold")
    p.text(.35 + p.width(day) + .22, 11.42, f"vs  {d['prev']}", 27, MUTED, "medium")

    gap, x0, y0, th = .2, .3, 10.15, .95
    tw = (W - 2 * x0 - gap) / 2
    for i, (idx, label, fmt, accent) in enumerate(((0, "Volume", usd, ACCENT), (2, "Transactions", cnt, FRONT["jupiter"]))):
        x = x0 + i * (tw + gap)
        now, before = sum(r[idx] for _, r in rows), sum(r[idx + 1] for _, r in rows)
        p.rbox(x, y0, tw, th, .16, fc=CARD, ec=EDGE, z=0, alpha=.88)
        p.rbox(x + .2, y0 + th - .07, .5, .035, .0175, fc=accent, z=2)
        p.text(x + .2, y0 + th - .27, f"{label}, top 10 frontends only", 10.5, MUTED, "medium", va="center")
        vt = p.text(x + .2, y0 + .17, fmt(now), 24, TEXT, "semibold")
        px = x + .2 + p.width(vt) + .14
        _, pw = p.chg_pill(px, y0 + .3, pct(now, before), 10)
        p.text(px + pw + .12, y0 + .3, f"was {fmt(before)}", 10, FAINT, va="center")

        # who makes up the total: thin strip + top 3
        order = sorted(rows, key=lambda kv: -kv[1][idx])
        sx0, sx1, sy = x + 4.0, x + tw - .25, y0 + th - .3
        p.rbox(sx0, sy - .04, sx1 - sx0, .08, .04, fc=TRACK, z=2)
        pos = sx0
        for n, r in order:
            seg = (sx1 - sx0) * r[idx] / now if now else 0
            if seg > .02:
                p.rbox(pos + .008, sy - .04, seg - .016, .08, min(.04, seg / 2 - .008), fc=color(n), z=3)
            pos += seg
        lx = sx0
        for n, r in order[:3]:
            p.glow_dot(lx + .04, y0 + .3, .035, color(n))
            t1 = p.text(lx + .13, y0 + .3, n, 10, MUTED, va="center")
            t2 = p.text(lx + .13 + p.width(t1) + .07, y0 + .3, f"{r[idx] / now * 100:.1f}%" if now else "–", 10,
                        TEXT, "semibold", va="center")
            lx = t2.get_position()[0] + p.width(t2) + .25
        if len(order) > 3 and now:
            rest = sum(r[idx] for _, r in order[3:]) / now * 100
            p.text(lx, y0 + .3, f"other {len(order) - 3}: {rest:.1f}%", 10, FAINT, va="center")


def section_flow(p, d, x0, y0, w, h):
    """1. Volume and transactions for the top 10 frontends."""
    p.card(x0, y0, w, h)
    rows = sorted(d["frontends"].items(), key=lambda kv: -kv[1][0])
    tv = sum(r[0] for _, r in rows) or 1
    tt = sum(r[2] for _, r in rows) or 1
    prev_rank = {n: i + 1 for i, (n, _) in enumerate(sorted(rows, key=lambda kv: -kv[1][1]))}

    moves = [(n, r[0] - r[1], pct(r[0], r[1])) for n, r in rows]
    drop, gain = min(moves, key=lambda m: m[1]), max(moves, key=lambda m: m[1])
    note = f"{rows[0][0]} is {rows[0][1][0] / tv * 100:.0f}% of volume and {rows[0][1][2] / tt * 100:.0f}% of transactions. "
    if drop[1] < 0:
        note += f"Biggest drop: {drop[0]} {signed_usd(drop[1])} ({chg(drop[2])}). "
    if gain[1] > 0:
        note += f"Biggest gain: {gain[0]} {signed_usd(gain[1])} ({chg(gain[2])}). "
    split = [(n, r) for n, r in rows if r[1] and r[3] and abs(pct(r[0], r[1])) >= 3 and abs(pct(r[2], r[3])) >= 3
             and (r[0] >= r[1]) != (r[2] >= r[3])]
    if split:
        n, r = split[0]
        note += f"{n}: transactions {chg(pct(r[2], r[3]))} but volume {chg(pct(r[0], r[1]))}."
    p.section_title(x0, y0 + h, 1, "Volume & transactions, top 10 frontends", note, 215)

    V_OUT, V_IN = 1.95, 7.25                     # volume bars grow left from V_IN
    N_RANK, N_DOT, N_NAME, N_VS, N_TS = 7.65, 8.2, 8.35, 10.0, 10.62
    T_IN, T_OUT = 10.8, 16.1                     # transaction bars grow right from T_IN

    top = y0 + h - 1.25
    p.text(V_IN, top, "Volume  (log scale)", 11, MUTED, "medium", ha="right")
    p.text(N_RANK, top, "#", 11, MUTED, "medium", ha="right")
    p.text(N_NAME, top, "Frontend", 11, MUTED, "medium")
    p.text(N_VS, top, "vol share", 10, MUTED, "medium", ha="right")
    p.text(N_TS, top, "txn share", 10, MUTED, "medium", ha="right")
    p.text(T_IN, top, "Transactions  (log scale)", 11, MUTED, "medium")

    vpos = [r[0] for _, r in rows if r[0] > 0] or [1]
    tpos = [r[2] for _, r in rows if r[2] > 0] or [1]
    v_lo, v_hi = math.floor(math.log10(min(vpos))), math.log10(max(vpos))
    t_lo, t_hi = math.floor(math.log10(min(tpos))), math.log10(max(tpos))
    v_hi, t_hi = max(v_hi, v_lo + 1), max(t_hi, t_lo + 1)
    vx = lambda v: V_IN - (math.log10(v) - v_lo) / (v_hi - v_lo) * (V_IN - V_OUT) if v > 10 ** v_lo else V_IN
    tx = lambda v: T_IN + (math.log10(v) - t_lo) / (t_hi - t_lo) * (T_OUT - T_IN) if v > 10 ** t_lo else T_IN

    first = top - .34
    pitch = min(.34, (first - (y0 + .3)) / max(1, len(rows) - 1))
    bh = .16
    for i, (n, (v, vp, t, tp)) in enumerate(rows):
        y, c = first - i * pitch, color(n)
        if i % 2 == 0:
            p.rbox(x0 + .15, y - pitch / 2, w - .3, pitch, .06, fc=TRACK, z=0.5, alpha=.35)

        # volume (left)
        tip = vx(v)
        p.bar(V_IN, tip, y, bh, c)
        edge = tip
        if vp > 10 ** v_lo:
            xp = vx(vp)
            p.ax.plot([xp, xp], [y - bh * .9, y + bh * .9], color=TEXT, lw=1.6, alpha=.75, zorder=4,
                      solid_capstyle="round")
            edge = min(edge, xp)
        vt = p.text(edge - .12, y, usd(v), 11.5, TEXT, "semibold", ha="right", va="center")
        p.chg_pill(edge - .12 - p.width(vt) - .12, y, pct(v, vp), 9.5, ha="right")

        # name column
        mv = prev_rank[n] - (i + 1)
        p.text(N_RANK, y, str(i + 1), 11, FAINT, "medium", ha="right", va="center")
        if mv:
            p.text(N_RANK + .07, y, ("▲" if mv > 0 else "▼") + str(abs(mv)), 8, UP if mv > 0 else DOWN,
                   "semibold", va="center")
        p.glow_dot(N_DOT, y, .045, c)
        p.text(N_NAME, y, n, 12, TEXT, "semibold", va="center")
        p.text(N_VS, y, f"{v / tv * 100:.1f}%", 11, MUTED, ha="right", va="center")
        p.text(N_TS, y, f"{t / tt * 100:.1f}%", 11, MUTED, ha="right", va="center")

        # transactions (right)
        tip = tx(t)
        p.bar(T_IN, tip, y, bh, c)
        edge = tip
        if tp > 10 ** t_lo:
            xp = tx(tp)
            p.ax.plot([xp, xp], [y - bh * .9, y + bh * .9], color=TEXT, lw=1.6, alpha=.75, zorder=4,
                      solid_capstyle="round")
            edge = max(edge, xp)
        tt_ = p.text(edge + .12, y, cnt(t), 11.5, TEXT, "semibold", va="center")
        p.chg_pill(edge + .12 + p.width(tt_) + .12, y, pct(t, tp), 9.5)


def section_share(p, d, x0, y0, w, h):
    """2. Router share of volume and txns inside each frontend."""
    p.card(x0, y0, w, h)
    vol_of = {k.lower(): v[0] for k, v in d["frontends"].items()}
    items = sorted(d["market_share"].items(), key=lambda kv: -vol_of.get(kv[0].lower(), 0))
    if not items:
        p.section_title(x0, y0 + h, 2, "Volume & txn market share", "", 92)
        return p.no_data(x0, y0, w, h)

    fell = [n for n, s in items if s["vol_chg"] < 0]
    worst = min(items, key=lambda kv: kv[1]["vol_chg"])
    gap = max(items, key=lambda kv: abs(kv[1]["txns"]["DFlow"] - kv[1]["vol"]["DFlow"]))
    note = f"DFlow volume share fell at {len(fell)} of {len(items)}"
    note += f"; biggest drop at {worst[0]} ({chg(worst[1]['vol_chg'])}). " if fell else ". "
    note += (f"At {gap[0]}, DFlow has {gap[1]['txns']['DFlow']:.0f}% of txns and "
             f"{gap[1]['vol']['DFlow']:.0f}% of volume.")
    p.section_title(x0, y0 + h, 2, "Volume & txn market share", note, 92)

    used = lambda s, r: s["vol"].get(r, 0) > 0 or s["txns"].get(r, 0) > 0
    lx, ly = x0 + .3, y0 + h - 1.38
    for name, c in ROUTER.items():
        if any(used(s, name) for _, s in items):
            p.glow_dot(lx + .05, ly, .045, c)
            t = p.text(lx + .16, ly, name, 10, MUTED, "medium", va="center")
            lx += p.width(t) + .4
    N_L, B_L, B_R, P_L = x0 + .3, x0 + 1.95, x0 + w - 1.0, x0 + w - .88
    p.text(P_L, ly, f"DFlow vs {d['prev']}", 9, FAINT, va="center")

    bh = (ly - .28 - (y0 + .2)) / len(items)
    for i, (name, s) in enumerate(items):
        yt = ly - .28 - i * bh
        if i:
            p.ax.plot([N_L, x0 + w - .3], [yt + .02, yt + .02], color=TRACK, lw=1, zorder=1)
        routers = ["DFlow"] + [r for r in ROUTER if r != "DFlow" and used(s, r)]
        rv = 1 + sum(v > s["vol"]["DFlow"] for v in s["vol"].values())
        rt = 1 + sum(v > s["txns"]["DFlow"] for v in s["txns"].values())
        p.text(N_L, yt - .2, name, 13, TEXT, "semibold", va="center")
        p.text(N_L, yt - .36, f"DFlow rank\n#{rv} vol, #{rt} txns", 8.5, MUTED, va="top", linespacing=1.3)

        for j, (key, label, change) in enumerate((("vol", "Volume", s["vol_chg"]), ("txns", "Txns", s["txns_chg"]))):
            y, bar_h = yt - .25 - j * .3, .21
            p.text(B_L - .1, y, label, 9, FAINT, ha="right", va="center")
            p.rbox(B_L, y - bar_h / 2, B_R - B_L, bar_h, bar_h / 2, fc=TRACK, z=2)
            tot, x = sum(s[key].values()), B_L
            for r in routers:
                v = s[key].get(r, 0)
                seg = (B_R - B_L) * v / tot if tot else 0
                if seg <= 0:
                    continue
                p.rbox(x + .012, y - bar_h / 2, max(seg - .024, .01), bar_h, min(bar_h / 2, max(seg / 2 - .012, .005)),
                       fc=ROUTER.get(r, FAINT), z=3)
                if seg >= .3:   # number fits inside the bar
                    p.text(x + seg / 2, y, f"{v:.1f}%" if seg >= .5 else f"{v:.1f}", 9, "#0B1020",
                           "bold" if r == "DFlow" else "semibold", ha="center", va="center", z=7)
                else:           # too thin: write it just above (volume) or below (txns) the bar
                    ty = y + bar_h / 2 + .03 if j == 0 else y - bar_h / 2 - .03
                    cx_ = min(x + seg / 2, B_R - .2)
                    p.text(cx_, ty, f"{v:.1f}%", 8, ROUTER.get(r, FAINT), "semibold", ha="center",
                           va="bottom" if j == 0 else "top", z=7)
                x += seg
            p.chg_pill(P_L, y, change, 9)


def section_payers(p, d, x0, y0, w, h):
    """3. Fee payers (unique wallets) per frontend."""
    p.card(x0, y0, w, h)
    fr = {k.lower(): v for k, v in d["frontends"].items()}
    rows = sorted(d["fee_payers"].items(), key=lambda kv: -kv[1][0])[:6]
    if not rows or not rows[0][1][0]:
        p.section_title(x0, y0 + h, 3, "Fee payers", "", 52)
        return p.no_data(x0, y0, w, h)
    rows = [(n, fp, fpp, *(lambda f: (f[2] / fp, f[0] / fp) if f and fp else (None, None))(fr.get(n.lower())))
            for n, (fp, fpp) in rows]
    tot, totp = sum(r[1] for r in rows), sum(r[2] for r in rows)
    known = [r for r in rows if r[3] is not None]
    note = f"{cnt(tot)} fee payers ({chg(pct(tot, totp))}). "
    if known:
        active, spend = max(known, key=lambda r: r[3]), max(known, key=lambda r: r[4])
        note += (f"{active[0]} wallets are the most active ({active[3]:.1f} txns) and spend the most "
                 f"({usd(active[4])})." if active is spend else
                 f"Most active: {active[0]} ({active[3]:.1f} txns each). Biggest spend: {spend[0]} ({usd(spend[4])}).")
    p.section_title(x0, y0 + h, 3, "Fee payers", note, 52)

    # each circle gets a slot as wide as the circle (but never narrower than its name)
    fmax, room = rows[0][1], w - .4
    min_slot = min(.85, room / len(rows))
    size = lambda rm, fp: max(.07, rm * math.sqrt(fp / fmax))
    rmax = .65
    while rmax > .2 and sum(max(2 * size(rmax, r[1]) + .1, min_slot) for r in rows) > room:
        rmax -= .01
    slots = [max(2 * size(rmax, r[1]) + .1, min_slot) for r in rows]
    space_top, space_bottom = y0 + h - 1.25, y0 + .2          # room left under the note
    base = (space_top + space_bottom) / 2 - (2 * rmax - .62) / 2  # circles + names + pills sit in the middle
    left = x0 + (w - sum(slots)) / 2
    for i, (n, fp, fpp, tpw, vpw) in enumerate(rows):
        cx, c = left + sum(slots[:i]) + slots[i] / 2, color(n)
        r = size(rmax, fp)
        p.orb(cx, base + r, r, c)
        if r >= .3:
            p.text(cx, base + r, cnt(fp), 13, "#0B1020", "bold", ha="center", va="center", z=8)
        else:
            p.text(cx, base + 2 * r + .14, cnt(fp), 12, TEXT, "semibold", ha="center", va="center")
        p.text(cx, base - .22, n, 12, TEXT, "semibold", ha="center", va="center")
        p.chg_pill(cx, base - .5, pct(fp, fpp), 9, ha="center")


def section_phantom(p, d, x0, y0, w, h):
    """4. Phantom revenue, split into prediction markets (PM) and everything else."""
    p.card(x0, y0, w, h)
    (tp, pmp), (tt, pmt) = d["phantom_rev"]["prev"], d["phantom_rev"]["today"]
    if tt <= 0:
        p.section_title(x0, y0 + h, 4, "Phantom revenue", "", 58)
        return p.no_data(x0, y0, w, h)
    op, ot = tp - pmp, tt - pmt
    share, share_p = pmt / tt * 100, (pmp / tp * 100 if tp else None)
    note = f"Revenue {chg(pct(tt, tp))} to {usd(tt, 2)}. PM {chg(pct(pmt, pmp))}, everything else {chg(pct(ot, op))}."
    if share_p is not None:
        note += f" PM share {share_p:.1f}% → {share:.1f}%."
    p.section_title(x0, y0 + h, 4, "Phantom revenue", note, 58)

    cx, cy = x0 + 1.4, y0 + h - 2.5
    p.ax.add_patch(Circle((cx, cy), 1.12, fc=PM_C, ec="none", alpha=.05, zorder=1))
    p.ring(cx, cy, 1.05, .3, [(pmt, PM_C), (ot, OTHER_C)])
    p.text(cx, cy + .1, usd(tt, 2), 18, TEXT, "semibold", ha="center", va="center")
    p.chg_pill(cx, cy - .22, pct(tt, tp), 9.5, ha="center")
    for k, (lab, c, v) in enumerate((("PM", PM_C, pmt), ("Everything else", OTHER_C, ot))):
        yy = cy - 1.35 - k * .25
        p.glow_dot(cx - .85, yy, .04, c)
        p.text(cx - .74, yy, lab, 9.5, MUTED, va="center")
        p.text(cx + .95, yy, f"{v / tt * 100:.1f}%", 9.5, TEXT, "semibold", ha="right", va="center")

    rx, y = x0 + 2.85, y0 + h - 1.35
    for label, c, val, prev_val, change in (("Prediction markets", PM_C, pmt, pmp, pct(pmt, pmp)),
                                            ("Everything else", OTHER_C, ot, op, pct(ot, op))):
        p.glow_dot(rx + .05, y, .045, c)
        p.text(rx + .17, y, label, 10.5, MUTED, "medium", va="center")
        vt = p.text(rx, y - .38, usd(val, 2), 18, TEXT, "semibold", va="center")
        p.chg_pill(rx + p.width(vt) + .12, y - .38, change, 9.5)
        p.text(rx, y - .68, f"was {usd(prev_val, 2)}  ({signed_usd(val - prev_val, 2)})", 9.5, FAINT, va="center")
        y -= 1.1
    p.text(rx, y, "PM share of revenue", 10.5, MUTED, "medium", va="center")
    vt = p.text(rx, y - .38, f"{share:.1f}%", 18, TEXT, "semibold", va="center")
    track_w = x0 + w - .3 - rx
    p.rbox(rx, y - 1.02, track_w, .08, .04, fc=TRACK, z=2)
    p.rbox(rx, y - 1.02, max(track_w * share / 100, .08), .08, .04, fc=PM_C, z=3)
    if share_p is not None:
        diff = share - share_p
        p.pill(rx + p.width(vt) + .12, y - .38, ("+" if diff >= 0 else "−") + f"{abs(diff):.1f} pts", diff >= 0, 9.5)
        p.text(rx, y - .68, f"was {share_p:.1f}%", 9.5, FAINT, va="center")
        xp = rx + track_w * share_p / 100
        p.ax.plot([xp, xp], [y - 1.03, y - .89], color=TEXT, lw=1.4, alpha=.8, zorder=4)


def render(d, path=OUT):
    p = Page(load_font())
    p.background()
    header(p, d)
    section_flow(p, d, .3, 5.55, W - .6, 4.45)
    section_share(p, d, .3, .45, 7.75, 4.9)
    section_payers(p, d, 8.25, .45, 4.3, 4.9)
    section_phantom(p, d, 12.75, .45, W - .3 - 12.75, 4.9)
    p.text(.35, .2, f"Volume and transaction bars use log scale; the thin white tick marks {d['prev']}. "
                    f"Changes are vs {d['prev']}. Fee payers can overlap across frontends. PM = prediction markets.",
           9, FAINT, va="center")
    return p.save(path)


# ============================== publish & slack ==============================
def git(args, cwd):
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    if r.returncode != 0:
        msg = (r.stderr or r.stdout).strip()
        if CHART_TOKEN:
            msg = msg.replace(CHART_TOKEN, "***")  # never print the token
        raise RuntimeError(f"git {args[0]} failed: {msg[:300]}")
    return r.stdout.strip()


def publish_to_github(path, d):
    """Put today's PNG on CHART_BRANCH as its only commit (force push, so old images are replaced).
    The link uses the commit id, so every day gets a new link and Slack never shows a cached old image."""
    work = tempfile.mkdtemp()
    shutil.copy(path, work)
    git(["init", "-q"], work)
    git(["symbolic-ref", "HEAD", f"refs/heads/{CHART_BRANCH}"], work)
    git(["config", "user.name", "github-actions[bot]"], work)
    git(["config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], work)
    git(["add", "-A"], work)
    git(["commit", "-q", "-m", f"frontend report for {d['day']}"], work)
    remote = f"https://x-access-token:{CHART_TOKEN}@github.com/{CHART_REPO}.git"
    git(["push", "-q", "--force", remote, f"HEAD:refs/heads/{CHART_BRANCH}"], work)
    sha = git(["rev-parse", "HEAD"], work)
    shutil.rmtree(work, ignore_errors=True)
    print(f"Pushed image to {CHART_REPO}, branch {CHART_BRANCH} ({sha[:7]})")
    return f"https://raw.githubusercontent.com/{CHART_REPO}/{sha}/{path.name}"


def link_works(url):
    """Open the link with no login, the same way Slack will. Waits up to ~1 minute."""
    for _ in range(6):
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 200 and r.headers.get("content-type", "").startswith("image/"):
                return True
        except requests.RequestException:
            pass
        time.sleep(10)
    return False


def post_to_slack(payload):
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to send to Slack: {r.status_code} - {r.text}")


def send_report(d, image_url):
    post_to_slack({
        "text": f"Top 10 frontends on DFlow, {d['day']}",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn",
                                         "text": f"*Top 10 Frontends: Volume, Transactions & Market Share*\n_{d['long_day']}_"}},
            {"type": "image", "image_url": image_url, "alt_text": f"Top 10 frontends report for {d['day']}"},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": "via Dune Analytics  ·  click the image to zoom"}]},
        ],
    })
    print("Report sent to Slack successfully!")


# ============================== main ==============================
def main():
    print(f"Starting frontend report at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    vol_rows = fetch_rows(QUERY_ID, "volume & txns")
    if not vol_rows:
        print("No data fetched from Dune. Exiting.")
        return
    d = build_data(vol_rows,
                   fetch_rows(SHARE_QUERY_ID, "market share"),
                   fetch_rows(FEE_QUERY_ID, "fee payers"),
                   fetch_rows(PHANTOM_REVENUE_QUERY_ID, "Phantom revenue"))

    print("\nTop 10 frontends:")
    for i, (n, (v, _, t, _)) in enumerate(d["frontends"].items(), 1):
        print(f"  {i}. {n}: {usd(v)} | {cnt(t)} txns")

    path = render(d)
    print(f"\nSaved {path}")

    if not SLACK_WEBHOOK_URL:
        print("SLACK_WEBHOOK_FRONTEND is empty, so the image is only saved.")
        return
    if not (CHART_REPO and CHART_TOKEN):
        print("GITHUB_TOKEN / GITHUB_REPOSITORY missing (not on GitHub Actions?), so the image is only saved.")
        return

    try:
        url = publish_to_github(path, d)
        if not link_works(url):
            raise RuntimeError(f"Slack could not open {url}. This happens when {CHART_REPO} is private: make it "
                               "public, or set CHART_REPO to a public repo (plus CHART_REPO_TOKEN).")
    except Exception as e:
        post_to_slack({"text": f"Top 10 frontends report for {d['day']}: the image could not be published "
                               f"({str(e)[:200]}). Check the GitHub Actions log."})
        raise
    send_report(d, url)
    print("\nJob completed!")


if __name__ == "__main__":
    main()
