#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fidibo Art scraper (NO Selenium) — concurrent edition

- Reads the main page (art.fidibo.com) and collects show URLs
- For each show:
  - Extracts event_id from URL (...-20)
  - Fetches sessions from Bilito API
  - Filters OUT sold-out sessions (is_sold_out=True)
  - If no remaining sessions -> skip show
  - Extracts event_uuid from show HTML (best-effort) and fetches rating/score
  - For each remaining session:
      - Fetch seatmap (seat metadata)
      - Fetch seat states (paginated)
      - Availability rule:
          state=3 => SOLD
          state=4 => LOCKED
          if seat_id not present in states response => AVAILABLE
      - Produces availability + price stats per session

Performance notes
-----------------
All network work is I/O bound, so the previous fully-sequential version spent
almost all of its wall-clock time waiting on round-trips. This version:
  * reuses a single pooled, retrying requests.Session (keep-alive + backoff),
  * fans show fetches out across a thread pool,
  * fans per-session seat lookups out across the same pool, and
  * pulls seat-state pages in larger chunks (fewer round-trips).
Output (stdout JSON, fidibo_art_shows.json, Telegram summary) is unchanged.

Tunables via environment:
  FIDIBO_WORKERS       -> thread pool size (default 16)
  FIDIBO_SEAT_LIMIT    -> seat-states page size (default 200)
  FIDIBO_MAX_SHOWS     -> cap number of shows processed (0 = no cap; for testing)
"""

from __future__ import annotations

import html
import json
import re
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional, Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter

try:  # Optional: only needed for the Gemini-researched show reviews
    from google import genai
    from google.genai import types as genai_types, errors as genai_errors
except ImportError:  # scraper still runs without the package / feature
    genai = None  # type: ignore
    genai_types = genai_errors = None  # type: ignore

try:  # urllib3 ships with requests; import location is stable in practice
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover - extremely defensive fallback
    Retry = None  # type: ignore

from bs4 import BeautifulSoup


# -------------------------
# Config
# -------------------------
MAIN_URL = "https://art.fidibo.com/?utm_source=homepage&utm_medium=gif&utm_campaign=theater"
BASE_URL = "https://art.fidibo.com/"

SESSIONS_API = "https://api.fidibo.com/bilito/api/client/v1/events/{event_id}/sessions"
SCORE_API = "https://api.fidibo.com/ratereview2/api/client/v1/opinions/entities/event/{event_uuid}/insight"
SEATMAP_API = "https://api.fidibo.com/bilito/api/client/v1/sessions/{session_id}/seatmap"
SEAT_STATES_API = "https://api.fidibo.com/bilito/api/client/v1/sessions/{session_id}/seats/states"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
# Optional second channel that only receives alerts for favorite shows.
TELEGRAM_FAVORITES_CHAT_ID = os.getenv("TELEGRAM_FAVORITES_CHAT_ID", "")
FAVORITES_FILE = os.getenv("FIDIBO_FAVORITES_FILE", "favorite_shows.txt")

# Optional: enables Gemini-researched public-opinion remarks in the report.
# When unset, the job runs normally without remarks.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
# Diagnosed Jul 2026 (Tiwall repo): on free-tier keys, Search grounding 429s on
# every Gemini 3.x model; 2.5-flash is the only model that still grounds
# successfully (grandfathered for existing users).
GEMINI_MODEL = "gemini-2.5-flash"
INFORMATION_FILE = "information.txt"   # persistent show-feedback bank, committed to the repo
INFO_MAX_AGE_DAYS = 14                 # re-research a show after this many days
NO_FEEDBACK_MARKER = "یافت نشد"        # Gemini's "no reliable feedback" fallback
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
TELEGRAM_PHOTO_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
TELEGRAM_CAPTION_LIMIT = 1024  # Telegram caps photo captions at 1024 chars

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.7,en;q=0.6",
    "Origin": "https://art.fidibo.com",
    "Referer": "https://art.fidibo.com/",
}

# Concurrency / paging tunables
WORKERS = max(1, int(os.getenv("FIDIBO_WORKERS", "8")))
SEAT_PAGE_LIMIT = max(1, int(os.getenv("FIDIBO_SEAT_LIMIT", "200")))
MAX_SHOWS = max(0, int(os.getenv("FIDIBO_MAX_SHOWS", "0")))
REQUEST_TIMEOUT = 30

# Seat state rules
STATE_SOLD = 3
STATE_LOCKED = 4

# "Front rows" alert config
#   FIDIBO_FRONT_ROWS    -> highest row number considered "front" (default 6 => rows 1-6)
#   FIDIBO_GROUND_ZONE   -> zone name treated as the ground floor / orchestra
#   FIDIBO_FRONT_SECTIONS-> comma-separated section letters whose rows count as
#                           front when labels look like 'A3' (default "A"; 'B1'
#                           is the first row of a rear block, not the hall front)
FRONT_ROW_MAX = max(1, int(os.getenv("FIDIBO_FRONT_ROWS", "6")))
GROUND_FLOOR_ZONE = os.getenv("FIDIBO_GROUND_ZONE", "همکف")
FRONT_SECTIONS = {x.strip().upper() for x in os.getenv("FIDIBO_FRONT_SECTIONS", "A").split(",") if x.strip()}

# Sessions carry a status; only these are actually buyable. Past sessions come
# back as "finished" and must be dropped or they show phantom availability.
# Comma-separated env override (FIDIBO_SELLABLE_STATUSES); empty string = keep all.
_RAW_SELLABLE = os.getenv("FIDIBO_SELLABLE_STATUSES", "live")
SELLABLE_STATUSES = {x.strip().lower() for x in _RAW_SELLABLE.split(",") if x.strip()}


_ROW_LABEL_RE = re.compile(r"^\s*([A-Za-z]?)\s*(\d+)\s*$")


def row_parts(row: Any) -> tuple[str, Optional[int]]:
    """Split a row label into (section, number): '12' -> ('', 12), 'A3' -> ('A', 3), 'VIP' -> ('', None)."""
    if row is None:
        return "", None
    m = _ROW_LABEL_RE.match(str(row))
    if not m:
        return "", None
    return m.group(1).upper(), int(m.group(2))


def row_number(row: Any) -> Optional[int]:
    """Numeric part of a row label ('1' -> 1, 'A3' -> 3, 'VIP' -> None)."""
    return row_parts(row)[1]


def is_front_seat(info: dict[str, Any]) -> bool:
    """
    A seat is 'front' when it's in the ground-floor zone and rows 1..FRONT_ROW_MAX.
    Rows may be plain numbers ('3') or section-prefixed ('A3'); prefixed rows
    only count when the section is in FRONT_SECTIONS.
    """
    if info.get("zone") != GROUND_FLOOR_ZONE:
        return False
    section, n = row_parts(info.get("row"))
    if n is None or not (1 <= n <= FRONT_ROW_MAX):
        return False
    return section == "" or section in FRONT_SECTIONS


# -------------------------
# Data models
# -------------------------
@dataclass
class SessionInfo:
    id: int
    week_day: str
    day: int
    month: str
    time: str
    is_sold_out: bool
    status: str = ""
    # session-level availability summary (computed from seatmap+states)
    seat_summary: Optional[dict[str, Any]] = None


@dataclass
class ScoreInfo:
    average: Optional[float]
    count: int
    replies: int
    breakdown: dict[str, int]


@dataclass
class ShowInfo:
    title: str
    url: str
    event_id: int
    event_uuid: Optional[str]
    image_url: Optional[str] = None
    sessions: list[SessionInfo] = field(default_factory=list)
    score: Optional[ScoreInfo] = None


# -------------------------
# HTTP session / helpers
# -------------------------
def build_session() -> requests.Session:
    """
    A single pooled session shared across worker threads.

    requests.Session is safe for concurrent GETs as long as the underlying
    connection pool is sized for the worker count, so we widen the pool and
    attach a retry policy with exponential backoff for transient failures.
    """
    s = requests.Session()
    s.headers.update(HEADERS)

    adapter_kwargs: dict[str, Any] = {
        "pool_connections": WORKERS,
        "pool_maxsize": WORKERS * 2,
    }
    if Retry is not None:
        adapter_kwargs["max_retries"] = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            raise_on_status=False,
        )

    adapter = HTTPAdapter(**adapter_kwargs)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def http_get(session: requests.Session, url: str, *, params: dict | None = None) -> requests.Response:
    r = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True, params=params)
    r.raise_for_status()
    return r


def safe_json(resp: requests.Response, context: str) -> Optional[dict]:
    try:
        return resp.json()
    except Exception as e:
        ct = resp.headers.get("Content-Type")
        print(f"[WARN] JSON decode failed: {context}")
        print(f"       status={resp.status_code} content-type={ct} err={e}")
        print(f"       first200={repr((resp.text or '')[:200])}")
        return None


# -------------------------
# HTML parsing
# -------------------------
def get_home_show_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: set[str] = set()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        # Only real show pages that end with -<digits>
        if (href.startswith("/theater/") or href.startswith("/concert/")) and re.search(r"-(\d+)(?:\?.*)?$", href):
            urls.add(urljoin(BASE_URL, href))

    return sorted(urls)


def extract_event_id(show_url: str) -> Optional[int]:
    m = re.search(r"-(\d+)(?:\?.*)?$", show_url)
    return int(m.group(1)) if m else None


def extract_title_from_html(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()
    return fallback


def extract_image_from_html(html: str) -> Optional[str]:
    """Poster URL from og:image; passed to Telegram as-is (no local download)."""
    soup = BeautifulSoup(html, "html.parser")
    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        return og["content"].strip()
    tw = soup.find("meta", attrs={"name": "twitter:image"})
    if tw and tw.get("content"):
        return tw["content"].strip()
    return None


_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)


def extract_event_uuid_from_html(html: str) -> Optional[str]:
    """
    Best-effort UUID extraction. If show pages don't include it, you can also
    hardcode an event_id->uuid mapping or discover the API that provides it.
    """
    m = _UUID_RE.search(html)
    if m:
        return m.group(0)

    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        txt = script.string or script.get_text() or ""
        m2 = _UUID_RE.search(txt)
        if m2:
            return m2.group(0)

    return None


# -------------------------
# API fetchers
# -------------------------
def fetch_sessions(session: requests.Session, event_id: int) -> list[SessionInfo]:
    url = SESSIONS_API.format(event_id=event_id)
    data = safe_json(http_get(session, url), f"sessions event_id={event_id}")
    if not data:
        return []

    rows = data.get("data", {}).get("result") or []
    out: list[SessionInfo] = []
    for r in rows:
        out.append(
            SessionInfo(
                id=int(r["id"]),
                week_day=str(r.get("week_day") or ""),
                day=int(r.get("day") or 0),
                month=str(r.get("month") or ""),
                time=str(r.get("time") or ""),
                is_sold_out=bool(r.get("is_sold_out")),
                status=str(r.get("status") or ""),
            )
        )
    return out


def is_sellable_session(sess: SessionInfo) -> bool:
    """True if a session is currently buyable (not sold out, not finished/past)."""
    if sess.is_sold_out:
        return False
    if not SELLABLE_STATUSES:
        return True
    if not sess.status:
        return True  # be defensive if the API stops returning status
    return sess.status.lower() in SELLABLE_STATUSES


def fetch_score(session: requests.Session, event_uuid: str) -> Optional[ScoreInfo]:
    url = SCORE_API.format(event_uuid=event_uuid)
    data = safe_json(http_get(session, url), f"score event_uuid={event_uuid}")
    if not data:
        return None

    result = (data.get("data", {}).get("result") or [])
    if not result:
        return None

    r0 = result[0]
    breakdown = {
        "rate_1": int(r0.get("rate_1_count", 0)),
        "rate_2": int(r0.get("rate_2_count", 0)),
        "rate_3": int(r0.get("rate_3_count", 0)),
        "rate_4": int(r0.get("rate_4_count", 0)),
        "rate_5": int(r0.get("rate_5_count", 0)),
    }
    avg = r0.get("rates_average")
    return ScoreInfo(
        average=float(avg) if isinstance(avg, (int, float)) else None,
        count=int(r0.get("rates_count", 0)),
        replies=int(r0.get("replies_count", 0)),
        breakdown=breakdown,
    )


def fetch_seatmap(session: requests.Session, session_id: int) -> Optional[dict]:
    url = SEATMAP_API.format(session_id=session_id)
    return safe_json(http_get(session, url), f"seatmap session_id={session_id}")


def fetch_seat_states(session: requests.Session, session_id: int) -> dict[int, int]:
    """
    Returns {seat_id: state} with pagination.
      state=3 => sold
      state=4 => locked
      missing seat_id => available
    """
    states: dict[int, int] = {}
    page = 1
    limit = SEAT_PAGE_LIMIT
    url = SEAT_STATES_API.format(session_id=session_id)

    while True:
        resp = http_get(session, url, params={"page": page, "limit": limit})
        data = safe_json(resp, f"seat_states session_id={session_id} page={page}")
        if not data:
            break

        result = data.get("data", {}).get("result") or []
        for item in result:
            sid = int(item["seat_id"])
            st = int(item["state"])
            states[sid] = st

        total = int(data.get("data", {}).get("total") or 0)
        if page * limit >= total or not result:
            break
        page += 1

    return states


# -------------------------
# Seat processing
# -------------------------
def seatmap_index(seatmap_json: dict) -> dict[int, dict[str, Any]]:
    """
    Index all seats from seatmap:
    {seat_id: {"seat_id", "display_name", "zone", "block", "row", "price", "currency"}}
    """
    idx: dict[int, dict[str, Any]] = {}
    result = (seatmap_json.get("data", {}).get("result") or [])
    if not result:
        return idx

    layout = result[0]
    for z in (layout.get("zones") or []):
        z_name = z.get("name")
        for b in (z.get("blocks") or []):
            b_name = b.get("name")
            for row in (b.get("rows") or []):
                row_name = row.get("name")
                for seat in (row.get("seats") or []):
                    sid = int(seat["id"])
                    # The row container's name is empty in this API; the real row
                    # label lives on each seat as "row_index".
                    row_label = row_name if row_name not in (None, "") else seat.get("row_index")
                    idx[sid] = {
                        "seat_id": sid,
                        "display_name": seat.get("display_name"),
                        "zone": z_name,
                        "block": b_name,
                        "row": row_label,
                        "price": seat.get("price"),
                        "currency": seat.get("currency"),
                    }
    return idx


def summarize_session_seats(seat_idx: dict[int, dict[str, Any]], states: dict[int, int]) -> dict[str, Any]:
    """
    Availability logic:
      - if seat_id not in states => AVAILABLE
      - if state=3 => SOLD
      - if state=4 => LOCKED
      - otherwise => NOT AVAILABLE (kept as 'other_state')
    Produces summary stats + price stats for AVAILABLE seats only.
    """
    available = 0
    sold = 0
    locked = 0
    other = 0

    prices = []
    currency = None

    front_available = 0           # available seats in ground-floor rows 1..FRONT_ROW_MAX
    available_zones: set[str] = set()
    front_rows_available: set[int] = set()  # which front row numbers actually have seats

    for sid, info in seat_idx.items():
        st = states.get(sid, None)  # missing => available
        if st is None:
            available += 1
            p = info.get("price")
            if isinstance(p, (int, float)):
                prices.append(p)
            if currency is None and info.get("currency"):
                currency = info.get("currency")
            zone = info.get("zone")
            if zone:
                available_zones.add(str(zone))
            if is_front_seat(info):
                front_available += 1
                front_rows_available.add(row_number(info.get("row")))

    # "Front rows only" => there ARE available seats and every one of them is a
    # ground-floor front-row seat (nothing left anywhere else).
    front_rows_only = available > 0 and front_available == available

    return {
        "total_seats_in_map": len(seat_idx),
        "available_seats": available,
        "sold_seats_state_3": sold,
        "locked_seats_state_4": locked,
        "other_state_seats": other,
        "currency": currency,
        "available_min_price": min(prices) if prices else None,
        "available_max_price": max(prices) if prices else None,
        "available_unique_prices": sorted(set(prices)) if prices else [],
        # Front-row alert fields
        "available_front_seats": front_available,
        "front_rows_only": front_rows_only,
        "front_rows_available": sorted(front_rows_available),
        "available_zones": sorted(available_zones),
    }


def build_session_seat_summary(session: requests.Session, session_id: int) -> Optional[dict[str, Any]]:
    seatmap_json = fetch_seatmap(session, session_id)
    if not seatmap_json:
        return None
    seat_idx = seatmap_index(seatmap_json)
    if not seat_idx:
        return None

    states = fetch_seat_states(session, session_id)
    return summarize_session_seats(seat_idx, states)


# -------------------------
# Per-show pipeline (runs in a worker thread)
# -------------------------
def process_show(session: requests.Session, show_url: str) -> Optional[ShowInfo]:
    """
    Fetch one show end-to-end. Seat summaries for the show's sessions are
    themselves fanned out so a show with many sessions doesn't serialize.
    Returns None when the show has no buyable sessions or on hard failure.
    """
    event_id = extract_event_id(show_url)
    if not event_id:
        return None

    try:
        show_html = http_get(session, show_url).text
        title = extract_title_from_html(show_html, fallback=show_url)

        sessions = fetch_sessions(session, event_id)
        # Keep only buyable sessions (drops sold-out and past/"finished" ones)
        sessions = [sess for sess in sessions if is_sellable_session(sess)]
        if not sessions:
            return None

        # Score (best-effort; may be None if UUID isn't present in HTML)
        event_uuid = extract_event_uuid_from_html(show_html)
        score = fetch_score(session, event_uuid) if event_uuid else None

        # Seat availability for each remaining session, concurrently.
        if len(sessions) == 1:
            sessions[0].seat_summary = build_session_seat_summary(session, sessions[0].id)
        else:
            seat_workers = min(len(sessions), WORKERS)
            with ThreadPoolExecutor(max_workers=seat_workers) as pool:
                futs = {
                    pool.submit(build_session_seat_summary, session, sess.id): sess
                    for sess in sessions
                }
                for fut in as_completed(futs):
                    sess = futs[fut]
                    try:
                        sess.seat_summary = fut.result()
                    except Exception as e:
                        print(f"[WARN] seat summary failed session_id={sess.id} err={e}")
                        sess.seat_summary = None

        return ShowInfo(
            title=title,
            url=show_url,
            event_id=event_id,
            event_uuid=event_uuid,
            image_url=extract_image_from_html(show_html),
            sessions=sessions,
            score=score,
        )

    except Exception as e:
        print(f"[WARN] Failed show_url={show_url} event_id={event_id} err={e}")
        return None


# -------------------------
# Main scrape
# -------------------------
def scrape() -> list[ShowInfo]:
    with build_session() as s:
        home_html = http_get(s, MAIN_URL).text
        show_urls = get_home_show_urls(home_html)
        if MAX_SHOWS:
            show_urls = show_urls[:MAX_SHOWS]

        print(f"[INFO] Discovered {len(show_urls)} show URLs; fetching with {WORKERS} workers.")

        shows: list[ShowInfo] = []
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futs = {pool.submit(process_show, s, url): url for url in show_urls}
            for fut in as_completed(futs):
                show = fut.result()
                if show is not None:
                    shows.append(show)

        return shows


def show_bayes_score(show: ShowInfo, *, prior_mean: float = 3.5, prior_weight: int = 20) -> float:
    if not show.score or show.score.average is None:
        return 0.0  # unrated shows go to the bottom
    return bayesian_rating(
        show.score.average,
        show.score.count,
        prior_mean=prior_mean,
        prior_weight=prior_weight,
    )


def bayesian_rating(raw_avg: float | None, votes: int, *, prior_mean: float = 3.5, prior_weight: int = 20) -> float | None:
    """
    Bayesian shrinkage of the raw rating towards a prior_mean.
    - prior_weight controls how strongly we shrink when votes are low.
    """
    if raw_avg is None:
        return None
    v = max(int(votes), 0)
    m = max(int(prior_weight), 1)
    C = float(prior_mean)
    R = float(raw_avg)
    return (v * R + m * C) / (v + m)


def compute_trust(score: Optional[ScoreInfo]) -> Optional[float]:
    """
    Turns rating stats into a 0..1 trust factor for the rate/reviews.
    None = not enough data to judge; new shows are not punished for that.

    Bought votes inflate the average but rarely the rest, so trust leans on:
      - volume: how many people actually rated,
      - replies: written opinions tend to accompany genuine ratings,
      - spread: real audiences disagree; one dominant star bucket looks bought.
    """
    if not score or score.average is None or score.count < 5:
        return None
    volume = min(score.count / 100, 1.0)
    replies = min(score.replies / 10, 1.0)
    total = sum(score.breakdown.values()) or score.count
    top_share = max(score.breakdown.values()) / total
    # top_share <= 0.5 reads as a healthy spread; 1.0 (all votes in one bucket) as fake.
    spread = min((1.0 - top_share) / 0.5, 1.0)
    return 0.4 * volume + 0.3 * replies + 0.3 * spread


# -------------------------
# Show opinion research (Gemini API)
# -------------------------
def load_show_info() -> dict[str, dict]:
    """Loads the persistent show-feedback bank.

    Line format: "event_id | YYYY-MM-DD | remark". Hand-added lines may omit
    the date ("event_id | remark") and are treated as researched today.
    """
    info: dict[str, dict] = {}
    if not os.path.exists(INFORMATION_FILE):
        return info
    with open(INFORMATION_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|", 2)]
            if len(parts) < 2:
                continue
            slug = parts[0]
            date = None
            if len(parts) >= 3:
                try:
                    date = datetime.strptime(parts[1], "%Y-%m-%d").date()
                except ValueError:
                    pass
            if date is not None:
                remark = parts[2]
            else:
                date = datetime.now().date()
                remark = " | ".join(p for p in parts[1:] if p)
            # "no feedback found" lines are misses, not answers — drop them so
            # the show gets re-researched.
            if slug and remark and NO_FEEDBACK_MARKER not in remark:
                info[slug] = {"date": date, "remark": remark}
    return info


def save_show_info(info: dict[str, dict]) -> None:
    lines = ["# Show feedback bank — format: event_id | researched date | remark", ""]
    for slug in sorted(info, key=lambda x: int(x) if x.isdigit() else 0):
        entry = info[slug]
        lines.append(f"{slug} | {entry['date'].strftime('%Y-%m-%d')} | {entry['remark']}")
    with open(INFORMATION_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def get_shows_info_batch(client: "genai.Client", shows: list[dict]) -> dict[str, str]:
    """Asks Gemini (one request, with Google Search grounding) for a detailed
    critical Persian remark on the public reception of each show. Each show
    dict has slug/title/rating/votes. Returns {slug: remark}."""
    listing_lines = []
    for s in shows:
        line = f"{s['slug']} | {s['title']}"
        if s.get("rating"):
            line += f" | امتیاز فیدیبو: {s['rating']:.1f} از 5 ({s.get('votes') or '?'} رای)"
        listing_lines.append(line)
    listing = "\n".join(listing_lines)
    prompt = (
        "نمایش‌های زیر هم‌اکنون در تهران روی صحنه هستند و بلیت آن‌ها در سایت فیدیبو (بیلیتو) فروخته می‌شود. "
        "امتیاز واقعی هر نمایش در سایت فیدیبو هم داده شده است. "
        "برای هر نمایش با جستجو در وب، بازخورد واقعی تماشاگران و منتقدان ایرانی را پیدا کن "
        "(نقدها و نظرات در تیوال، فیدیبو، شبکه‌های اجتماعی و رسانه‌ها).\n"
        "مانند یک منتقد تئاتر بی‌طرف و سخت‌گیر عمل کن:\n"
        "- امتیاز داده‌شده را مبنا قرار بده و در جمله‌ات بیاور؛ با جستجو نقاط قوت و ضعف مشخص را پیدا کن.\n"
        "- نقاط ضعف و نقدهای منفی را به همان اندازه نقاط قوت ذکر کن. "
        "اگر بازخوردها متفاوت یا متوسط است، صادقانه بنویس نظرها دوپهلوست و ضعف اصلی را نام ببر.\n"
        "- از صفت‌های تبلیغاتی و کلی مثل «عالی» و «بی‌نظیر» بدون استناد به نظر واقعی خودداری کن.\n"
        "- فقط اگر هیچ نظر کیفی پیدا نکردی و امتیازی هم داده نشده، بنویس: بازخورد قابل اعتمادی یافت نشد.\n"
        "پاسخ را دقیقاً در همین قالب بده: برای هر نمایش فقط یک خط، به شکل\n"
        "شناسه | نقد فشرده در یک پاراگراف حداکثر ۵ تا ۶ جمله به فارسی؛ "
        "هر جمله باید اطلاعات مهم بدهد: مهم‌ترین نقاط قوت (بازی‌ها، کارگردانی، متن، طراحی صحنه یا موسیقی)، "
        "مهم‌ترین نقاط ضعف مشخص، و جمع‌بندی نظر تماشاگران و منتقدان. از حاشیه‌روی و تکرار خودداری کن\n"
        "داخل نقد از علامت | استفاده نکن.\n"
        "از همان شناسه عددی که داده شده استفاده کن و هیچ متن دیگری ننویس.\n\n"
        f"فهرست نمایش‌ها:\n{listing}"
    )
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
            ),
        )
        text = response.text or ""
    except genai_errors.APIError as e:
        print(f"[WARN] Batch opinion research failed: {e}")
        return {}
    except Exception as e:
        print(f"[WARN] Batch opinion research unexpected error: {e}")
        return {}

    valid_slugs = {s["slug"] for s in shows}
    results: dict[str, str] = {}
    for line in text.splitlines():
        if "|" not in line:
            continue
        slug, _, remark = line.partition("|")
        # Grounded responses often decorate lines with markdown (e.g. "**slug**"
        # or list bullets) — strip those from both ends before matching.
        slug = slug.strip().strip("-*•`_ \t")
        remark = " ".join(remark.replace("*", "").split())  # collapse md/whitespace
        # A remark containing another show's "slug |" means the model ran
        # several answers together on one line — banking it would store many
        # reviews under one show. Treat it as a miss.
        run_on = any(f"{other} |" in remark for other in valid_slugs if other != slug)
        # "no feedback found" is a miss, not an answer — don't bank it for 14
        # days; leave the show absent so the next run retries.
        if slug in valid_slugs and remark and not run_on and NO_FEEDBACK_MARKER not in remark:
            results[slug] = remark
    missed = valid_slugs - set(results)
    if missed:
        print(f"[WARN] Batch opinion research got no answer for: {', '.join(sorted(missed))}")
        # Log the raw reply so format drift is visible in the Actions log.
        print(f"--- raw Gemini reply ({len(text)} chars) ---\n{text[:2000]}\n---")
    return results


def research_show_remarks(shows: list[ShowInfo]) -> dict[str, dict]:
    """Ensures every given show has a fresh remark in the feedback bank
    (researching missing/stale ones via Gemini) and returns the whole bank,
    keyed by str(event_id). Stale entries keep showing until re-researched."""
    show_info = load_show_info()
    today = datetime.now().date()
    missing = [
        s for s in shows
        if str(s.event_id) not in show_info
        or (today - show_info[str(s.event_id)]["date"]).days > INFO_MAX_AGE_DAYS
    ]
    if not missing:
        return show_info
    if not GEMINI_API_KEY:
        print("[INFO] GEMINI_API_KEY not set; skipping show opinion research.")
        return show_info
    if genai is None:
        print("[WARN] google-genai package not installed; skipping show opinion research.")
        return show_info

    # Bounded timeout (ms): a hung API must not stall the job.
    client = genai.Client(
        api_key=GEMINI_API_KEY,
        http_options=genai_types.HttpOptions(timeout=240_000),
    )
    payload = [
        {
            "slug": str(s.event_id),
            "title": s.title,
            "rating": s.score.average if s.score else None,
            "votes": s.score.count if s.score else None,
        }
        for s in missing
    ]
    # Small batches: one call asked to write many detailed paragraphs gets
    # truncated/mangled and most lines fail to parse.
    got_any = False
    for i in range(0, len(payload), 6):
        chunk = payload[i:i + 6]
        print(f"[INFO] Researching {len(chunk)} shows: {', '.join(c['slug'] for c in chunk)}")
        new_remarks = get_shows_info_batch(client, chunk)
        for slug, remark in new_remarks.items():
            show_info[slug] = {"date": today, "remark": remark}
        got_any = got_any or bool(new_remarks)
    if got_any:
        save_show_info(show_info)
    return show_info


# -------------------------
# Telegram
# -------------------------
def telegram_send(text: str, chat_id: Optional[str] = None) -> None:
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or "PUT_YOUR" in TELEGRAM_BOT_TOKEN:
        print("[WARN] Telegram token not set, skipping send.")
        return
    if not chat_id:
        print("[WARN] Telegram chat_id not set, skipping send.")
        return

    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(TELEGRAM_API, data=payload, timeout=30)
    if r.status_code != 200:
        print("[WARN] Telegram send failed:", r.status_code, r.text[:300])


def telegram_send_photo(photo_url: str, caption: str, chat_id: Optional[str] = None) -> bool:
    """
    Send a photo by URL: Telegram's servers fetch it, so nothing is downloaded
    or uploaded locally (URL photos are limited to 5 MB / jpg-png by Telegram).
    Returns False when not sent so the caller can fall back to plain text.
    """
    chat_id = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or "PUT_YOUR" in TELEGRAM_BOT_TOKEN:
        print("[WARN] Telegram token not set, skipping send.")
        return False
    if not chat_id:
        print("[WARN] Telegram chat_id not set, skipping send.")
        return False

    payload = {
        "chat_id": chat_id,
        "photo": photo_url,
        "caption": caption,
        "parse_mode": "HTML",
    }
    r = requests.post(TELEGRAM_PHOTO_API, data=payload, timeout=30)
    if r.status_code != 200:
        print("[WARN] Telegram photo send failed:", r.status_code, r.text[:300])
        return False
    return True


def telegram_send_many(text: str, chat_id: Optional[str] = None, max_len: int = 3500) -> None:
    """
    Telegram limit is ~4096 chars; we chunk smaller for safety.
    Splits on line boundaries.
    """
    lines = text.splitlines()
    buf = []
    size = 0

    for line in lines:
        add = len(line) + 1
        if size + add > max_len and buf:
            telegram_send("\n".join(buf), chat_id)
            buf = [line]
            size = len(line) + 1
        else:
            buf.append(line)
            size += add

    if buf:
        telegram_send("\n".join(buf), chat_id)


def _fmt_front_rows(seat: dict[str, Any]) -> str:
    rows = seat.get("front_rows_available") or []
    return ", ".join(str(r) for r in rows)


def build_front_row_alert(shows: list[ShowInfo]) -> list[str]:
    """Lines highlighting sessions whose only available seats are front rows."""
    hits: list[tuple[ShowInfo, SessionInfo]] = []
    for show in shows:
        for sess in show.sessions:
            if (sess.seat_summary or {}).get("front_rows_only"):
                hits.append((show, sess))

    if not hits:
        return []

    lines = [
        f"🚨 <b>Front-row-only sessions</b> (rows 1–{FRONT_ROW_MAX}, {GROUND_FLOOR_ZONE})",
        f"Only the front rows are left in <b>{len(hits)}</b> session(s):",
        "",
    ]
    for show, sess in hits:
        seat = sess.seat_summary or {}
        av = seat.get("available_seats")
        rows = _fmt_front_rows(seat)
        minp = seat.get("available_min_price")
        maxp = seat.get("available_max_price")
        cur = seat.get("currency") or ""
        price_txt = ""
        if minp is not None and maxp is not None:
            price_txt = f" | 💰 {minp}-{maxp} {cur}".rstrip()
        lines.append(f"• <b>{show.title}</b>")
        lines.append(
            f"    {sess.week_day} {sess.day} {sess.month} {sess.time}"
            f" | 🪑 {av} (rows {rows}){price_txt}"
        )
        lines.append(f"    <a href=\"{show.url}\">Open show</a>")
    lines.append("")
    return lines


def session_front_seats(sess: SessionInfo) -> int:
    return int((sess.seat_summary or {}).get("available_front_seats") or 0)


def relevant_front_shows(shows: list[ShowInfo]) -> list[tuple[ShowInfo, list[SessionInfo]]]:
    # The report is about front rows, so only include shows/sessions that
    # actually have front-row availability. A show with buyable seats only in
    # back rows / balcony (e.g. خون بس) is intentionally left out.
    relevant: list[tuple[ShowInfo, list[SessionInfo]]] = []
    for show in shows:
        front_sessions = [s for s in show.sessions if session_front_seats(s) > 0]
        if front_sessions:
            relevant.append((show, front_sessions))
    return relevant


def build_show_block(
    idx: int,
    show: ShowInfo,
    front_sessions: list[SessionInfo],
    remark: Optional[str] = None,
) -> str:
    # Header: rate | number of sessions matching this repo's criteria
    # (front-row availability) | how much the rate/reviews can be trusted.
    rate_txt = "—"
    if show.score and show.score.average is not None:
        raw = show.score.average
        v = show.score.count
        bayes = bayesian_rating(raw, v, prior_mean=3.5, prior_weight=20)
        rate_txt = f"{raw:.2f}/5 (bayes {bayes:.2f}, v={v})"
    trust = compute_trust(show.score)
    trust_txt = f"{trust:.0%}" if trust is not None else "—"

    lines = [f"{idx}. <b>{show.title}</b>"]
    lines.append(f"⭐ {rate_txt} | 🗓️ {len(front_sessions)} session(s) | 🛡️ {trust_txt}")
    if remark:
        lines.append(f"💬 {html.escape(remark)}")
    lines.append(f"  <a href=\"{show.url}\">Open show</a>")

    # Only sessions that have front-row availability.
    for sess in front_sessions:
        seat = sess.seat_summary or {}
        front = session_front_seats(sess)
        av = seat.get("available_seats")
        rows = _fmt_front_rows(seat)
        minp = seat.get("available_min_price")
        maxp = seat.get("available_max_price")
        cur = seat.get("currency") or ""

        seat_txt = f" | 🎯 {front} front (rows {rows})"
        if av is not None:
            seat_txt += f" | 🪑 {av} total"
        if minp is not None and maxp is not None:
            seat_txt += f" | 💰 {minp}-{maxp} {cur}".rstrip()
        if seat.get("front_rows_only"):
            seat_txt += " | ⚠️ front rows only"

        lines.append(f"    - {sess.week_day} {sess.day} {sess.month} {sess.time}{seat_txt}")

    return "\n".join(lines)


def _split_lines(text: str, limit: int) -> list[str]:
    """Splits text into chunks under the limit, breaking at line boundaries."""
    chunks: list[str] = []
    current = ""
    for line in text.split("\n"):
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) <= limit:
            current = candidate
        else:
            if current:
                chunks.append(current)
            # A single line longer than the limit gets hard-cut
            current = line[:limit]
    if current:
        chunks.append(current)
    return chunks


def send_front_row_report(
    shows: list[ShowInfo],
    chat_id: Optional[str] = None,
    remarks: Optional[dict[str, dict]] = None,
) -> None:
    """
    One header message with the totals and the front-rows-only alert, then one
    message per show topped with its poster (sent by URL, no local download)
    and the details as the caption. Captions are limited to 1024 chars, so any
    overflow (e.g. a long review remark) goes out as follow-up text messages.
    Falls back to plain text when the show has no image or the send fails.
    """
    relevant = relevant_front_shows(shows)
    if not relevant:
        return

    total = total_front_available(shows)
    header = [
        f"🎯 <b>Fidibo — front-row seats available</b> (rows 1–{FRONT_ROW_MAX}, {GROUND_FLOOR_ZONE})",
        f"🪑 <b>{total}</b> front-row seat(s) across <b>{len(relevant)}</b> show(s)",
        "",
    ]
    # Highlight "only front rows left" sessions up top (the urgent subset).
    header.extend(build_front_row_alert(shows))
    telegram_send_many("\n".join(header), chat_id)

    remarks = remarks or {}
    for idx, (show, front_sessions) in enumerate(relevant, start=1):
        entry = remarks.get(str(show.event_id))
        block = build_show_block(idx, show, front_sessions, entry["remark"] if entry else None)
        if show.image_url:
            chunks = _split_lines(block, TELEGRAM_CAPTION_LIMIT)
            if telegram_send_photo(show.image_url, chunks[0], chat_id):
                for chunk in chunks[1:]:
                    telegram_send_many(chunk, chat_id)
                continue
        telegram_send_many(block, chat_id)


def total_front_available(shows: list[ShowInfo]) -> int:
    """Total available ground-floor front-row seats across all sessions."""
    return sum(
        int((sess.seat_summary or {}).get("available_front_seats") or 0)
        for show in shows
        for sess in show.sessions
    )


def load_favorites(path: str = FAVORITES_FILE) -> list[str]:
    """
    Read favorite show identifiers, one per line. Blank lines and lines starting
    with '#' are ignored. Each identifier may be an event id (e.g. '46'), part
    of the show title, or part of the show URL.
    """
    if not os.path.exists(path):
        return []
    favorites: list[str] = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            # Strip inline comments too (titles never contain '#').
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            favorites.append(line)
    return favorites


def show_matches_favorite(show: ShowInfo, favorites: list[str]) -> bool:
    """Match a favorite line against the show's event id, title, or URL."""
    title = (show.title or "").casefold()
    url = (show.url or "").casefold()
    for fav in favorites:
        f = fav.strip()
        if not f:
            continue
        if f.isdigit() and int(f) == show.event_id:
            return True
        fl = f.casefold()
        if fl and (fl in title or fl in url):
            return True
    return False


def favorite_shows(shows: list[ShowInfo], favorites: list[str]) -> list[ShowInfo]:
    return [s for s in shows if show_matches_favorite(s, favorites)]


def _force_utf8_stdio() -> None:
    """
    The JSON we print contains Persian text. On some consoles (notably Windows
    cp1252) that raises UnicodeEncodeError; force UTF-8 so the script runs the
    same everywhere.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8")
            except Exception:
                pass


def main():
    _force_utf8_stdio()
    started = time.perf_counter()
    shows = scrape()
    # SORT by Bayesian rating (descending)
    shows.sort(key=lambda s: show_bayes_score(s), reverse=True)
    payload = [asdict(x) for x in shows]

    print(json.dumps(payload, ensure_ascii=False, indent=2))

    with open("fidibo_art_shows.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    elapsed = time.perf_counter() - started
    print(f"\nSaved fidibo_art_shows.json (shows={len(payload)}) in {elapsed:.1f}s")

    # Opinion research: one batched Gemini call per 6 shows that will actually
    # appear in the report and don't have fresh banked info yet.
    reported = [show for show, _ in relevant_front_shows(shows)]
    remarks = research_show_remarks(reported) if reported else {}

    # Main channel: notify when any ground-floor front-row seat is available.
    front_total = total_front_available(shows)
    if front_total > 0:
        send_front_row_report(shows, remarks=remarks)
        print(f"Sent Telegram summary ({front_total} front-row seat(s) available).")
    else:
        print("No front-row seats available; skipping Telegram send.")

    # Favorites channel: notify only for favorite shows with front-row seats.
    favorites = load_favorites()
    if not favorites:
        print(f"No favorites in {FAVORITES_FILE}; skipping favorites channel.")
    elif not TELEGRAM_FAVORITES_CHAT_ID:
        print("TELEGRAM_FAVORITES_CHAT_ID not set; skipping favorites channel.")
    else:
        favs = favorite_shows(shows, favorites)
        fav_front = total_front_available(favs)
        if fav_front > 0:
            send_front_row_report(favs, TELEGRAM_FAVORITES_CHAT_ID, remarks=remarks)
            matched = ", ".join(s.title for s in favs)
            print(f"Sent favorites summary ({fav_front} front-row seat(s)): {matched}")
        else:
            print(f"Favorites matched {len(favs)} show(s) but none have front-row seats; skipping.")


if __name__ == "__main__":
    main()
