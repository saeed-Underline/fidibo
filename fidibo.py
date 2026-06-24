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

import json
import re
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from typing import Optional, Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter

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
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

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
#   FIDIBO_FRONT_ROWS  -> highest row number considered "front" (default 3 => rows 1-3)
#   FIDIBO_GROUND_ZONE -> zone name treated as the ground floor / orchestra
FRONT_ROW_MAX = max(1, int(os.getenv("FIDIBO_FRONT_ROWS", "3")))
GROUND_FLOOR_ZONE = os.getenv("FIDIBO_GROUND_ZONE", "همکف")

# Sessions carry a status; only these are actually buyable. Past sessions come
# back as "finished" and must be dropped or they show phantom availability.
# Comma-separated env override (FIDIBO_SELLABLE_STATUSES); empty string = keep all.
_RAW_SELLABLE = os.getenv("FIDIBO_SELLABLE_STATUSES", "live")
SELLABLE_STATUSES = {x.strip().lower() for x in _RAW_SELLABLE.split(",") if x.strip()}


def row_number(row: Any) -> Optional[int]:
    """Parse the leading integer of a row label ('1', '12', 'A1' -> None, 'VIP' -> None)."""
    if row is None:
        return None
    m = re.match(r"^\s*(\d+)\s*$", str(row))
    return int(m.group(1)) if m else None


def is_front_seat(info: dict[str, Any]) -> bool:
    """A seat is 'front' when it's in the ground-floor zone and rows 1..FRONT_ROW_MAX."""
    if info.get("zone") != GROUND_FLOOR_ZONE:
        return False
    n = row_number(info.get("row"))
    return n is not None and 1 <= n <= FRONT_ROW_MAX


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


# -------------------------
# Telegram
# -------------------------
def telegram_send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or "PUT_YOUR" in TELEGRAM_BOT_TOKEN:
        print("[WARN] Telegram token not set, skipping send.")
        return

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(TELEGRAM_API, data=payload, timeout=30)
    if r.status_code != 200:
        print("[WARN] Telegram send failed:", r.status_code, r.text[:300])


def telegram_send_many(text: str, max_len: int = 3500) -> None:
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
            telegram_send("\n".join(buf))
            buf = [line]
            size = len(line) + 1
        else:
            buf.append(line)
            size += add

    if buf:
        telegram_send("\n".join(buf))


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


def build_telegram_summary(shows: list[ShowInfo]) -> str:
    if not shows:
        return "هیچ سانسِ قابل خریدی پیدا نشد."

    lines = []
    lines.append("🎭 <b>Fidibo Art Summary</b>")
    lines.append(f"✅ Shows with available sessions: <b>{len(shows)}</b>")
    lines.append("")

    # Highlight front-row-only sessions up top so the alert is the first thing seen.
    lines.extend(build_front_row_alert(shows))

    for idx, show in enumerate(shows, start=1):
        score_txt = ""
        if show.score and show.score.average is not None:
            raw = show.score.average
            v = show.score.count
            bayes = bayesian_rating(raw, v, prior_mean=3.5, prior_weight=20)
            # show both raw and bayes so you can compare
            score_txt = f" ⭐ raw {raw:.2f}/5 (v={v}) | bayes {bayes:.2f}/5"

        lines.append(f"{idx}. <b>{show.title}</b>{score_txt}")
        lines.append(f"  <a href=\"{show.url}\">Open show</a>")

        # ALL sessions (already filtered to not sold-out)
        for sess in show.sessions:
            seat = sess.seat_summary or {}
            av = seat.get("available_seats")
            minp = seat.get("available_min_price")
            maxp = seat.get("available_max_price")
            cur = seat.get("currency") or ""

            seat_txt = ""
            if av is not None:
                seat_txt = f" | 🪑 {av}"
                if minp is not None and maxp is not None:
                    seat_txt += f" | 💰 {minp}-{maxp} {cur}".rstrip()
            if seat.get("front_rows_only"):
                seat_txt += f" | 🎯 front rows only ({_fmt_front_rows(seat)})"

            lines.append(f"    - {sess.week_day} {sess.day} {sess.month} {sess.time}{seat_txt}")

        lines.append("")

    return "\n".join(lines)


def total_front_available(shows: list[ShowInfo]) -> int:
    """Total available ground-floor front-row seats across all sessions."""
    return sum(
        int((sess.seat_summary or {}).get("available_front_seats") or 0)
        for show in shows
        for sess in show.sessions
    )


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

    # Only notify when at least one ground-floor front-row seat is available.
    front_total = total_front_available(shows)
    if front_total > 0:
        summary = build_telegram_summary(shows)
        telegram_send_many(summary)
        print(f"Sent Telegram summary ({front_total} front-row seat(s) available).")
    else:
        print("No front-row seats available; skipping Telegram send.")


if __name__ == "__main__":
    main()
