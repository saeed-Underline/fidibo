#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fidibo Art scraper (NO Selenium)
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
      - Availability rule (per your requirement):
          state=3 => SOLD
          state=4 => LOCKED
          if seat_id not present in states response => AVAILABLE
      - Produces availability + price stats per session

Outputs:
- Prints JSON to stdout
- Saves fidibo_art_shows.json
"""

from __future__ import annotations

import json
import re
import os
from dataclasses import dataclass, asdict, field
from typing import Optional, Any
from urllib.parse import urljoin

import requests
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

# Seat state rules (per your instruction)
STATE_SOLD = 3
STATE_LOCKED = 4


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
# HTTP helpers
# -------------------------
def http_get(session: requests.Session, url: str, *, params: dict | None = None) -> requests.Response:
    r = session.get(url, headers=HEADERS, timeout=30, allow_redirects=True, params=params)
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


def extract_event_uuid_from_html(html: str) -> Optional[str]:
    """
    Best-effort UUID extraction. If your show pages don’t include it,
    you can also hardcode mapping event_id->uuid or discover the API that provides it.
    """
    uuid_re = re.compile(
        r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
        re.IGNORECASE,
    )

    m = uuid_re.search(html)
    if m:
        return m.group(0)

    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        txt = script.string or script.get_text() or ""
        m2 = uuid_re.search(txt)
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
            )
        )
    return out


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
    Your rule:
      state=3 => sold
      state=4 => locked
      missing seat_id => available
    """
    states: dict[int, int] = {}
    page = 1
    limit = 50

    while True:
        url = SEAT_STATES_API.format(session_id=session_id)
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
# Seat processing (your rules)
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
                    idx[sid] = {
                        "seat_id": sid,
                        "display_name": seat.get("display_name"),
                        "zone": z_name,
                        "block": b_name,
                        "row": row_name,
                        "price": seat.get("price"),
                        "currency": seat.get("currency"),
                    }
    return idx


def summarize_session_seats(seat_idx: dict[int, dict[str, Any]], states: dict[int, int]) -> dict[str, Any]:
    """
    Your availability logic:
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

    for sid, info in seat_idx.items():
        st = states.get(sid, None)  # missing => available
        if st is None:
            available += 1
            p = info.get("price")
            if isinstance(p, (int, float)):
                prices.append(p)
            if currency is None and info.get("currency"):
                currency = info.get("currency")
        elif st == STATE_SOLD:
            sold += 1
        elif st == STATE_LOCKED:
            locked += 1
        else:
            other += 1

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
# Main scrape
# -------------------------
def scrape() -> list[ShowInfo]:
    with requests.Session() as s:
        home_html = http_get(s, MAIN_URL).text
        show_urls = get_home_show_urls(home_html)

        shows: list[ShowInfo] = []

        for show_url in show_urls:
            event_id = extract_event_id(show_url)
            if not event_id:
                continue

            try:
                show_html = http_get(s, show_url).text
                title = extract_title_from_html(show_html, fallback=show_url)

                # Sessions
                sessions = fetch_sessions(s, event_id)

                # Remove sold-out sessions (per your request)
                sessions = [sess for sess in sessions if not sess.is_sold_out]

                # Skip shows with no available sessions
                if not sessions:
                    continue

                # Score (best-effort; may be None if UUID isn't present in HTML)
                event_uuid = extract_event_uuid_from_html(show_html)
                score = fetch_score(s, event_uuid) if event_uuid else None

                # For each remaining session, attach seat availability summary
                for sess in sessions:
                    sess.seat_summary = build_session_seat_summary(s, sess.id)

                shows.append(
                    ShowInfo(
                        title=title,
                        url=show_url,
                        event_id=event_id,
                        event_uuid=event_uuid,
                        sessions=sessions,
                        score=score,
                    )
                )

            except Exception as e:
                print(f"[WARN] Failed show_url={show_url} event_id={event_id} err={e}")
                continue

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


def telegram_send(text: str) -> None:
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


def build_telegram_summary(shows: list[ShowInfo]) -> str:
    if not shows:
        return "هیچ سانسِ قابل خریدی پیدا نشد."

    lines = []
    lines.append("🎭 <b>Fidibo Art Summary</b>")
    lines.append(f"✅ Shows with available sessions: <b>{len(shows)}</b>")
    lines.append("")

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
                    seat_txt += f" | 💰 {minp}-{maxp} {cur}".strip()

            lines.append(f"    - {sess.week_day} {sess.day} {sess.month} {sess.time}{seat_txt}")

        lines.append("")

    return "\n".join(lines)

def main():
    shows = scrape()
    # SORT by Bayesian rating (descending)
    shows.sort(key=lambda s: show_bayes_score(s), reverse=True)
    payload = [asdict(x) for x in shows]

    print(json.dumps(payload, ensure_ascii=False, indent=2))

    with open("fidibo_art_shows.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\nSaved fidibo_art_shows.json (shows={len(payload)})")
    # Send Telegram summary
    summary = build_telegram_summary(shows)
    telegram_send_many(summary)
    print("Sent Telegram summary.")


if __name__ == "__main__":
    main()
