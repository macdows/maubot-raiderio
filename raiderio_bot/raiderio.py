from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import deque
from typing import Any

import aiohttp

log = logging.getLogger("maubot.raiderio.api")

API_BASE = "https://raider.io/api/v1"


class RateLimiter:
    """Sliding-window rate limiter: at most `max_requests` per `window_seconds`.

    Raider.IO's documented limit is 200 req/min for unauthenticated clients.
    We default to 180 to leave headroom for clock skew and retries.
    """

    def __init__(self, max_requests: int = 180, window_seconds: float = 60.0) -> None:
        self.max = max_requests
        self.window = window_seconds
        self._lock = asyncio.Lock()
        self._times: deque[float] = deque()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            # Evict timestamps outside the window
            while self._times and now - self._times[0] >= self.window:
                self._times.popleft()
            if len(self._times) >= self.max:
                wait = self.window - (now - self._times[0])
                if wait > 0:
                    await asyncio.sleep(wait)
                now = time.monotonic()
                while self._times and now - self._times[0] >= self.window:
                    self._times.popleft()
            self._times.append(now)

# Realm slugs: raider.io uses lowercase, apostrophes removed, spaces -> hyphens.
_SLUG_STRIP = re.compile(r"[^a-z0-9\- ]+")


def slugify_realm(name: str) -> str:
    """Convert a realm display name to raider.io's slug form.

    "Mal'Ganis" -> "malganis"
    "Altar of Storms" -> "altar-of-storms"
    """
    s = (name or "").strip().lower()
    s = _SLUG_STRIP.sub("", s)  # drop apostrophes, punctuation
    s = re.sub(r"\s+", "-", s)
    return s


# Season URL pattern: https://raider.io/mythic-plus-runs/<season-slug>/<id>-<level>-<dungeon>
_SEASON_URL_RE = re.compile(
    r"raider\.io/mythic-plus-runs/([a-z0-9\-]+)/"
)


def extract_season_from_run_url(url: str | None) -> str | None:
    if not url:
        return None
    m = _SEASON_URL_RE.search(url)
    return m.group(1) if m else None


# 5xx responses from raider.io's edge are common and usually recover within seconds.
_RETRYABLE_5XX = {500, 502, 503, 504}
_MAX_ATTEMPTS = 3
# Exponential backoff for transient 5xx: attempt 0 -> 2s, attempt 1 -> 8s.
_BACKOFF_SECONDS = (2.0, 8.0)


async def _get_json(
    http: aiohttp.ClientSession,
    limiter: RateLimiter,
    path: str,
    params: dict[str, Any],
) -> dict[str, Any] | None:
    """Rate-limited GET returning parsed JSON, or None on any error/non-200.

    Retries transient failures (429, 5xx, network errors) with backoff.
    Only logs at WARNING after retries are exhausted, so occasional edge
    hiccups don't spam the log.
    """
    last_status: int | str | None = None
    last_text: str = ""

    for attempt in range(_MAX_ATTEMPTS):
        await limiter.acquire()
        try:
            async with http.get(f"{API_BASE}{path}", params=params) as resp:
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        delay = float(retry_after) if retry_after else 30.0
                    except ValueError:
                        delay = 30.0
                    last_status = 429
                    if attempt < _MAX_ATTEMPTS - 1:
                        log.info(
                            "raider.io 429 on %s (attempt %d); sleeping %.1fs",
                            path, attempt + 1, delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    break
                if resp.status in _RETRYABLE_5XX:
                    last_status = resp.status
                    if attempt < _MAX_ATTEMPTS - 1:
                        delay = _BACKOFF_SECONDS[attempt]
                        await asyncio.sleep(delay)
                        continue
                    break
                if resp.status != 200:
                    last_status = resp.status
                    last_text = (await resp.text())[:200]
                    break
                return await resp.json()
        except aiohttp.ClientError as e:
            last_status = type(e).__name__
            if attempt < _MAX_ATTEMPTS - 1:
                await asyncio.sleep(_BACKOFF_SECONDS[attempt])
                continue
            break
        except Exception:
            log.exception("raider.io request failed: %s", path)
            return None

    log.warning(
        "raider.io %s failed after %d attempts (last=%s)%s",
        path, _MAX_ATTEMPTS, last_status,
        f": {last_text}" if last_text else "",
    )
    return None


async def get_guild_members(
    http: aiohttp.ClientSession,
    limiter: RateLimiter,
    guild: str,
    realm: str,
    region: str,
) -> list[dict[str, Any]]:
    """Return the guild's member list. Empty list on failure."""
    data = await _get_json(
        http,
        limiter,
        "/guilds/profile",
        {
            "region": region,
            "realm": realm,
            "name": guild,
            "fields": "members",
        },
    )
    if not data:
        return []
    members = data.get("members")
    if not isinstance(members, list):
        return []
    return members


def _extract_current_score(profile: dict[str, Any]) -> int | None:
    """Pull the current-season 'all' score out of a /characters/profile response."""
    seasons = profile.get("mythic_plus_scores_by_season") or []
    if not seasons:
        return None
    scores = seasons[0].get("scores") or {}
    raw = scores.get("all")
    if raw is None:
        return None
    try:
        return int(round(float(raw)))
    except (TypeError, ValueError):
        return None


async def get_character_recent_runs(
    http: aiohttp.ClientSession,
    limiter: RateLimiter,
    name: str,
    realm_slug: str,
    region: str,
) -> tuple[int | None, list[dict[str, Any]]]:
    """Fetch a character's current-season score and recent M+ runs.

    Returns (score_or_None, runs_list). On any error, returns (None, []).
    """
    data = await _get_json(
        http,
        limiter,
        "/characters/profile",
        {
            "region": region,
            "realm": realm_slug,
            "name": name,
            "fields": "mythic_plus_recent_runs,mythic_plus_scores_by_season:current",
        },
    )
    if not data:
        return None, []

    score = _extract_current_score(data)
    runs = data.get("mythic_plus_recent_runs") or []
    if not isinstance(runs, list):
        runs = []
    return score, runs


async def get_character_score(
    http: aiohttp.ClientSession,
    limiter: RateLimiter,
    name: str,
    realm_slug: str,
    region: str,
) -> int | None:
    """Fetch just a character's current-season M+ score. None on any error.

    Lighter-weight than get_character_recent_runs — only requests the score
    field, so we can cheaply score non-guild players seen in run rosters.
    """
    data = await _get_json(
        http,
        limiter,
        "/characters/profile",
        {
            "region": region,
            "realm": realm_slug,
            "name": name,
            "fields": "mythic_plus_scores_by_season:current",
        },
    )
    if not data:
        return None
    return _extract_current_score(data)


async def get_run_details(
    http: aiohttp.ClientSession,
    limiter: RateLimiter,
    season: str,
    run_id: int,
) -> dict[str, Any] | None:
    """Fetch the full roster/modifiers for a specific run."""
    return await _get_json(
        http,
        limiter,
        "/mythic-plus/run-details",
        {"season": season, "id": run_id},
    )
