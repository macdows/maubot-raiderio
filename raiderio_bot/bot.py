from __future__ import annotations

import asyncio
import time
from typing import Any, Type

from maubot import Plugin
from mautrix.errors import MForbidden
from mautrix.types import Format, MessageType, RoomID, TextMessageEventContent
from mautrix.util.async_db import UpgradeTable
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

from .formatter import count_guild_members, format_run
from .raiderio import (
    RateLimiter,
    extract_season_from_run_url,
    get_character_recent_runs,
    get_character_score,
    get_guild_members,
    get_run_details,
    slugify_realm,
)
from .store import MetaStore, PostedRunsStore, upgrade_table


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("guild_name")
        helper.copy("guild_realm")
        helper.copy("guild_region")
        helper.copy("min_guild_members")
        helper.copy("min_member_score")
        helper.copy("room_id")
        helper.copy("poll_interval_seconds")
        helper.copy("members_refresh_seconds")
        helper.copy("max_requests_per_minute")


class RaiderIOBot(Plugin):
    runs: PostedRunsStore
    meta: MetaStore
    _task: asyncio.Task | None
    _members: list[dict[str, Any]]
    _members_fetched_at: float
    _limiter: RateLimiter
    _last_season: str | None

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable:
        return upgrade_table

    async def start(self) -> None:
        await super().start()
        self.config.load_and_update()
        self.runs = PostedRunsStore(self.database)
        self.meta = MetaStore(self.database)
        self._members = []
        self._members_fetched_at = 0.0
        self._last_season = None
        rpm = int(self.config["max_requests_per_minute"] or 180)
        self._limiter = RateLimiter(max_requests=max(rpm, 1), window_seconds=60.0)

        # Diagnostic: confirm the bot can actually see the configured room.
        # If `send_message` is returning MForbidden but the room appears to
        # have the bot joined, this will show whether the server-side
        # membership list matches what you see in the client.
        await self._log_room_diagnostics()

        self._task = asyncio.create_task(self._poll_loop())

    async def _log_room_diagnostics(self) -> None:
        # Startup sanity check. Info-level under normal operation; escalates
        # to ERROR only if the configured room isn't in the bot's joined list
        # (which is the failure mode that wastes a full poll cycle before
        # surfacing as MForbidden).
        configured = self.config["room_id"]
        try:
            whoami = await self.client.whoami()
            mxid = getattr(whoami, "user_id", None) or whoami
            self.log.info("Bot authenticated as %s", mxid)
        except Exception:
            self.log.exception("whoami failed")

        try:
            joined = await self.client.get_joined_rooms()
        except Exception:
            self.log.exception("get_joined_rooms failed")
            return

        if any(str(r) == configured for r in joined):
            self.log.info(
                "Configured room %s is in joined list (%d total)",
                configured, len(joined),
            )
        else:
            self.log.error(
                "Configured room %s is NOT in the bot's joined rooms. "
                "Nothing will post until the bot joins that exact room ID. "
                "Joined: %s",
                configured, [str(r) for r in joined],
            )

    async def stop(self) -> None:
        task = getattr(self, "_task", None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self.log.exception("Error awaiting poll task cancellation")
        await super().stop()

    # --- Poll loop ---

    async def _poll_loop(self) -> None:
        try:
            await self._seed_if_fresh()
            while True:
                interval = int(self.config["poll_interval_seconds"] or 300)
                await asyncio.sleep(max(interval, 30))
                try:
                    await self._poll()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.log.exception("Poll cycle failed")
        except asyncio.CancelledError:
            raise

    async def _seed_if_fresh(self) -> None:
        """On first start, mark every currently-visible run as posted to avoid flooding."""
        if await self.meta.get("seeded") == "true":
            return

        await self._refresh_members_if_stale()
        if not self._members:
            self.log.warning("Seed skipped: no members fetched (will retry next start)")
            return

        run_ids, _scores = await self._scan_members_for_runs()
        for rid in run_ids:
            await self.runs.mark_posted(rid)
        await self.meta.set("seeded", "true")
        self.log.warning(
            "Seeded: marked %d existing runs as posted (no messages sent)",
            len(run_ids),
        )

    async def _poll(self) -> None:
        await self._refresh_members_if_stale()
        if not self._members:
            return

        run_ids, scores_by_key = await self._scan_members_for_runs(with_urls=True)
        new_ids = []
        for rid, url in run_ids.items():
            if await self.runs.is_posted(rid):
                continue
            new_ids.append((rid, url))

        if not new_ids:
            self.log.warning(
                "Poll: scanned %d members, %d visible runs, nothing new",
                len(self._members), len(run_ids),
            )
            return

        self.log.warning(
            "Poll: scanned %d members, %d visible runs, %d new to consider",
            len(self._members), len(run_ids), len(new_ids),
        )

        guild_name = self.config["guild_name"]
        region = self.config["guild_region"]
        min_members = int(self.config["min_guild_members"] or 0)
        room_id = RoomID(self.config["room_id"])

        posted = 0
        filtered = 0  # too few guildies
        details_failed = 0
        no_season = 0

        # Fetch run details for each new run. Use a known season from the run URL
        # where possible; fall back to the last one we saw.
        for rid, url in new_ids:
            season = extract_season_from_run_url(url) or self._last_season
            if not season:
                self.log.debug("Skipping run %s: no season available", rid)
                await self.runs.mark_posted(rid)
                no_season += 1
                continue
            self._last_season = season

            details = await get_run_details(self.http, self._limiter, season, rid)
            if not details:
                # Transient failure — leave unposted so we retry next cycle.
                details_failed += 1
                continue

            members = count_guild_members(details, guild_name)
            if members < min_members:
                # Seen & rejected: mark posted so we skip it forever.
                await self.runs.mark_posted(rid)
                filtered += 1
                continue

            # Score any roster player we don't already have. Guild members are
            # pre-populated in scores_by_key from _scan_members_for_runs; this
            # fills in pugs. Cache spans the whole poll cycle so a pug in two
            # runs is only fetched once. Failures leave the key unset, which
            # the formatter renders as "? Score".
            await self._fetch_roster_scores(details, scores_by_key, region)

            dungeon_obj = details.get("dungeon") or {}
            dungeon_name = (
                dungeon_obj.get("name") if isinstance(dungeon_obj, dict) else ""
            ) or "?"
            level = details.get("mythic_level", "?")

            body, formatted = format_run(details, scores_by_key, region, season, members)
            content = TextMessageEventContent(
                msgtype=MessageType.TEXT,
                body=body,
                format=Format.HTML,
                formatted_body=formatted,
            )
            try:
                await self.client.send_message(room_id, content)
            except MForbidden as e:
                # Bot isn't in the room (or lacks send perms). Retrying every
                # other queued run will spam the log with identical tracebacks,
                # so log once without traceback and abort this poll cycle.
                # Runs stay unposted and will flush once access is granted.
                self.log.error(
                    "Cannot post to %s: %s. Invite the bot and it will catch up.",
                    room_id, e,
                )
                return
            except Exception:
                self.log.exception("Failed to send run %s", rid)
                continue
            await self.runs.mark_posted(rid)
            posted += 1
            self.log.warning(
                "Posted run %d: +%s %s (%d guildies)",
                rid, level, dungeon_name, members,
            )

        self.log.warning(
            "Poll complete: posted=%d filtered=%d details_failed=%d no_season=%d",
            posted, filtered, details_failed, no_season,
        )

    # --- Member list management ---

    async def _refresh_members_if_stale(self) -> None:
        ttl = int(self.config["members_refresh_seconds"] or 3600)
        now = time.monotonic()
        if self._members and (now - self._members_fetched_at) < ttl:
            return

        members = await get_guild_members(
            self.http,
            self._limiter,
            guild=self.config["guild_name"],
            realm=self.config["guild_realm"],
            region=self.config["guild_region"],
        )
        if not members:
            self.log.warning("Member refresh returned empty list; keeping cached list")
            return
        self._members = members
        self._members_fetched_at = now
        self.log.info("Refreshed guild member list: %d entries", len(members))

    # --- Scan helpers ---

    async def _fetch_roster_scores(
        self,
        run_details: dict[str, Any],
        scores_by_key: dict[str, int | None],
        region: str,
    ) -> None:
        """Populate scores_by_key for any roster player missing from it.

        Mutates scores_by_key in place. Runs requests concurrently; the
        sliding-window rate limiter serializes them against the global budget.
        """
        tasks = []
        keys: list[str] = []
        for member in run_details.get("roster") or []:
            char = member.get("character") or {}
            name = (char.get("name") or "").strip()
            realm = char.get("realm") or {}
            realm_slug = (realm.get("slug") or "").strip() if isinstance(realm, dict) else ""
            if not name or not realm_slug:
                continue
            key = f"{name.lower()}-{realm_slug.lower()}"
            if key in scores_by_key:
                continue
            keys.append(key)
            tasks.append(
                get_character_score(
                    self.http, self._limiter, name, realm_slug, region,
                )
            )

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for key, result in zip(keys, results):
            if isinstance(result, Exception):
                # Leave unset — formatter renders as "? Score".
                continue
            scores_by_key[key] = result

    async def _scan_members_for_runs(
        self,
        with_urls: bool = False,
    ) -> tuple[dict[int, str | None], dict[str, int | None]]:
        """Concurrently fetch every member's recent runs + scores.

        Returns:
          run_ids: {run_id -> first-seen run url} (url may be None)
          scores_by_key: {"name-realmslug" (lowercased) -> score}
        """
        min_score = int(self.config["min_member_score"] or 0)
        region = self.config["guild_region"]

        tasks = []
        coords: list[tuple[str, str]] = []  # (name, realm_slug)
        for m in self._members:
            char = m.get("character") or {}
            name = (char.get("name") or "").strip()
            realm_display = (char.get("realm") or "").strip()
            if not name or not realm_display:
                continue
            realm_slug = slugify_realm(realm_display)
            coords.append((name, realm_slug))
            tasks.append(
                get_character_recent_runs(
                    self.http, self._limiter, name, realm_slug, region
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        run_ids: dict[int, str | None] = {}
        scores_by_key: dict[str, int | None] = {}

        for (name, realm_slug), result in zip(coords, results):
            if isinstance(result, Exception):
                continue
            score, runs = result
            key = f"{name.lower()}-{realm_slug.lower()}"
            scores_by_key[key] = score

            if score is None or score < min_score:
                continue

            for run in runs:
                rid = run.get("keystone_run_id")
                if not isinstance(rid, int):
                    continue
                if rid in run_ids:
                    continue
                run_ids[rid] = run.get("url") if with_urls else None

        return run_ids, scores_by_key
