from __future__ import annotations

import html
from typing import Any
from urllib.parse import quote

ROLE_EMOJI = {
    "tank": "🛡️",
    "healer": "💚",
    "dps": "⚔️",
}

ROLE_LABEL = {
    "tank": "Tank",
    "healer": "Healer",
    "dps": "DPS",
}

ROLE_ORDER = {"tank": 0, "healer": 1, "dps": 2}


def _normalize(s: str | None) -> str:
    return (s or "").strip().casefold()


def _roster_role(member: dict[str, Any]) -> str:
    """Roster member role: prefer top-level 'role', fall back to spec.role."""
    role = _normalize(member.get("role"))
    if role:
        return role
    char = member.get("character") or {}
    spec = char.get("spec") or {}
    return _normalize(spec.get("role"))


def count_guild_members(run: dict[str, Any], guild_name: str) -> int:
    """Count roster members whose guild matches (case-insensitive).

    In run-details, each roster member has a top-level `guild` object.
    """
    target = _normalize(guild_name)
    if not target:
        return 0
    count = 0
    for member in run.get("roster") or []:
        guild = member.get("guild") or {}
        if _normalize(guild.get("name")) == target:
            count += 1
    return count


def _stars(upgrades: int) -> str:
    if not upgrades or upgrades < 1:
        return ""
    return "⭐" * min(int(upgrades), 3)


def _fmt_ms(ms: int) -> str:
    total_seconds = max(int(ms) // 1000, 0)
    return f"{total_seconds // 60}:{total_seconds % 60:02d}"


def _timer_parenthetical(clear_ms: int, par_ms: int) -> str:
    if par_ms <= 0:
        return ""
    diff = par_ms - clear_ms
    if diff >= 0:
        pct = diff / par_ms * 100
        return f"{pct:.1f}% remaining"
    return f"{_fmt_ms(-diff)} over time"


def _sort_roster(roster: list[dict[str, Any]]) -> list[int]:
    """Return roster indices sorted by role (tank, healer, dps)."""
    indexed = list(enumerate(roster))
    indexed.sort(key=lambda ix: ROLE_ORDER.get(_roster_role(ix[1]), 99))
    return [i for i, _ in indexed]


def _spec_class(char: dict[str, Any]) -> str:
    spec = (char.get("spec") or {}).get("name") or ""
    cls = (char.get("class") or {}).get("name") or ""
    return " ".join(p for p in (spec, cls) if p)


def _score_str(score: int | None) -> str:
    return f"{score} Score" if score is not None else "? Score"


def _build_run_url(run: dict[str, Any], season: str) -> str:
    """Construct the raider.io run URL. Returns '' if we can't build it."""
    run_id = run.get("keystone_run_id")
    level = run.get("mythic_level")
    dungeon = run.get("dungeon") or {}
    slug = dungeon.get("slug") if isinstance(dungeon, dict) else None
    if run_id and level and slug and season:
        return f"https://raider.io/mythic-plus-runs/{season}/{run_id}-{level}-{slug}"
    return ""


def _build_character_url(region: str, realm_slug: str, name: str) -> str:
    """raider.io character profile: /characters/<region>/<realm>/<name>."""
    if not region or not realm_slug or not name:
        return ""
    return (
        f"https://raider.io/characters/"
        f"{region.lower()}/{realm_slug.lower()}/{quote(name)}"
    )


def _build_dungeon_leaderboard_url(season: str, dungeon_slug: str, region: str) -> str:
    """raider.io per-dungeon leaderboard (strict = current season's base pool)."""
    if not season or not dungeon_slug or not region:
        return ""
    return (
        f"https://raider.io/mythic-plus-rankings/"
        f"{season}/{dungeon_slug}/{region.lower()}/leaderboards-strict"
    )


# raider.io CDN groups dungeon art by expansion folder, e.g.
#   https://cdn.raiderio.net/images/dungeons/expansion11/base/<slug>.jpg
# We prefer an explicit expansion_id from the dungeon object when present.
# If the API doesn't surface one, we fall back to this season-prefix map
# (the token between "season-" and the numeric suffix, e.g. "tww" in
# "season-tww-3"). Extend as new expansions release; if a prefix is missing
# and dungeon.expansion_id is absent, the image link is omitted.
_SEASON_PREFIX_TO_EXPANSION: dict[str, int] = {
    "tww": 11,   # The War Within
    "mn": 11,    # Midnight — tentative; update if the CDN moves to expansion12
}


def _expansion_id_for(dungeon: dict[str, Any], season: str) -> int | None:
    exp = dungeon.get("expansion_id") if isinstance(dungeon, dict) else None
    if isinstance(exp, int):
        return exp
    if not season:
        return None
    # Season slugs look like "season-<prefix>-<N>"; pick the prefix token.
    parts = season.split("-")
    if len(parts) >= 3 and parts[0] == "season":
        return _SEASON_PREFIX_TO_EXPANSION.get(parts[1])
    return None


def _build_dungeon_image_url(dungeon: dict[str, Any], season: str) -> str:
    slug = dungeon.get("slug") if isinstance(dungeon, dict) else None
    if not slug:
        return ""
    exp = _expansion_id_for(dungeon, season)
    if exp is None:
        return ""
    return f"https://cdn.raiderio.net/images/dungeons/expansion{exp}/base/{slug}.jpg"


def format_run(
    run: dict[str, Any],
    scores_by_char_key: dict[str, int | None],
    region: str,
    season: str,
    guild_member_count: int,
    dungeon_images: dict[str, str],
) -> tuple[str, str]:
    """Format a run-details response into (plain_body, html_body).

    `scores_by_char_key` maps "{name}-{realm_slug}" (lowercased) to int score,
    populated from recent member polls and on-demand non-guild lookups.
    `guild_member_count` controls the header: <2 guildies gets "Solo Run!",
    otherwise "Guild Run!".
    `dungeon_images` maps dungeon slug → mxc:// URI. When a slug matches, the
    image is embedded inline in the HTML body and the CDN fallback text link
    is suppressed. Missing slugs fall back to the existing CDN link.
    """
    if guild_member_count >= 2:
        title_emoji = "👊"
        title_text = "Guild Run!"
    else:
        title_emoji = "👀"
        title_text = "Solo Run!"
    level = run.get("mythic_level", "?")
    dungeon = run.get("dungeon") or {}
    dungeon_name = (dungeon.get("name") if isinstance(dungeon, dict) else dungeon) or ""

    # run-details uses num_chests / keystone_time_ms; fall back to alt names just in case.
    upgrades = run.get("num_chests")
    if upgrades is None:
        upgrades = run.get("num_keystone_upgrades") or 0
    stars = _stars(int(upgrades or 0))

    clear_ms = int(run.get("clear_time_ms") or 0)
    par_ms = int(
        run.get("keystone_time_ms")
        or run.get("par_time_ms")
        or ((dungeon.get("keystone_timer_ms") if isinstance(dungeon, dict) else 0) or 0)
    )

    try:
        score_int = int(round(float(run.get("score") or 0)))
    except (TypeError, ValueError):
        score_int = 0

    modifiers = run.get("weekly_modifiers") or run.get("affixes") or []
    affix_names = [
        (m.get("name") or "").strip()
        for m in modifiers
        if m.get("name")
    ]
    affixes_str = ", ".join(affix_names)

    roster = run.get("roster") or []
    order = _sort_roster(roster)

    region_up = (region or "").upper()
    run_url = _build_run_url(run, season)

    dungeon_slug = dungeon.get("slug") if isinstance(dungeon, dict) else ""
    dungeon_leaderboard_url = _build_dungeon_leaderboard_url(
        season, dungeon_slug or "", region
    )
    # Prefer a pre-uploaded Matrix mxc:// URI when we have one (embeds inline);
    # otherwise fall back to the raider.io CDN link in the footer.
    dungeon_mxc = dungeon_images.get(dungeon_slug or "")
    dungeon_image_url = (
        "" if dungeon_mxc else _build_dungeon_image_url(
            dungeon if isinstance(dungeon, dict) else {}, season
        )
    )

    # --- Plain body ---
    lines: list[str] = []
    header = f"{title_emoji} {title_text} +{level} {dungeon_name}"
    if stars:
        header += f" {stars}"
    if region_up:
        header += f" ({region_up})"
    lines.append(header)
    lines.append("")

    timer_paren = _timer_parenthetical(clear_ms, par_ms)
    timer_paren_part = f" ({timer_paren})" if timer_paren else ""
    lines.append(
        f"Cleared in {_fmt_ms(clear_ms)} of {_fmt_ms(par_ms)}"
        f"{timer_paren_part} for {score_int} Points"
    )
    lines.append("")

    for idx in order:
        member = roster[idx]
        char = member.get("character") or {}
        name = char.get("name") or "?"
        realm_slug = ""
        realm = char.get("realm") or {}
        if isinstance(realm, dict):
            realm_slug = realm.get("slug") or ""
        key = f"{name.lower()}-{realm_slug.lower()}"
        score = scores_by_char_key.get(key)

        role = _roster_role(member)
        emoji = ROLE_EMOJI.get(role, "❔")
        label = ROLE_LABEL.get(role, role.title() or "?")
        spec_cls = _spec_class(char)
        spec_part = f" ({spec_cls})" if spec_cls else ""
        lines.append(f"{emoji} {name} - {label}{spec_part} - {_score_str(score)}")

    lines.append("")
    footer_bits: list[str] = []
    if run_url:
        footer_bits.append("Group Details")
    if dungeon_name:
        footer_bits.append(dungeon_name)
    if affixes_str:
        footer_bits.append(affixes_str)
    if footer_bits:
        lines.append(" • ".join(footer_bits))

    plain = "\n".join(lines).rstrip()

    # --- HTML body ---
    h_dungeon = html.escape(dungeon_name)
    h_affixes = html.escape(affixes_str)

    parts: list[str] = []
    header_html = f"{title_emoji} <strong>{title_text}</strong> +{level} {h_dungeon}"
    if stars:
        header_html += f" {stars}"
    if region_up:
        header_html += f" ({html.escape(region_up)})"
    parts.append(header_html)
    parts.append("<br><br>")

    timer_paren_html = f" ({html.escape(timer_paren)})" if timer_paren else ""
    parts.append(
        f"Cleared in <strong>{_fmt_ms(clear_ms)}</strong> of "
        f"{_fmt_ms(par_ms)}{timer_paren_html} for <strong>{score_int} Points</strong>"
    )
    parts.append("<br><br>")

    roster_html: list[str] = []
    for idx in order:
        member = roster[idx]
        char = member.get("character") or {}
        name_raw = char.get("name") or "?"
        name = html.escape(name_raw)
        realm_slug = ""
        realm = char.get("realm") or {}
        if isinstance(realm, dict):
            realm_slug = realm.get("slug") or ""
        key = f"{name_raw.lower()}-{realm_slug.lower()}"
        score = scores_by_char_key.get(key)

        role = _roster_role(member)
        emoji = ROLE_EMOJI.get(role, "❔")
        label = ROLE_LABEL.get(role, role.title() or "?")
        spec_cls = html.escape(_spec_class(char))
        spec_part = f" ({spec_cls})" if spec_cls else ""
        score_str = html.escape(_score_str(score))

        char_url = _build_character_url(region, realm_slug, name_raw)
        if char_url:
            name_html = f'<a href="{html.escape(char_url)}"><strong>{name}</strong></a>'
        else:
            name_html = f"<strong>{name}</strong>"

        roster_html.append(
            f"{emoji} {name_html} - {label}{spec_part} - {score_str}"
        )
    parts.append("<br>".join(roster_html))
    parts.append("<br><br>")

    footer_html: list[str] = []
    if run_url:
        footer_html.append(
            f'<a href="{html.escape(run_url)}">Group Details</a>'
        )
    if dungeon_name:
        if dungeon_leaderboard_url:
            footer_html.append(
                f'<a href="{html.escape(dungeon_leaderboard_url)}">{h_dungeon}</a>'
            )
        else:
            footer_html.append(h_dungeon)
    if affixes_str:
        footer_html.append(h_affixes)
    if dungeon_image_url:
        footer_html.append(
            f'<a href="{html.escape(dungeon_image_url)}">Image</a>'
        )
    if footer_html:
        parts.append(" • ".join(footer_html))

    if dungeon_mxc:
        parts.append(
            f'<br><br><img src="{html.escape(dungeon_mxc)}" alt="{h_dungeon}"/>'
        )

    return plain, "".join(parts)
