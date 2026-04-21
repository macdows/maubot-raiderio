from __future__ import annotations

from mautrix.util.async_db import UpgradeTable, Connection

upgrade_table = UpgradeTable()


@upgrade_table.register(description="Initial revision: posted_runs + meta")
async def upgrade_v1(conn: Connection) -> None:
    await conn.execute(
        """CREATE TABLE posted_runs (
            run_id    BIGINT PRIMARY KEY,
            posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    )
    await conn.execute(
        """CREATE TABLE meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )"""
    )


class PostedRunsStore:
    def __init__(self, db) -> None:
        self.db = db

    async def is_posted(self, run_id: int) -> bool:
        row = await self.db.fetchval(
            "SELECT 1 FROM posted_runs WHERE run_id=$1", run_id
        )
        return row is not None

    async def mark_posted(self, run_id: int) -> None:
        await self.db.execute(
            "INSERT INTO posted_runs (run_id) VALUES ($1) "
            "ON CONFLICT (run_id) DO NOTHING",
            run_id,
        )


class MetaStore:
    def __init__(self, db) -> None:
        self.db = db

    async def get(self, key: str) -> str | None:
        return await self.db.fetchval("SELECT value FROM meta WHERE key=$1", key)

    async def set(self, key: str, value: str) -> None:
        await self.db.execute(
            "INSERT INTO meta (key, value) VALUES ($1, $2) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value",
            key,
            value,
        )
