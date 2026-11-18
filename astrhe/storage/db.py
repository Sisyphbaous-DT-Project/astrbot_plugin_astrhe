"""AstrHe SQLite 存储：建表、连接与写锁。

全插件共用一个 aiosqlite 连接（WAL 模式），写操作外加一把 asyncio.Lock。
时间一律存 UTC 时间戳（秒）。
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import aiosqlite

from .. import constants as C

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    umo TEXT NOT NULL,
    platform_id TEXT NOT NULL,
    message_type TEXT NOT NULL,
    session_id TEXT NOT NULL,
    platform_message_id TEXT,
    sender_id TEXT NOT NULL DEFAULT '',
    sender_name TEXT NOT NULL DEFAULT '',
    sender_card TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL DEFAULT '',
    ts REAL NOT NULL,
    is_bot INTEGER NOT NULL DEFAULT 0,
    recalled INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'live'
);
-- 部分唯一索引：仅在 platform_message_id 非空时按 (umo, platform_message_id) 去重
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_umo_pmid
    ON messages(umo, platform_message_id)
    WHERE platform_message_id IS NOT NULL AND platform_message_id != '';
CREATE INDEX IF NOT EXISTS idx_messages_umo_ts ON messages(umo, ts);

CREATE TABLE IF NOT EXISTS notices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    umo TEXT NOT NULL,
    kind TEXT NOT NULL,
    actor_id TEXT NOT NULL DEFAULT '',
    target_id TEXT NOT NULL DEFAULT '',
    message_id TEXT NOT NULL DEFAULT '',
    detail_json TEXT NOT NULL DEFAULT '{}',
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notices_umo_ts ON notices(umo, ts);

CREATE TABLE IF NOT EXISTS friend_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL,
    flag TEXT NOT NULL,
    comment TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'normal',
    ts REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_friend_requests_flag ON friend_requests(flag);

CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at REAL NOT NULL,
    ended_at REAL,
    window_start REAL,
    window_end REAL,
    plan_date TEXT NOT NULL DEFAULT '',
    mode TEXT NOT NULL DEFAULT 'auto',
    status TEXT NOT NULL DEFAULT 'running',
    error TEXT NOT NULL DEFAULT '',
    stats_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    umo TEXT NOT NULL,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    content_json TEXT NOT NULL DEFAULT '{}',
    raw_text TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_reports_run ON reports(run_id);

CREATE TABLE IF NOT EXISTS drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    seq INTEGER NOT NULL DEFAULT 0,
    applied INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active'
);
CREATE INDEX IF NOT EXISTS idx_drafts_run ON drafts(run_id);

CREATE TABLE IF NOT EXISTS reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    focus TEXT NOT NULL DEFAULT '',
    result_text TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'ok'
);

CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    plan_date TEXT NOT NULL,
    full_text TEXT NOT NULL DEFAULT '',
    reflection TEXT NOT NULL DEFAULT '',
    day_reminders TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_plans_date ON plans(plan_date);

CREATE TABLE IF NOT EXISTS plan_sections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL,
    umo TEXT NOT NULL,
    text TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_plan_sections_plan ON plan_sections(plan_id);

CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content TEXT NOT NULL,
    visible_umos_json TEXT NOT NULL DEFAULT '[]',
    secret_from_json TEXT NOT NULL DEFAULT '[]',
    due_kind TEXT NOT NULL DEFAULT 'today',
    due_date TEXT NOT NULL DEFAULT '',
    show_from TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open',
    note TEXT NOT NULL DEFAULT '',
    evidence_json TEXT NOT NULL DEFAULT '[]',
    alarm_id INTEGER,
    created_run_id INTEGER,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS alarms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cron_job_id TEXT NOT NULL DEFAULT '',
    target_umo TEXT NOT NULL,
    creator_id TEXT NOT NULL DEFAULT '',
    origin TEXT NOT NULL DEFAULT 'night',
    run_at REAL,
    cron_expression TEXT NOT NULL DEFAULT '',
    card_json TEXT NOT NULL DEFAULT '{}',
    note_text TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'scheduled',
    result_note TEXT NOT NULL DEFAULT '',
    created_run_id INTEGER,
    created_at REAL NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alarms_job ON alarms(cron_job_id)
    WHERE cron_job_id != '';

CREATE TABLE IF NOT EXISTS alarm_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alarm_id INTEGER NOT NULL,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_alarm_events_alarm ON alarm_events(alarm_id, ts);

CREATE TABLE IF NOT EXISTS persons (
    qq TEXT PRIMARY KEY,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    relation_text TEXT NOT NULL DEFAULT '',
    recent_text TEXT NOT NULL DEFAULT '',
    anniversaries_json TEXT NOT NULL DEFAULT '[]',
    expected_friend_until REAL,
    expected_reason TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL,
    evidence TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS promises (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL DEFAULT '',
    to_id TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '[]',
    due_date TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    target TEXT NOT NULL DEFAULT '',
    umo TEXT NOT NULL DEFAULT '',
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_action_log ON action_log(action, target, ts);

CREATE TABLE IF NOT EXISTS memory_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    target_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    before_json TEXT NOT NULL,
    after_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    run_id INTEGER,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_memory_revisions_target
    ON memory_revisions(kind, target_id, ts);

CREATE TABLE IF NOT EXISTS sent_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_umo TEXT NOT NULL,
    target_cid TEXT NOT NULL DEFAULT '',
    origin_cid TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    sent_at REAL NOT NULL,
    exec_id TEXT NOT NULL DEFAULT '',
    alarm_id INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    saved_at REAL
);
CREATE INDEX IF NOT EXISTS idx_sent_receipts_target
    ON sent_receipts(target_umo, status, id);

CREATE TABLE IF NOT EXISTS receipt_resets (
    cid TEXT PRIMARY KEY, cutoff REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS kv_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);
"""


class Database:
    """AstrHe 数据库连接与写锁。"""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def open(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(str(self.db_path))
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        # 大批量清理会把 WAL 撑大；限制检查点后保留的大小，避免长期占用磁盘
        await conn.execute(f"PRAGMA journal_size_limit={C.SQLITE_JOURNAL_SIZE_LIMIT}")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.executescript(SCHEMA)
        await conn.commit()
        self._conn = conn
        await self.ensure_column("drafts", "status", "TEXT NOT NULL DEFAULT 'active'")
        await self.ensure_column("plans", "day_reminders", "TEXT NOT NULL DEFAULT ''")
        for table in ("plans", "todos", "promises"):
            await self.ensure_column(table, "deleted_at", "REAL")
            await self.ensure_column(
                table, "deleted_reason", "TEXT NOT NULL DEFAULT ''"
            )
        await self.ensure_column("plans", "updated_at", "REAL")
        await self.ensure_column("alarms", "finished_at", "REAL")
        await self.ensure_column(
            "alarms", "schedule_revision", "INTEGER NOT NULL DEFAULT 0"
        )
        await self.ensure_column("promises", "created_at", "REAL NOT NULL DEFAULT 0")
        await self.ensure_column("promises", "updated_at", "REAL NOT NULL DEFAULT 0")
        await self.ensure_column("promises", "note", "TEXT NOT NULL DEFAULT ''")
        await self.ensure_column(
            "sent_receipts", "origin_cid", "TEXT NOT NULL DEFAULT ''"
        )
        for column in ("scope_umo", "request_id", "call_id"):
            await self.ensure_column(
                "sent_receipts", column, "TEXT NOT NULL DEFAULT ''"
            )
        await conn.execute(
            "UPDATE drafts SET status='applied' WHERE applied=1 AND status='active'"
        )
        await conn.commit()

    async def ensure_column(self, table: str, column: str, definition: str) -> None:
        """迁移本插件固定表结构；只接受内部常量，不接受用户输入。"""
        rows = await self.conn.execute(f"PRAGMA table_info({table})")
        if column not in {r[1] for r in await rows.fetchall()}:
            await self.conn.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
            )

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("AstrHe 数据库尚未打开")
        return self._conn

    @property
    def write_lock(self) -> asyncio.Lock:
        return self._write_lock

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        """写操作：持写锁执行并提交。"""
        async with self._write_lock:
            cur = await self.conn.execute(sql, params)
            await self.conn.commit()
            return cur

    async def executemany(self, sql: str, seq: list[tuple]) -> None:
        async with self._write_lock:
            await self.conn.executemany(sql, seq)
            await self.conn.commit()

    async def fetchall(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        cur = await self.conn.execute(sql, params)
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def fetchone(self, sql: str, params: tuple = ()) -> dict[str, Any] | None:
        cur = await self.conn.execute(sql, params)
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_state(self, key: str, default: str = "") -> str:
        row = await self.fetchone("SELECT value FROM kv_state WHERE key=?", (key,))
        return row["value"] if row else default

    async def set_state(self, key: str, value: str) -> None:
        await self.execute(
            "INSERT INTO kv_state(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
