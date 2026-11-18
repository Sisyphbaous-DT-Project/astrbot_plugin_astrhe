"""闹钟卡片仓库：alarms 表。"""

from __future__ import annotations

import json
from typing import Any

from .db import Database
from ..timeutil import utc_now

#: 生效中：一次性闹钟等待或执行中；重复闹钟只要平台任务还在且未取消、未失踪，
#: 上一次执行失败也照常算生效（任务仍会按规则继续触发）。
LIVE_ALARM_SQL = (
    "(status IN ('scheduled','fired') OR (cron_expression!='' AND cron_job_id!=''"
    " AND status NOT IN ('cancelled','missing')))"
)


def is_live_alarm(alarm: dict) -> bool:
    return alarm["status"] in ("scheduled", "fired") or bool(
        alarm["cron_expression"]
        and alarm["cron_job_id"]
        and alarm["status"] not in ("cancelled", "missing")
    )


class AlarmsRepo:
    def __init__(self, db: Database):
        self.db = db

    async def add_alarm(
        self,
        *,
        cron_job_id: str = "",
        target_umo: str,
        creator_id: str = "",
        origin: str = "night",
        run_at: float | None = None,
        cron_expression: str = "",
        card: dict[str, Any] | None = None,
        note_text: str = "",
        created_run_id: int | None = None,
    ) -> int:
        cur = await self.db.execute(
            """
            INSERT INTO alarms(cron_job_id, target_umo, creator_id, origin, run_at,
                cron_expression, card_json, note_text, status, created_run_id,
                created_at)
            VALUES(?,?,?,?,?,?,?,?, 'scheduled', ?, ?)
            """,
            (
                cron_job_id,
                target_umo,
                creator_id,
                origin,
                run_at,
                cron_expression,
                json.dumps(card or {}, ensure_ascii=False),
                note_text,
                created_run_id,
                utc_now(),
            ),
        )
        return int(cur.lastrowid)

    async def bind_cron_job(self, alarm_id: int, cron_job_id: str) -> None:
        await self.db.execute(
            "UPDATE alarms SET cron_job_id=? WHERE id=?", (cron_job_id, alarm_id)
        )

    async def claim_dispatch(self, info: dict) -> bool:
        """一次性任务以平台绑定认领，重复任务的每次触发单独入账。"""
        revision = info.get("revision", 0)
        detail = json.dumps(
            {
                "exec": info["exec_no"],
                "job": info["job_id"],
                "at": info["dispatched_at"],
                "revision": revision,
            }
        )
        cur = await self.db.execute(
            "INSERT INTO alarm_events(alarm_id,ts,kind,detail) SELECT id,?,'dispatched',?"
            " FROM alarms WHERE id=? AND cron_job_id=? AND schedule_revision=? AND status NOT IN ('cancelled','missing')"
            " AND (NOT ? OR (status='scheduled' AND NOT EXISTS (SELECT 1 FROM alarm_events"
            " WHERE alarm_id=alarms.id AND kind='dispatched' AND json_valid(detail)"
            " AND json_extract(detail,'$.job')=? AND COALESCE(json_extract(detail,'$.revision'),0)=alarms.schedule_revision)))",
            (
                info["dispatched_at"],
                detail,
                info["alarm_id"],
                info["job_id"],
                revision,
                info["run_once"],
                info["job_id"],
            ),
        )
        return cur.rowcount == 1

    async def move_platform_schedule(self, alarm_id, job_id, at, expression):
        await self.db.execute(
            "UPDATE alarms SET run_at=?, cron_expression=?, schedule_revision=schedule_revision+1,"
            " status='scheduled', result_note='', finished_at=NULL WHERE id=? AND cron_job_id=?"
            " AND status NOT IN ('cancelled','missing')",
            (at, expression, alarm_id, job_id),
        )

    async def cancel_platform_schedule(self, alarm_id, job_id):
        await self.db.execute(
            "UPDATE alarms SET status='cancelled', finished_at=?, result_note='平台任务已删除'"
            " WHERE id=? AND cron_job_id=? AND status NOT IN ('cancelled','missing')"
            " AND (cron_expression!='' OR status IN ('scheduled','fired'))",
            (utc_now(), alarm_id, job_id),
        )

    async def record_execution(
        self,
        alarm_id: int,
        job: str,
        execution: str,
        kind: str,
        status: str,
        note: str = "",
    ) -> None:
        """流水与状态一次提交。迟到结果可留流水，不能复活取消的安排或覆盖下一轮。"""
        terminal = kind in ("done", "interrupted")
        detail = json.dumps({"exec": execution, "note": note}, ensure_ascii=False)
        async with self.db.write_lock:
            conn = self.db.conn
            try:
                cur = await conn.execute(
                    "INSERT INTO alarm_events(alarm_id,ts,kind,detail) SELECT id,?,?,? FROM alarms"
                    " WHERE id=? AND cron_job_id=? AND NOT EXISTS (SELECT 1 FROM alarm_events"
                    " WHERE alarm_id=? AND (kind IN ('done','interrupted') OR kind=?)"
                    " AND json_valid(detail) AND json_extract(detail,'$.exec')=?)",
                    (utc_now(), kind, detail, alarm_id, job, alarm_id, kind, execution),
                )
                if cur.rowcount:
                    await conn.execute(
                        "UPDATE alarms SET status=?,result_note=?,finished_at=CASE WHEN ? AND cron_expression=''"
                        " THEN ? ELSE NULL END WHERE id=? AND cron_job_id=? AND status NOT IN ('cancelled','missing')"
                        " AND schedule_revision=COALESCE((SELECT json_extract(detail,'$.revision') FROM alarm_events"
                        " WHERE alarm_id=alarms.id AND kind='dispatched' AND json_valid(detail) AND json_extract(detail,'$.exec')=?),0)"
                        " AND COALESCE((SELECT json_extract(detail,'$.exec') FROM alarm_events WHERE alarm_id=?"
                        " AND kind='dispatched' AND json_valid(detail) AND json_extract(detail,'$.job')=?"
                        " ORDER BY id DESC LIMIT 1),?)=?",
                        (
                            status,
                            note if status == "failed" else "",
                            terminal,
                            utc_now(),
                            alarm_id,
                            job,
                            execution,
                            alarm_id,
                            job,
                            execution,
                            execution,
                        ),
                    )
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    async def set_status(
        self, alarm_id: int, status: str, result_note: str | None = None
    ) -> None:
        # 进入终态时记下结束时间，供 30 天清理判断；重复闹钟只有取消、失踪，
        # 或者平台任务根本没建成时才算结束。
        await self.db.execute(
            "UPDATE alarms SET status=?, result_note=COALESCE(?, result_note),"
            " finished_at=CASE WHEN ?='cancelled'"
            " OR (cron_expression='' AND ? IN ('done','failed','missing'))"
            " OR (cron_expression!='' AND (?='missing' OR (?='failed' AND cron_job_id='')))"
            " THEN COALESCE(finished_at, ?) ELSE NULL END WHERE id=?",
            (status, result_note, status, status, status, status, utc_now(), alarm_id),
        )

    async def set_status_by_job(
        self, cron_job_id: str, status: str, result_note: str | None = None
    ) -> None:
        alarm = await self.get_by_job(cron_job_id)
        if alarm:
            await self.set_status(alarm["id"], status, result_note)

    async def get_alarm(self, alarm_id: int) -> dict[str, Any] | None:
        return await self.db.fetchone("SELECT * FROM alarms WHERE id=?", (alarm_id,))

    async def get_by_job(self, cron_job_id: str) -> dict[str, Any] | None:
        return await self.db.fetchone(
            "SELECT * FROM alarms WHERE cron_job_id=?", (cron_job_id,)
        )

    async def get_by_alarm_id_str(self, astrhe_alarm_id: str) -> dict[str, Any] | None:
        """cron payload 里的 astrhe_alarm_id 即 alarms.id 的字符串形式。"""
        try:
            alarm_id = int(astrhe_alarm_id)
        except (TypeError, ValueError):
            return None
        return await self.get_alarm(alarm_id)

    async def active_alarms(self) -> list[dict[str, Any]]:
        """仍在等待执行、执行中或还会再次触发的闹钟（对账、注入与保密提醒用）。

        与 live_alarms 同一套生效口径：重复闹钟上一次执行失败也算生效
        （平台任务仍会按规则继续触发），后续保密提醒不丢。
        """
        return await self.db.fetchall(
            f"SELECT * FROM alarms WHERE {LIVE_ALARM_SQL} ORDER BY id"
        )

    async def list_alarms(self, limit: int = 200) -> list[dict[str, Any]]:
        return await self.db.fetchall(
            "SELECT * FROM alarms ORDER BY id DESC LIMIT ?", (limit,)
        )

    async def live_alarms(self) -> list[dict[str, Any]]:
        """夜间展示与撤销用；重复任务上一次失败不代表停止重复。"""
        return await self.db.fetchall(
            f"SELECT * FROM alarms WHERE {LIVE_ALARM_SQL} ORDER BY id"
        )

    async def finished_alarms(self, limit: int = -1, offset: int = 0) -> list[dict]:
        return await self.db.fetchall(
            f"SELECT * FROM alarms WHERE NOT {LIVE_ALARM_SQL}"
            " ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )

    async def finished_since(self, since_ts: float) -> list[dict]:
        """在 since_ts 之后结束的闹钟（夜间材料只展示最近一段）。"""
        return await self.db.fetchall(
            f"SELECT * FROM alarms WHERE NOT {LIVE_ALARM_SQL}"
            " AND COALESCE(finished_at, run_at, created_at)>=? ORDER BY id DESC",
            (since_ts,),
        )

    async def count_alarms(self, scope: str) -> int:
        where = {
            "active": f"WHERE {LIVE_ALARM_SQL}",
            "finished": f"WHERE NOT {LIVE_ALARM_SQL}",
            "all": "",
        }[scope]
        row = await self.db.fetchone(f"SELECT COUNT(*) AS n FROM alarms {where}")
        return row["n"]

    async def count_created_since(
        self,
        since_ts: float,
        *,
        origin: str = "",
        target_umo: str = "",
        creator_id: str = "",
    ) -> int:
        """频率上限统计：since_ts 之后创建且未取消的闹钟数量。"""
        sql = (
            "SELECT COUNT(*) AS c FROM alarms"
            " WHERE created_at>=? AND status NOT IN ('cancelled')"
        )
        params: list[Any] = [since_ts]
        if origin:
            sql += " AND origin=?"
            params.append(origin)
        if target_umo:
            sql += " AND target_umo=?"
            params.append(target_umo)
        if creator_id:
            sql += " AND creator_id=?"
            params.append(creator_id)
        row = await self.db.fetchone(sql, tuple(params))
        return int(row["c"]) if row else 0

    async def add_event(
        self, alarm_id: int, kind: str, detail: str = "", ts=None
    ) -> None:
        await self.db.execute(
            "INSERT INTO alarm_events(alarm_id,ts,kind,detail) VALUES(?,?,?,?)",
            (alarm_id, utc_now() if ts is None else ts, kind, detail),
        )

    async def events(
        self, alarm_id: int, start: float = 0, end: float = 1e20
    ) -> list[dict]:
        return await self.db.fetchall(
            "SELECT * FROM alarm_events WHERE alarm_id=? AND ts>=? AND ts<=? ORDER BY ts,id",
            (alarm_id, start, end),
        )
