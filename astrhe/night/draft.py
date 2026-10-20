"""草稿箱提交：事务写库 + 外部动作收尾。

提交点：先在一个数据库事务里写入规划书、段落、待办、关系本、承诺。
事务成功，这次运行就算已提交（committed）。
之后的外部动作（建或删定时任务、改签名或状态）用 asyncio.shield 包住，
不受运行取消的影响；每条记录成功或失败，失败的留给下一晚处理。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from .. import constants as C
from ..timeutil import utc_now
from ..tools.alarm_card import render_card_note, alarm_description
from ..tools.validation import (
    validate_alarm,
    validate_alarm_cancel,
    validate_night_alarm_frequency,
)
from .memory_apply import apply_memory_edit


class CommitError(Exception):
    """提交被拒绝（缺规划书 / 缺审稿等）。"""


class DraftCommitter:
    def __init__(
        self,
        storage: Any,
        context: Any,
        run_id: int,
        plan_date: str,
        tz: Any,
        rate_limiter: Any = None,
        qq_client: Any = None,
        admins=None,
        access: Any = None,
    ):
        self.storage = storage
        self.context = context
        self.run_id = run_id
        self.plan_date = plan_date
        self.tz = tz
        self.rate_limiter = rate_limiter
        self.qq = qq_client
        self.access = access
        self.admins = (
            admins if admins is not None else context.get_config().get("admins_id", [])
        )

    async def validate(self) -> None:
        """提交前校验；不通过抛 CommitError。"""
        run = await self.storage.runs.get_run(self.run_id)
        if run and run["status"] == "committed":
            raise CommitError("今晚已经提交，不能重复提交")
        drafts = await self.storage.runs.get_drafts(self.run_id)
        if not any(d["kind"] == "plan" for d in drafts):
            raise CommitError("草稿箱里没有规划书，不能提交")
        ok_reviews = await self.storage.runs.count_ok_reviews(self.run_id)
        invalid_reviews = await self.storage.runs.count_reviews(self.run_id, "invalid")
        # 审稿人反复给不出有效意见时，主 agent 自查后可以提交，不让整晚卡死
        if ok_reviews < C.REVIEW_MIN_OK and invalid_reviews < C.REVIEW_MAX_INVALID:
            raise CommitError(
                "还没有成功完成过审稿，不能提交；先用 astrhe_request_review"
            )

    async def commit_db(self) -> dict[str, Any]:
        """在一个事务里应用全部数据库草稿，返回统计。"""
        drafts = await self.storage.runs.get_drafts(self.run_id)
        # 先归并，再按依赖顺序执行：规划书先落库，段落只清空一次。
        plans = [d for d in drafts if d["kind"] == "plan"]
        sections = {}
        for d in drafts:
            if d["kind"] == "sections":
                sections.update(json.loads(d["payload_json"]).get("sections", {}))
        ordered = plans[-1:]
        ordered += [
            {
                "kind": "sections",
                "payload_json": json.dumps({"sections": sections}, ensure_ascii=False),
            }
        ]
        for kind in ("todo", "todo_drop", "person", "promise", "memory_edit"):
            ordered.extend(d for d in drafts if d["kind"] == kind)
        stats: dict[str, Any] = {
            "plan": 0,
            "sections": 0,
            "todos": 0,
            "todo_drops": 0,
            "persons": 0,
            "promises": 0,
            "memory_edits": 0,
        }
        db = self.storage.db
        now = utc_now()
        async with db.write_lock:
            try:
                await db.conn.execute("BEGIN")
                run = await self.storage.runs.get_run(self.run_id)
                if run["status"] == "committed":
                    raise CommitError("今晚已经提交")
                for d in ordered:
                    kind = d["kind"]
                    payload = json.loads(d["payload_json"] or "{}")
                    if kind == "plan":
                        await db.conn.execute(
                            "INSERT INTO plans(run_id, plan_date, full_text, reflection,"
                            " day_reminders, created_at) VALUES(?,?,?,?,?,?) "
                            "ON CONFLICT(plan_date) DO UPDATE SET run_id=excluded.run_id,"
                            " full_text=excluded.full_text, reflection=excluded.reflection,"
                            " day_reminders=excluded.day_reminders, updated_at=excluded.created_at,"
                            " deleted_at=NULL, deleted_reason=''",
                            (
                                self.run_id,
                                self.plan_date,
                                payload.get("full_text", ""),
                                payload.get("reflection", ""),
                                payload.get("day_reminders", ""),
                                now,
                            ),
                        )
                        stats["plan"] = 1
                    elif kind == "sections":
                        row = await db.conn.execute(
                            "SELECT id FROM plans WHERE plan_date=?", (self.plan_date,)
                        )
                        plan_row = await row.fetchone()
                        if plan_row:
                            plan_id = plan_row[0]
                            await db.conn.execute(
                                "DELETE FROM plan_sections WHERE plan_id=?", (plan_id,)
                            )
                            for umo, text in (payload.get("sections") or {}).items():
                                await db.conn.execute(
                                    "INSERT INTO plan_sections(plan_id, umo, text)"
                                    " VALUES(?,?,?)",
                                    (plan_id, umo, text),
                                )
                                stats["sections"] += 1
                    elif kind == "todo":
                        await db.conn.execute(
                            "INSERT INTO todos(content, visible_umos_json,"
                            " secret_from_json, due_kind, due_date, show_from, status,"
                            " note, evidence_json, created_run_id, updated_at)"
                            " VALUES(?,?,?,?,?,?, 'open', '', ?, ?, ?)",
                            (
                                payload.get("content", ""),
                                json.dumps(
                                    payload.get("visible_umos") or [],
                                    ensure_ascii=False,
                                ),
                                json.dumps(
                                    payload.get("secret_from") or [], ensure_ascii=False
                                ),
                                payload.get("due_kind", "today"),
                                payload.get("due_date", ""),
                                payload.get("show_from", ""),
                                json.dumps(
                                    payload.get("evidence") or [], ensure_ascii=False
                                ),
                                self.run_id,
                                now,
                            ),
                        )
                        stats["todos"] += 1
                    elif kind == "todo_drop":
                        await db.conn.execute(
                            "UPDATE todos SET status='dropped', note=?, updated_at=?"
                            " WHERE id=? AND deleted_at IS NULL",
                            (
                                f"夜间删除：{payload.get('reason', '')}",
                                now,
                                int(payload.get("todo_id", 0)),
                            ),
                        )
                        stats["todo_drops"] += 1
                    elif kind == "person":
                        await self._apply_person(db, payload, now)
                        stats["persons"] += 1
                    elif kind == "promise":
                        await self._apply_promise(db, payload, now)
                        stats["promises"] += 1
                    elif kind == "memory_edit":
                        await apply_memory_edit(
                            self.storage, payload, self.plan_date, self.run_id, now
                        )
                        stats["memory_edits"] += 1
                await db.conn.execute(
                    "UPDATE drafts SET applied=1, status='applied' WHERE run_id=? AND status='active'",
                    (self.run_id,),
                )
                await db.conn.execute(
                    "UPDATE runs SET status='committed' WHERE id=?", (self.run_id,)
                )
                await db.conn.commit()
            except BaseException:
                await db.conn.rollback()
                raise
        return stats

    async def _apply_person(self, db: Any, payload: dict, now: float) -> None:
        qq = str(payload.get("qq", ""))
        if not qq:
            return
        existing = await self.storage.persons.get_person(qq)
        aliases = payload.get("aliases")
        if existing is None:
            await db.conn.execute(
                "INSERT INTO persons(qq, aliases_json, relation_text, recent_text,"
                " anniversaries_json, updated_at) VALUES(?,?,?,?,?,?)",
                (
                    qq,
                    json.dumps(aliases or [], ensure_ascii=False),
                    payload.get("relation_text", ""),
                    payload.get("recent_text", ""),
                    json.dumps(payload.get("anniversaries") or [], ensure_ascii=False),
                    now,
                ),
            )
        else:
            if aliases is None:
                merged = json.loads(existing["aliases_json"] or "[]")
            else:
                merged = list(
                    dict.fromkeys(
                        json.loads(existing["aliases_json"] or "[]") + aliases
                    )
                )
            await db.conn.execute(
                "UPDATE persons SET aliases_json=?, relation_text=?, recent_text=?,"
                " anniversaries_json=?, updated_at=? WHERE qq=?",
                (
                    json.dumps(merged, ensure_ascii=False),
                    payload.get("relation_text", existing["relation_text"]),
                    payload.get("recent_text", existing["recent_text"]),
                    json.dumps(payload["anniversaries"], ensure_ascii=False)
                    if payload.get("anniversaries") is not None
                    else existing["anniversaries_json"],
                    now,
                    qq,
                ),
            )
        if payload.get("expected_friend"):
            until = now + C.EXPECTED_FRIEND_TTL_SECONDS
            await db.conn.execute(
                "UPDATE persons SET expected_friend_until=?, expected_reason=? WHERE qq=?",
                (until, payload.get("expected_reason", ""), qq),
            )

    async def _apply_promise(self, db: Any, payload: dict, now: float) -> None:
        if payload.get("action") == "add":
            await db.conn.execute(
                "INSERT INTO promises(from_id, to_id, content, due_date, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?)",
                (
                    payload.get("from_id", ""),
                    payload.get("to_id", ""),
                    payload.get("content", ""),
                    payload.get("due_date", ""),
                    now,
                    now,
                ),
            )
        elif payload.get("action") == "update":
            status = payload.get("status", "")
            if status in ("done", "broken", "waived"):
                await db.conn.execute(
                    "UPDATE promises SET status=?, updated_at=?,"
                    " note=CASE WHEN ?='' THEN note ELSE ? END WHERE id=? AND deleted_at IS NULL",
                    (
                        status,
                        now,
                        payload.get("reason", ""),
                        payload.get("reason", ""),
                        int(payload.get("promise_id", 0)),
                    ),
                )

    async def run_external_actions(self) -> list[str]:
        """提交后的外部动作：建闹钟、自我形象。shield 包住，不被取消影响。

        返回每条动作的结果描述（成功或失败原因）。
        """

        async def _run() -> list[str]:
            results: list[str] = []
            drafts = await self.storage.runs.get_drafts(self.run_id, "applied")
            # 先真正取消旧闹钟，再按实际状态检查新闹钟额度。
            for d in sorted(drafts, key=lambda d: d["kind"] != "alarm_cancel"):
                kind = d["kind"]
                payload = json.loads(d["payload_json"] or "{}")
                if kind == "alarm_cancel":
                    results.append(await self._cancel_alarm(payload))
                elif kind == "alarm":
                    results.append(await self._create_alarm(payload))
                elif kind == "appearance":
                    results.append(await self._apply_appearance(payload))
            return results

        return await _run()

    async def _cancel_alarm(self, payload: dict) -> str:
        alarm_id = int(payload["alarm_id"])
        try:
            alarm = await validate_alarm_cancel(
                self.storage,
                alarm_id,
                admins=self.admins,
                evidence_message_id=payload.get("evidence_message_id", ""),
            )
            await self.context.cron_manager.delete_job(alarm["cron_job_id"])
        except Exception as exc:
            return f"取消闹钟 #{alarm_id} 失败，保留原状态：{exc}"
        reason = payload["reason"]
        await self.storage.alarms.set_status(alarm_id, "cancelled", reason)
        await self.storage.alarms.add_event(alarm_id, "cancelled", reason)
        return f"闹钟 #{alarm_id} 已取消：{reason}"

    async def _create_alarm(self, payload: dict) -> str:
        target_umo = payload.get("target_umo", "")
        card = payload.get("card") or {}
        run_at_str = payload.get("run_at", "")
        cron_expression = payload.get("cron_expression", "")
        try:
            checked, warning = await validate_alarm(
                payload, self.qq, self.tz, access=self.access
            )
            target_umo = checked["target_umo"]
            await validate_night_alarm_frequency(
                self.storage, checked, self.tz, self.run_id
            )
            run_at_ts = checked["next_run_at"] if run_at_str else None
        except Exception as exc:
            # 留存失败闹钟，次夜对账能看到，也回给提交工具。
            alarm_id = await self.storage.alarms.add_alarm(
                target_umo=target_umo,
                creator_id=C.NIGHT_ALARM_SENDER_ID,
                origin="night",
                card=card,
                created_run_id=self.run_id,
            )
            await self.storage.alarms.set_status(alarm_id, "failed", str(exc))
            return f"闹钟 #{alarm_id} 失败（{target_umo}）：{exc}"

        note_text = render_card_note(card, target_umo)
        alarm_id = await self.storage.alarms.add_alarm(
            target_umo=target_umo,
            creator_id=C.NIGHT_ALARM_SENDER_ID,
            origin="night",
            run_at=run_at_ts,
            cron_expression=cron_expression,
            card=card,
            note_text=note_text,
            created_run_id=self.run_id,
        )
        try:
            job = await self.context.cron_manager.add_active_job(
                name=f"astrhe闹钟#{alarm_id}",
                cron_expression=cron_expression or None,
                payload={
                    "session": target_umo,
                    "sender_id": C.NIGHT_ALARM_SENDER_ID,
                    "note": note_text,
                    "origin": C.NIGHT_ALARM_ORIGIN,
                    "astrhe_alarm_id": str(alarm_id),
                },
                description=alarm_description(card, alarm_id),
                timezone=getattr(self.tz, "key", None) or str(self.tz),
                run_once=run_at_ts is not None,
                run_at=datetime.fromtimestamp(run_at_ts, self.tz)
                if run_at_ts
                else None,
            )
            await self.storage.alarms.bind_cron_job(alarm_id, job.job_id)
            return f"闹钟 #{alarm_id} 已建：{target_umo}" + (
                f"；{warning}" if warning else ""
            )
        except Exception as exc:  # noqa: BLE001
            await self.storage.alarms.set_status(alarm_id, "failed", str(exc))
            return f"闹钟失败（{target_umo}）：{exc}"

    async def _apply_appearance(self, payload: dict) -> str:
        from ..qq.client import QQClient
        from ..platform import PlatformHelper
        from astrbot.api import logger

        signature = payload.get("signature", "")
        preset = payload.get("status_preset", "")
        qq = QQClient(PlatformHelper(self.context, logger))
        results = []
        deny = ""
        if signature and self.rate_limiter is not None:
            deny = await self.rate_limiter.check("signature", utc_now())
        if signature and deny:
            # 签名被频率拦下时，同一份草稿里的在线状态照常处理
            results.append(f"改签名被拦下：{deny}")
        elif signature:
            try:
                await qq.call("set_self_longnick", longNick=signature)
                if self.rate_limiter is not None:
                    await self.rate_limiter.record("signature", target=signature)
                results.append("签名已更新")
            except Exception as exc:  # noqa: BLE001
                results.append(f"改签名失败：{exc}")
        if preset:
            presets = {
                "在线": (10, 0),
                "离开": (30, 0),
                "忙碌": (50, 0),
                "请勿打扰": (70, 0),
                "Q我吧": (60, 0),
            }
            if preset in presets:
                if self.rate_limiter is not None:
                    deny = await self.rate_limiter.check("status", utc_now())
                    if deny:
                        results.append(f"改状态被拦下：{deny}")
                        return "；".join(results)
                status, ext = presets[preset]
                try:
                    await qq.call(
                        "set_online_status",
                        status=status,
                        ext_status=ext,
                        battery_status=0,
                    )
                    if self.rate_limiter is not None:
                        await self.rate_limiter.record("status")
                    results.append(f"状态已改为{preset}")
                except Exception as exc:  # noqa: BLE001
                    results.append(f"改状态失败：{exc}")
        return "；".join(results) if results else "自我形象：无动作"
