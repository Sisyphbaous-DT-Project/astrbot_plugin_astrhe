"""草稿工具：夜间主 agent 专用（写草稿、请求审稿、提交）。

所有产出先进 drafts 表（草稿箱），只有 astrhe_commit 才会触发真正提交。
这些工具只给夜间主 agent，不注册到白天。
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from .. import constants as C
from ..platform import resolve_session_ref
from ..timeutil import utc_now
from .base import AstrHeFunctionTool
from .validation import (
    validate_alarm,
    validate_alarm_cancel,
    validate_anniversaries,
    validate_night_alarm_frequency,
)
from .secrecy import normalize_qq_ids, normalize_secret_text


class DraftTools:
    """草稿箱工具集工厂。"""

    def __init__(
        self,
        storage: Any,
        run_id: int,
        request_review_cb: Callable[[str], Awaitable[str]],
        commit_cb: Callable[[str], Awaitable[str]],
        *,
        qq_client=None,
        tz_getter=None,
        commit_state=None,
        admins=(),
        access=None,
    ):
        self.storage = storage
        self.run_id = run_id
        self._request_review_cb = request_review_cb
        self._commit_cb = commit_cb
        self._seq = 0
        self.qq = qq_client
        self._tz_getter = tz_getter
        self.commit_state = commit_state if commit_state is not None else {}
        self.admins = admins
        self.access = access

    def guard(self, fn, name):
        async def guarded(event, **kw):
            if name != "astrhe_read_drafts" and await self._committed():
                return "今晚已经提交，不能再改或重复提交"
            try:
                return await fn(event, **kw)
            except (ValueError, TypeError) as exc:
                return f"草稿未保存：{exc}"

        return guarded

    async def _committed(self):
        run = await self.storage.runs.get_run(self.run_id)
        return self.commit_state.get("committed") or (
            run and run["status"] in ("committed", "dry_done")
        )

    def _resolve(self, ref):
        pid = (
            self.qq.platform.default_platform_id() if self.qq is not None else "default"
        )
        return resolve_session_ref(ref, pid)

    async def _add(self, kind: str, payload: dict[str, Any], replace_draft_id=0) -> str:
        if await self._committed():
            return "今晚已经提交，不能再改"
        drafts = await self.storage.runs.get_drafts(self.run_id)
        old = None
        if replace_draft_id:
            old = next(
                (
                    d
                    for d in drafts
                    if d["id"] == int(replace_draft_id) and d["kind"] == kind
                ),
                None,
            )
            if old is None:
                return "替换被拒绝：旧草稿不存在、已撤回，或类型不一致"
        self._seq = max([self._seq] + [d["seq"] for d in drafts]) + 1
        db = self.storage.db
        async with db.write_lock:
            try:
                await db.conn.execute("BEGIN")
                run = await self.storage.runs.get_run(self.run_id)
                if run["status"] in ("committed", "dry_done") or self.commit_state.get(
                    "committed"
                ):
                    await db.conn.rollback()
                    return "今晚已经提交，不能再改"
                if old:
                    await db.conn.execute(
                        "UPDATE drafts SET status='discarded' WHERE id=?", (old["id"],)
                    )
                cur = await db.conn.execute(
                    "INSERT INTO drafts(run_id,kind,payload_json,seq) VALUES(?,?,?,?)",
                    (
                        self.run_id,
                        kind,
                        json.dumps(payload, ensure_ascii=False),
                        self._seq,
                    ),
                )
                await db.conn.commit()
            except BaseException:
                await db.conn.rollback()
                raise
        return f"草稿 #{cur.lastrowid} 已保存（{kind}）"

    async def _discard(self, event, draft_id=0, reason="", **kw):
        if not reason.strip():
            return "撤回草稿必须写理由"
        ok = await self.storage.runs.discard_draft(self.run_id, int(draft_id))
        return (
            f"草稿 #{draft_id} 已撤回：{reason}" if ok else "草稿不存在或已不再 active"
        )

    def build(self) -> list[AstrHeFunctionTool]:
        from .draft_schema import build_draft_tools

        return build_draft_tools(self)

    # ------------------------------------------------------------------

    @staticmethod
    def _json_list(value: Any) -> list:
        if isinstance(value, list):
            return value
        if not value:
            return []
        try:
            parsed = json.loads(str(value))
            return parsed if isinstance(parsed, list) else [parsed]
        except (ValueError, TypeError):
            return [str(value)]

    async def _draft_plan(
        self, event: Any, full_text: str = "", reflection: str = "", **kw
    ) -> str:
        if not full_text.strip():
            return "规划书正文不能为空"
        return await self._add(
            "plan",
            {
                "full_text": normalize_secret_text(full_text),
                "reflection": normalize_secret_text(reflection),
                "day_reminders": normalize_secret_text(
                    str(kw.get("day_reminders", ""))
                ),
            },
            kw.get("replace_draft_id", 0),
        )

    async def _draft_sections(self, event: Any, sections_json: str = "", **kw) -> str:
        try:
            sections = json.loads(sections_json)
        except (ValueError, TypeError):
            return "sections_json 不是合法 JSON"
        if not isinstance(sections, dict):
            return "sections_json 需要是 {会话UMO: 段落文本} 的对象"
        sections = {
            self._resolve(k): normalize_secret_text(v) for k, v in sections.items()
        }
        return await self._add(
            "sections", {"sections": sections}, kw.get("replace_draft_id", 0)
        )

    async def _draft_todo(
        self, event: Any, content: str = "", visible_umos: Any = None, **kw
    ) -> str:
        if not content.strip():
            return "待办内容不能为空"
        umos = self._json_list(visible_umos)
        if not umos:
            return "visible_umos 不能为空：待办要知道贴给哪些会话看"
        payload = {
            "content": content,
            "visible_umos": [self._resolve(u) for u in umos],
            "secret_from": normalize_qq_ids(self._json_list(kw.get("secret_from"))),
            "due_kind": str(kw.get("due_kind", "today") or "today"),
            "due_date": str(kw.get("due_date", "") or ""),
            "show_from": str(kw.get("show_from", "") or ""),
            "evidence": self._json_list(kw.get("evidence")),
        }
        run = await self.storage.runs.get_run(self.run_id)
        if payload["due_kind"] not in ("today", "future", "deadline"):
            return "due_kind 只能为 today / future / deadline"
        payload["due_date"] = payload["due_date"] or run["plan_date"]
        payload["show_from"] = payload["show_from"] or (
            run["plan_date"]
            if payload["due_kind"] == "deadline"
            else payload["due_date"]
        )
        return await self._add("todo", payload, kw.get("replace_draft_id", 0))

    async def _draft_todo_drop(
        self, event: Any, todo_id: int = 0, reason: str = "", **kw
    ) -> str:
        if not reason.strip():
            return "删除待办要写理由"
        return await self._add(
            "todo_drop",
            {"todo_id": int(todo_id), "reason": reason},
            kw.get("replace_draft_id", 0),
        )

    async def _draft_alarm(self, event: Any, target_umo: str = "", **kw) -> str:
        if not target_umo.strip():
            return "target_umo 不能为空"
        for key in ("background", "purpose", "how"):
            if not str(kw.get(key, "") or "").strip():
                return f"闹钟卡片缺少「{key}」"
        payload = {
            "target_umo": target_umo,
            "run_at": str(kw.get("run_at", "") or ""),
            "cron_expression": str(kw.get("cron_expression", "") or ""),
            "card": {
                "background": str(kw.get("background", "")),
                "purpose": str(kw.get("purpose", "")),
                "how": str(kw.get("how", "")),
                "involved": self._json_list(kw.get("involved")),
                "mentions": normalize_qq_ids(self._json_list(kw.get("mentions"))),
                "secret_from": normalize_qq_ids(self._json_list(kw.get("secret_from"))),
                "taboo": str(kw.get("taboo", "") or ""),
                "agreed": bool(kw.get("agreed", False)),
                "agreed_reason": str(kw.get("agreed_reason", "") or ""),
            },
        }
        if self.qq is None or self._tz_getter is None:
            return "闹钟草稿校验不可用：缺少平台或时区"
        payload, warning = await validate_alarm(
            payload, self.qq, self._tz_getter(), access=self.access
        )
        drafts = await self.storage.runs.get_drafts(self.run_id)
        active = [
            json.loads(d["payload_json"])
            for d in drafts
            if d["kind"] == "alarm" and d["id"] != kw.get("replace_draft_id")
        ]
        cancelled_ids = {
            json.loads(d["payload_json"])["alarm_id"]
            for d in drafts
            if d["kind"] == "alarm_cancel"
        }
        await validate_night_alarm_frequency(
            self.storage,
            payload,
            self._tz_getter(),
            self.run_id,
            active,
            excluded_alarm_ids=cancelled_ids,
        )
        result = await self._add("alarm", payload, kw.get("replace_draft_id", 0))
        return result + ("；注意：" + warning if warning else "")

    async def _draft_alarm_cancel(
        self, event: Any, alarm_id: int = 0, reason: str = "", **kw
    ) -> str:
        if not reason.strip():
            return "取消闹钟必须写理由"
        evidence = str(kw.get("evidence_message_id", "") or "").strip().lstrip("#")
        await validate_alarm_cancel(
            self.storage,
            int(alarm_id),
            admins=self.admins,
            evidence_message_id=evidence,
        )
        payload = {"alarm_id": int(alarm_id), "reason": reason.strip()}
        if evidence:
            payload["evidence_message_id"] = evidence
        return await self._add(
            "alarm_cancel",
            payload,
            kw.get("replace_draft_id", 0),
        )

    async def _draft_person(self, event: Any, qq: str = "", **kw) -> str:
        qq = str(qq).strip()
        if not qq:
            return "qq 不能为空"
        payload: dict[str, Any] = {"qq": qq}
        for key in ("relation_text", "recent_text", "expected_reason"):
            if kw.get(key):
                payload[key] = str(kw[key])
        if kw.get("aliases"):
            payload["aliases"] = self._json_list(kw["aliases"])
        raw = kw.get("anniversaries")
        if isinstance(raw, str):
            raw = raw.strip()
            if raw:
                try:
                    raw = json.loads(raw)
                except ValueError as exc:
                    raise ValueError(
                        'anniversaries 需要 JSON 数组，例如 [{"name":"生日","date":"10-11"}]'
                    ) from exc
        if raw is not None and raw != "":
            payload["anniversaries"] = validate_anniversaries(raw)
        if kw.get("expected_friend"):
            active = {
                json.loads(d["payload_json"])["qq"]
                for d in await self.storage.runs.get_drafts(self.run_id)
                if d["kind"] == "person"
                and json.loads(d["payload_json"]).get("expected_friend")
                and d["id"] != kw.get("replace_draft_id")
            }
            if len(active | {qq}) > C.EXPECTED_FRIEND_MAX_PER_NIGHT:
                return "本晚预期联系人最多 3 人"
            payload["expected_friend"] = True
        return await self._add("person", payload, kw.get("replace_draft_id", 0))

    async def _draft_promise(self, event: Any, action: str = "", **kw) -> str:
        if action == "add":
            if not str(kw.get("content", "") or "").strip():
                return "承诺内容不能为空"
            return await self._add(
                "promise",
                {
                    "action": "add",
                    "from_id": str(kw.get("from_id", "") or ""),
                    "to_id": str(kw.get("to_id", "") or ""),
                    "content": str(kw["content"]),
                    "due_date": str(kw.get("due_date", "") or ""),
                },
                kw.get("replace_draft_id", 0),
            )
        if action == "update":
            return await self._add(
                "promise",
                {
                    "action": "update",
                    "promise_id": int(kw.get("promise_id", 0) or 0),
                    "status": str(kw.get("status", "") or ""),
                    "reason": str(kw.get("reason", "") or ""),
                },
                kw.get("replace_draft_id", 0),
            )
        return "action 只能是 add / update"

    async def _draft_appearance(self, event: Any, **kw) -> str:
        signature = str(kw.get("signature", "") or "")
        status_preset = str(kw.get("status_preset", "") or "")
        if not signature and not status_preset:
            return "signature 和 status_preset 至少给一个"
        if signature and self._tz_getter is not None:
            # 草稿时就告诉主 agent 换不了，免得审稿、提交后才被拦下
            from ..qq.rate_limit import RateLimiter

            deny = await RateLimiter(self.storage, self._tz_getter).check(
                "signature", utc_now()
            )
            if deny:
                return f"这次不能换签名：{deny}。不要写签名草稿；在线状态可以单独用 status_preset 改"
        return await self._add(
            "appearance",
            {"signature": signature, "status_preset": status_preset},
            kw.get("replace_draft_id", 0),
        )

    async def _request_review(self, event: Any, focus: str = "", **kw) -> str:
        return await self._request_review_cb(focus)

    async def _commit(self, event: Any, summary: str = "", **kw) -> str:
        return await self._commit_cb(summary)

    async def _read_drafts(self, event: Any, **kw) -> str:
        drafts = await self.storage.runs.get_drafts(self.run_id)
        if not drafts:
            return "草稿箱是空的"
        lines = []
        for d in drafts:
            payload = json.loads(d["payload_json"] or "{}")
            text = json.dumps(payload, ensure_ascii=False)
            lines.append(f"#{d['id']} [{d['kind']}] {text}")
        return "当前草稿箱：\n" + "\n".join(lines)
