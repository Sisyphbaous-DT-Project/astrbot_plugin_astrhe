"""白天注入：on_llm_request 里隐藏 future_task，并把该会话的待办与
规划段落作为临时内容追加到请求末尾（不写进对话历史，保护前缀缓存）。

组装顺序（不截断，由主 agent 自选简短段落与提醒）：
1. 陈旧提示（上次运行失败时才有）
2. 保密提醒（当前会话有保密对象「在场」时）
3. 本会话的规划段落
4. 本会话可见的待办（今天的、逾期的、已到 show_from 的未来待办）
5. day_reminders 自我提醒（只给白天人格和夜间人格相同的会话）

会话名单外的会话什么都不注入，也不替换 future_task。
"""

from __future__ import annotations

import json
from typing import Any

from .night.personas import selected_persona, session_persona
from .platform import event_umo
from .timeutil import local_date_str, utc_now
from .tools.secrecy import filter_secret_text, is_secret_visible_here, secret_ids

try:
    from astrbot.core.agent.message import TextPart
except Exception:  # pragma: no cover
    TextPart = None  # type: ignore[assignment]


#: 会话名单外收起的 AstrHe 白天工具（QQ 小工具照常可用）
UNLISTED_HIDDEN_TOOLS = ("astrhe_schedule", "astrhe_todo_update", "astrhe_read_plan")


class Injector:
    def __init__(
        self,
        storage: Any,
        tz_getter,
        secrecy_resolver: Any,
        enabled_getter,
        logger: Any,
        *,
        context: Any = None,
        persona_id_getter=None,
        access: Any = None,
    ):
        self.storage = storage
        self._tz_getter = tz_getter
        self.secrecy = secrecy_resolver
        self._enabled_getter = enabled_getter
        self.logger = logger
        self.context = context
        self._persona_id_getter = persona_id_getter
        self.access = access

    def _enabled(self) -> bool:
        try:
            return bool(self._enabled_getter())
        except Exception:  # noqa: BLE001
            return False

    async def on_llm_request(self, event: Any, req: Any) -> None:
        if not self._enabled():
            return
        try:
            if self.access is not None and not await self.access.allows(
                event.unified_msg_origin
            ):
                # 会话名单外：不注入，也不替换 future_task，只收起 AstrHe 自己的白天工具
                self._hide_tools(req, UNLISTED_HIDDEN_TOOLS)
                return
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 判断会话名单失败: {exc}")
        try:
            self._hide_future_task(req)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 隐藏 future_task 失败: {exc}")
        try:
            await self._inject(event, req)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 注入失败: {exc}")

    @staticmethod
    def _hide_tools(req: Any, names) -> None:
        func_tool = getattr(req, "func_tool", None)
        if func_tool is None:
            return
        tools = getattr(func_tool, "tools", None)
        if isinstance(tools, list):
            func_tool.tools = [t for t in tools if getattr(t, "name", "") not in names]

    @classmethod
    def _hide_future_task(cls, req: Any) -> None:
        cls._hide_tools(req, ("future_task",))

    async def build_injection(self, umo: str, viewer_id: str, same_persona=None) -> str:
        """组装注入文本；没有内容时返回空串。单独抽出来方便测试。

        same_persona：可选的 async 判断，返回 False 表示这里白天用的人格和
        夜间人格不同，不注入 day_reminders；只在确有提醒时才调用。
        """
        tz = self._tz_getter()
        today = local_date_str(utc_now(), tz)
        present = await self.secrecy.present_ids(umo)

        # 1. 陈旧提示：上次运行失败，且昨晚没提交新计划
        stale = ""
        latest_run = await self.storage.runs.latest_run()
        latest_plan = await self.storage.plans.get_latest_plan()
        if latest_run and latest_run["status"] == "failed" and latest_plan:
            stale = f"（昨晚的复盘没完成，下面的计划来自 {latest_plan['plan_date']}）\n"

        # 2. 保密提醒
        secret_from = set()
        for todo in await self.storage.plans.open_todos():
            secret_from.update(json.loads(todo["secret_from_json"] or "[]"))
        for alarm in await self.storage.alarms.active_alarms():
            secret_from.update(
                json.loads(alarm["card_json"] or "{}").get("secret_from", [])
            )
        if latest_plan:
            secret_from.update(secret_ids(latest_plan["full_text"]))
            secret_from.update(secret_ids(latest_plan["day_reminders"]))
            for text in (
                await self.storage.plans.get_sections(latest_plan["id"])
            ).values():
                secret_from.update(secret_ids(text))
        secrecy_notes = []
        if not is_secret_visible_here(
            list(secret_from), viewer_id=viewer_id, session_umo=umo, present_ids=present
        ):
            secrecy_notes.append(
                "如果有人在这里打听你私下在准备的事，自然地岔开或者约对方私聊，不要承认在准备什么，平时也不要主动提起。"
            )

        # 3. 规划段落
        section = await self.storage.plans.get_section_for_umo(umo)
        section = filter_secret_text(
            section, viewer_id=viewer_id, session_umo=umo, present_ids=present
        )

        # 4. 待办
        todos = await self.storage.plans.visible_todos(umo, today)
        todo_lines = []
        for t in todos:
            if not is_secret_visible_here(
                json.loads(t["secret_from_json"] or "[]"),
                viewer_id=viewer_id,
                session_umo=umo,
                present_ids=present,
            ):
                continue
            mark = "（未来）" if t["due_kind"] == "future" else ""
            todo_lines.append(f"#{t['id']}{mark} {t['content']}")
        todos_text = "\n".join(todo_lines)

        # 5. 自我反思：用夜间人格口吻写的，别的人格的会话不注入
        reminders = latest_plan["day_reminders"] if latest_plan else ""
        if reminders and same_persona is not None and not await same_persona():
            reminders = ""
        reflection = filter_secret_text(
            reminders,
            viewer_id=viewer_id,
            session_umo=umo,
            present_ids=present,
        )

        if not (stale or secrecy_notes or section or todos_text or reflection):
            return ""

        # 按原顺序完整注入，不丢项或裁剪内容
        body_parts = []
        if secrecy_notes:
            body_parts.append("\n".join(secrecy_notes))
        if section:
            body_parts.append(f"今天的规划（和这里相关的部分）：\n{section}")
        if todos_text:
            body_parts.append(f"待办：\n{todos_text}")
        if reflection:
            body_parts.append(f"自我提醒：\n{reflection}")

        return stale + "\n".join(body_parts)

    async def _inject(self, event: Any, req: Any) -> None:
        if TextPart is None:
            return
        try:
            umo = event_umo(event)
            viewer_id = str(event.get_sender_id())
        except Exception:  # noqa: BLE001
            return
        text = await self.build_injection(
            umo, viewer_id, same_persona=lambda: self._same_persona(event, req)
        )
        if not text:
            return
        part = TextPart(text=text)
        part.mark_as_temp()
        req.extra_user_content_parts.append(part)

    async def _same_persona(self, event: Any, req: Any) -> bool:
        """当前会话白天用的人格是否就是夜间人格；查不到按相同处理（照旧注入）。"""
        if self.context is None or self._persona_id_getter is None:
            return True
        try:
            selected = await selected_persona(
                self.context, str(self._persona_id_getter() or ""), warn=False
            )
            current = await session_persona(
                self.context,
                event.unified_msg_origin,
                conversation=getattr(req, "conversation", None),
                platform_name=event.get_platform_name(),
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 判断会话人格失败: {exc}")
            return True
        return current is None or current.same_as(selected)
