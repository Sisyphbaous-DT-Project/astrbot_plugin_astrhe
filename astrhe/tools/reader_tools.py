"""只读工具集：夜间子代理、主 agent、审稿共用；astrhe_read_plan 同时给白天用。

所有工具只读，不写任何状态；输出都是给模型看的纯文本。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .. import constants as C
from ..platform import parse_umo, event_umo
from .base import AstrHeFunctionTool
from .alarm_view import read_alarms
from .secrecy import filter_secret_text
from .memory_readers import (
    PLAN_READ_PARAMETERS,
    build_memory_readers,
    page_footer,
    read_items,
    search_memory,
)


def format_message_line(row: dict, tz) -> str:
    """渲染成紧凑文本行：[HH:MM] 名片或昵称(QQ): 文本  #msgid"""
    dt = datetime.fromtimestamp(row["ts"], tz)
    name = row["sender_card"] or row["sender_name"] or "?"
    text = (row["text"] or "").replace("\n", " ")
    mid = row["platform_message_id"] or f"local-{row['id']}"
    prefix = "[我] " if row["is_bot"] else ""
    return f"[{dt.strftime('%m-%d %H:%M')}] {prefix}{name}({row['sender_id']}): {text}  #{mid}"


_UNLISTED = "这个会话不在 AstrHe 的会话名单里，它的聊天记录不再读取"


class ReaderTools:
    """只读工具工厂。"""

    def __init__(
        self,
        storage: Any,
        context: Any,
        tz_getter,
        window_getter,
        secrecy_resolver: Any = None,
        run_id: int | None = None,
        access: Any = None,
    ):
        self.storage = storage
        self.context = context
        self._tz_getter = tz_getter
        self._window_getter = window_getter
        self.secrecy_resolver = secrecy_resolver
        self.run_id = run_id
        self.access = access

    async def _allowed(self, umo: str) -> bool:
        """会话名单外的旧记录留着，但不再给夜间读取。"""
        return self.access is None or await self.access.allows(umo)

    def _tz(self):
        return self._tz_getter()

    def build(self) -> list[AstrHeFunctionTool]:
        return build_memory_readers(self) + [
            AstrHeFunctionTool(
                name="astrhe_read_reports",
                description="完整读取今晚各会话汇报，审稿核对用。",
                fn=self._read_reports,
            ),
            AstrHeFunctionTool(
                name="astrhe_list_sessions",
                description="列出本次复盘窗口内有记录的会话（群聊和私聊），带消息条数。",
                fn=self._list_sessions,
            ),
            AstrHeFunctionTool(
                name="astrhe_read_messages",
                description=(
                    "按会话分页读取聊天记录（已撤回的消息看不到）。"
                    "参数：umo（必填）、before_row_id（可选，翻更旧的页）。"
                    "单次最多 200 条，从旧到新排列。"
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "umo": {"type": "string", "description": "会话 UMO"},
                        "before_row_id": {
                            "type": "integer",
                            "description": "翻页锚点：只取该行之前的消息",
                        },
                    },
                    "required": ["umo"],
                },
                fn=self._read_messages,
            ),
            AstrHeFunctionTool(
                name="astrhe_search_messages",
                description=(
                    "按关键词搜索全部聊天记录（不含已撤回）。"
                    "参数：keyword（必填）、umo（可选，限定会话）。"
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "offset": {"type": "integer", "description": "搜索翻页偏移"},
                        "keyword": {"type": "string"},
                        "umo": {"type": "string"},
                    },
                    "required": ["keyword"],
                },
                fn=self._search_messages,
            ),
            AstrHeFunctionTool(
                name="astrhe_message_context",
                description="按消息 id 取它前后的上下文。参数：message_id（# 后面的编号）。",
                parameters={
                    "type": "object",
                    "properties": {"message_id": {"type": "string"}},
                    "required": ["message_id"],
                },
                fn=self._message_context,
            ),
            AstrHeFunctionTool(
                name="astrhe_find_person",
                description="查关系本：按 QQ 号或昵称别名查一个人。参数：query。",
                parameters={
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                },
                fn=self._find_person,
            ),
            AstrHeFunctionTool(
                name="astrhe_read_todos",
                description="读完整待办含备注与日期。status=open（默认，含 progress）/done/dropped/all；每页 100 条，offset 翻页。",
                parameters={
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "enum": ["open", "done", "dropped", "all"],
                        },
                        "offset": {"type": "integer"},
                        "include_done": {
                            "type": "boolean",
                            "description": "兼容旧调用；true 等同 status=all",
                        },
                    },
                },
                fn=self._read_todos,
            ),
            AstrHeFunctionTool(
                name="astrhe_read_promises",
                description="读承诺含备注与日期。status=open/done/broken/waived/all（默认）；每页 100 条，offset 翻页。",
                parameters={
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "enum": ["open", "done", "broken", "waived", "all"],
                        },
                        "offset": {"type": "integer"},
                    },
                },
                fn=self._read_promises,
            ),
            AstrHeFunctionTool(
                name="astrhe_read_alarms",
                description=(
                    "读闹钟；scope=active（默认）/finished/all；alarm_id 读单条完整卡片"
                    "及全部事件；已结束列表每页 50 条，用 offset 翻页。附平台任务清单供查重。"
                ),
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {
                            "type": "string",
                            "enum": ["active", "finished", "all"],
                        },
                        "alarm_id": {"type": "integer"},
                        "offset": {"type": "integer"},
                    },
                },
                fn=self._read_alarms,
            ),
            AstrHeFunctionTool(
                name="astrhe_read_plan",
                description=(
                    "读规划书。参数：date（可选，YYYY-MM-DD，默认最近一份）。"
                    "list=true 列出已有日期，每页 60 条，offset 翻页。"
                    "白天只有管理员私聊能读全文，其他会话只返回与本会话相关的段落；"
                    "对别人保密的内容可能被遮挡。"
                ),
                parameters=PLAN_READ_PARAMETERS,
                fn=self._read_plan,
            ),
        ]

    # ------------------------------------------------------------------

    async def _list_sessions(self, event: Any, **kwargs: Any) -> str:
        start_ts, end_ts = self._window_getter()
        umos = await self.storage.ledger.list_session_umos(start_ts)
        umos = [u for u in umos if await self._allowed(u)]
        if not umos:
            return "窗口内没有任何聊天记录"
        lines = []
        for umo in umos:
            msgs = await self.storage.ledger.get_messages(umo, start_ts, end_ts)
            _, mtype, sid = parse_umo(umo)
            kind = "群聊" if mtype == "GroupMessage" else "私聊"
            lines.append(f"{umo}（{kind} {sid}，{len(msgs)} 条）")
        return "窗口内的会话：\n" + "\n".join(lines)

    async def _read_messages(self, event: Any, umo: str = "", **kwargs: Any) -> str:
        if not umo:
            return "需要 umo 参数"
        if not await self._allowed(umo):
            return _UNLISTED
        before_row_id = kwargs.get("before_row_id")
        rows = await self.storage.ledger.get_messages_page(
            umo, before_row_id=before_row_id, limit=C.READ_MESSAGES_PAGE_LIMIT
        )
        if not rows:
            return "没有更多消息了"
        tz = self._tz()
        lines = [format_message_line(r, tz) for r in rows]
        oldest = rows[0]["id"]
        header = f"共 {len(rows)} 条（最早行号 {oldest}，翻更旧的页传 before_row_id={oldest}）："
        return header + "\n" + "\n".join(lines)

    async def _search_messages(
        self, event: Any, keyword: str = "", umo: str = "", **kwargs: Any
    ) -> str:
        if not keyword:
            return "需要 keyword 参数"
        if umo and not await self._allowed(umo):
            return _UNLISTED
        rows = await self.storage.ledger.search_messages(
            keyword,
            0,
            umo=umo or None,
            limit=51,
            offset=max(0, int(kwargs.get("offset", 0))),
        )
        if not rows:
            return f"没有找到含「{keyword}」的消息"
        tz = self._tz()
        more = len(rows) > 50
        rows = [r for r in rows[:50] if await self._allowed(r["umo"])]
        if not rows and not more:
            return f"没有找到含「{keyword}」的消息"
        lines = [f"{r['umo']} " + format_message_line(r, tz) for r in rows]
        suffix = (
            f"\n还有更多，继续翻传 offset={max(0, int(kwargs.get('offset', 0))) + 50}"
            if more
            else ""
        )
        return f"找到 {len(rows)} 条：\n" + "\n".join(lines) + suffix

    async def _message_context(
        self, event: Any, message_id: str = "", **kwargs: Any
    ) -> str:
        mid = str(message_id).lstrip("#")
        if mid.startswith("local-") and mid[6:].isdigit():
            row = await self.storage.db.fetchone(
                "SELECT * FROM messages WHERE id=? AND recalled=0", (int(mid[6:]),)
            )
        else:
            row = await self.storage.ledger.get_message_by_platform_id(mid)
        if not row or row["recalled"] or not await self._allowed(row["umo"]):
            return "账本里找不到这条消息"
        rows = await self.storage.ledger.get_message_context(row["umo"], row["ts"])
        tz = self._tz()
        return "上下文：\n" + "\n".join(format_message_line(r, tz) for r in rows)

    async def _find_person(self, event: Any, query: str = "", **kwargs: Any) -> str:
        query = str(query).strip()
        if not query:
            return "需要 query 参数"
        person = await self.storage.persons.get_person(query)
        if person is None:
            person = await self.storage.persons.find_by_alias(query)
        if person is None:
            return f"关系本里没有「{query}」"
        return (
            f"QQ {person['qq']}\n别名：{person['aliases_json']}\n"
            f"关系：{person['relation_text'] or '（未记录）'}\n"
            f"近况：{person['recent_text'] or '（未记录）'}\n"
            f"纪念日：{person['anniversaries_json']}"
        )

    async def _read_todos(self, event: Any, **kwargs: Any) -> str:
        status = kwargs.get("status", "all" if kwargs.get("include_done") else "open")
        return await read_items(
            self.storage,
            "todo",
            self._tz(),
            status=status,
            offset=kwargs.get("offset", 0),
        )

    async def _read_promises(self, event: Any, **kwargs: Any) -> str:
        return await read_items(
            self.storage,
            "promise",
            self._tz(),
            status=kwargs.get("status", "all"),
            offset=kwargs.get("offset", 0),
        )

    async def _search_memory(
        self, event: Any, keyword="", kind="all", offset=0, **kwargs
    ):
        return await search_memory(self.storage, keyword, kind, offset, self._tz())

    async def _read_alarms(self, event: Any, **kwargs: Any) -> str:
        admins = self.context.get_config().get("admins_id", [])
        admins = [str(x) for x in admins if str(x).isdigit()]
        return await read_alarms(
            self.storage,
            self.context,
            self._tz(),
            admins,
            scope=kwargs.get("scope", "active"),
            alarm_id=kwargs.get("alarm_id", 0),
            offset=kwargs.get("offset", 0),
        )

    async def _read_plan(self, event: Any, date: str = "", **kwargs: Any) -> str:
        if kwargs.get("list"):
            offset = max(0, int(kwargs.get("offset", 0)))
            total = await self.storage.plans.count_entries("plan")
            rows = await self.storage.plans.list_plans(C.MEMORY_PLAN_PAGE_SIZE, offset)
            return (
                f"规划书日期（共 {total} 份，本页 {len(rows)} 份）：\n"
                + "\n".join(r["plan_date"] for r in rows)
                + page_footer(total, offset, len(rows))
            )
        if date:
            plan = await self.storage.plans.get_plan_by_date(date)
        else:
            plan = await self.storage.plans.get_latest_plan()
        if not plan:
            return "还没有任何规划书"
        if self.run_id is not None:
            # 夜间复盘读自己的规划书，不遮挡
            return f"规划书（{plan['plan_date']}）：\n{plan['full_text']}"
        viewer_id = ""
        umo = ""
        try:
            viewer_id = str(event.get_sender_id())
            umo = event_umo(event)
        except Exception:  # noqa: BLE001
            pass
        _, message_type, _ = parse_umo(umo)
        # 全文里有各个会话的事和私聊细节，白天只在管理员私聊里给全文，
        # 其他会话只给本会话的段落，免得被群友一问就整本复述出来
        admin_private = (
            getattr(event, "role", "member") == "admin"
            and message_type == "FriendMessage"
        )
        if admin_private:
            title = f"规划书（{plan['plan_date']}）"
            text = plan["full_text"]
        else:
            title = f"规划书（{plan['plan_date']}）里与本会话相关的段落"
            sections = await self.storage.plans.get_sections(plan["id"])
            text = sections.get(umo, "") or "（没有专门写给本会话的段落）"
            text += "\n（完整规划书只在管理员私聊里可以翻阅）"
        if umo and self.secrecy_resolver is not None:
            present = await self.secrecy_resolver.present_ids(umo)
            text = filter_secret_text(
                text, viewer_id=viewer_id, session_umo=umo, present_ids=present
            )
        return f"{title}：\n{text}"

    async def _read_reports(self, event: Any, **kwargs) -> str:
        if self.run_id is None:
            return "当前没有夜间运行"
        reports = await self.storage.runs.get_reports(self.run_id)
        return (
            "\n\n".join(f"{r['umo']}\n{r['raw_text']}" for r in reports)
            or "今晚还没有汇报"
        )
