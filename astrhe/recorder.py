"""记录器：把消息、bot 回复、notice / request 事件写进聊天账本。

注册方式见 main.py：用 @filter.platform_adapter_type(ALL, priority=sys.maxsize-3)
保证尽早收到事件；本模块不产出任何结果、不 stop 事件。
插件停用时不记录；所有异常吞掉并打 DEBUG 日志。
"""

from __future__ import annotations

from typing import Any

from .platform import build_umo, normalize_session_id
from .timeutil import utc_now
from .storage import Storage

#: 装饰阶段暂存的整条回复（分段发送前的全文）
FULL_REPLY_EXTRA_KEY = "astrhe_full_reply"

#: notice 事件分类映射：(notice_type, sub_type) -> kind
_NOTICE_KIND_MAP = {
    ("notify", "poke"): "poke",
    ("notify", "group_msg_emoji_like"): "emoji_like",
    ("notify", "profile_like"): "profile_like",
    ("notify", "group_card"): "card_change",
    ("notify", "group_title"): "title_change",
    ("group_increase", ""): "group_increase",
    ("group_decrease", ""): "group_decrease",
    ("group_recall", ""): "recall",
    ("friend_recall", ""): "recall",
    ("essence", ""): "essence",
}

#: 消息组件类型 → 占位符（不存 base64，只留文字与占位符）
_COMPONENT_PLACEHOLDER = {
    "Image": "[图片]",
    "Record": "[语音]",
    "Video": "[视频]",
    "Face": "[表情]",
    "At": "[@某人]",
    "Reply": "[引用]",
    "Forward": "[合并转发]",
    "File": "[文件]",
    "Json": "[卡片消息]",
    "Xml": "[卡片消息]",
    "Poke": "[戳一戳]",
}


def components_to_text(message: list) -> str:
    """把 AstrBot 消息组件列表转成纯文本；非文本组件写成占位符。"""
    parts: list[str] = []
    for comp in message or []:
        cls_name = type(comp).__name__
        if cls_name == "Plain":
            parts.append(getattr(comp, "text", "") or "")
        elif cls_name == "At":
            qq = getattr(comp, "qq", "")
            name = getattr(comp, "name", "") or ""
            parts.append(f"[@{name or qq}]")
        elif cls_name == "Poke":
            parts.append("[戳一戳]")
        else:
            parts.append(_COMPONENT_PLACEHOLDER.get(cls_name, f"[{cls_name}]"))
    return "".join(parts).strip()


def _squash(text: str) -> str:
    """去掉空白后比较，分段插件会修剪每段首尾空白。"""
    return "".join(text.split())


class Recorder:
    """聊天账本记录器。"""

    def __init__(self, storage: Storage, logger: Any, enabled_getter, access=None):
        self.storage = storage
        self.logger = logger
        self._enabled_getter = enabled_getter
        self.access = access

    def _allowed(self, umo: str) -> bool:
        """会话名单外的不记（AstrBot 白名单会放行管理员在名单外群里说的话，这里也拦下）。

        到达的消息已经过 AstrBot 会话管理的过滤，只需再看名单。
        """
        return self.access is None or self.access.list_allows(umo)

    def _enabled(self) -> bool:
        try:
            return bool(self._enabled_getter())
        except Exception:  # noqa: BLE001
            return False

    async def record_event(self, event: Any) -> None:
        """处理一条到达的事件（消息 / notice / request）。"""
        try:
            if not self._enabled():
                return
            if event.get_platform_name() != "aiocqhttp":
                return
            raw = event.message_obj.raw_message
            if isinstance(raw, dict) and raw.get("post_type") == "notice":
                await self._record_notice(event, raw)
            elif isinstance(raw, dict) and raw.get("post_type") == "request":
                await self._record_request(event, raw)
            else:
                await self._record_message(event)
        except Exception as exc:  # noqa: BLE001 - 记录器绝不影响主流程
            self.logger.debug(f"[AstrHe] 记录事件失败: {exc}")

    def remember_reply(self, event: Any) -> None:
        """on_decorating_result（排在其他装饰插件之前）：先记下整条回复。

        分段插件会自己发出前几段、只把最后一段留给 AstrBot 发送，
        after_message_sent 时结果里只剩最后一段，所以要在拆分之前记住全文。
        """
        try:
            if not self._enabled() or event.get_platform_name() != "aiocqhttp":
                return
            result = event.get_result()
            if result is None or not getattr(result, "chain", None):
                return
            text = components_to_text(result.chain)
            if text:
                event.set_extra(FULL_REPLY_EXTRA_KEY, text)
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 暂存整条回复失败: {exc}")

    async def record_sent(self, event: Any) -> None:
        """after_message_sent：记录 bot 的普通回复（临时记录，夜间补拉合并）。"""
        try:
            if not self._enabled():
                return
            if event.get_platform_name() != "aiocqhttp":
                return
            result = event.get_result()
            if result is None or not getattr(result, "chain", None):
                return
            text = components_to_text(result.chain)
            full = event.get_extra(FULL_REPLY_EXTRA_KEY)
            event.set_extra(FULL_REPLY_EXTRA_KEY, None)
            # 只有最后发出的确实是整条的一部分（分段）才用整条；
            # 被后面的插件整条替换或改写过的，按实际发出的记
            if full and text and _squash(text) in _squash(full):
                text = full
            if not text:
                return
            obj = event.message_obj
            message_type = (
                obj.type.value if hasattr(obj.type, "value") else str(obj.type)
            )
            session_id = normalize_session_id(message_type, str(obj.session_id))
            umo = build_umo(event.get_platform_id(), message_type, session_id)
            # 发送完成钩子不受 AstrBot「对会话停用插件」的过滤，要做完整判断
            if self.access is not None and not await self.access.allows(
                getattr(event, "unified_msg_origin", "") or umo
            ):
                return
            await self.storage.ledger.add_message(
                umo=umo,
                platform_id=event.get_platform_id(),
                message_type=message_type,
                session_id=session_id,
                sender_id=str(obj.self_id),
                sender_name="",
                sender_card="",
                text=text,
                ts=utc_now(),
                platform_message_id=None,
                is_bot=True,
                source="live",
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.debug(f"[AstrHe] 记录 bot 回复失败: {exc}")

    # ------------------------------------------------------------------

    async def _record_message(self, event: Any) -> None:
        obj = event.message_obj
        message_type = obj.type.value if hasattr(obj.type, "value") else str(obj.type)
        session_id = normalize_session_id(message_type, str(obj.session_id))
        umo = build_umo(event.get_platform_id(), message_type, session_id)
        if not self._allowed(umo):
            return
        text = components_to_text(obj.message)
        sender = obj.sender
        platform_message_id = str(obj.message_id) if obj.message_id else None
        await self.storage.ledger.add_message(
            umo=umo,
            platform_id=event.get_platform_id(),
            message_type=message_type,
            session_id=session_id,
            sender_id=str(getattr(sender, "user_id", "") or ""),
            sender_name=str(getattr(sender, "nickname", "") or ""),
            sender_card=str(getattr(sender, "card", "") or ""),
            text=text,
            ts=float(obj.timestamp),
            platform_message_id=platform_message_id,
            is_bot=False,
            source="live",
        )

    async def _record_notice(self, event: Any, raw: dict) -> None:
        notice_type = str(raw.get("notice_type", ""))
        sub_type = str(raw.get("sub_type", ""))
        kind = _NOTICE_KIND_MAP.get((notice_type, sub_type)) or _NOTICE_KIND_MAP.get(
            (notice_type, ""), notice_type or "notice"
        )
        group_id = str(raw.get("group_id", "") or "")
        user_id = str(raw.get("user_id", "") or "")
        target_id = str(raw.get("target_id", "") or "")
        message_id = str(raw.get("message_id", "") or "")
        if group_id:
            umo = build_umo(event.get_platform_id(), "GroupMessage", group_id)
        else:
            umo = build_umo(
                event.get_platform_id(),
                "FriendMessage",
                user_id or str(event.get_sender_id()),
            )
        if not self._allowed(umo):
            return

        if kind == "recall":
            # 撤回：把对应消息标记为已撤回，并从夜间素材中排除
            if message_id:
                if group_id:
                    await self.storage.ledger.mark_recalled(
                        build_umo(event.get_platform_id(), "GroupMessage", group_id),
                        message_id,
                    )
                else:
                    await self.storage.ledger.mark_recalled_any(message_id)

        # detail 只留少量字段，避免把大对象塞进账本
        detail = {
            k: raw.get(k)
            for k in (
                "notice_type",
                "sub_type",
                "operator_id",
                "likes",
                "is_add",
                "card_new",
                "honor_type",
            )
            if raw.get(k) is not None
        }
        await self.storage.ledger.add_notice(
            umo=umo,
            kind=kind,
            actor_id=user_id,
            target_id=target_id,
            message_id=message_id,
            detail=detail,
            ts=float(event.message_obj.timestamp),
        )

    async def _record_request(self, event: Any, raw: dict) -> None:
        request_type = str(raw.get("request_type", ""))
        if request_type != "friend":
            return
        user_id = str(raw.get("user_id", "") or "")
        flag = str(raw.get("flag", "") or "")
        comment = str(raw.get("comment", "") or "")
        if not user_id or not flag:
            return
        await self.storage.ledger.add_friend_request(
            user_id=user_id,
            flag=flag,
            comment=comment,
            source="normal",
            ts=float(event.message_obj.timestamp),
        )
