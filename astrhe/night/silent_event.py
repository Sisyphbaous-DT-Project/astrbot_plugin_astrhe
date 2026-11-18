"""SilentNightEvent：夜间 agent 专用的静默事件。

AstrBot 4.30 起删除了 astrbot.core.cron.events.CronMessageEvent，这里直接基于
两版共有的 AstrMessageEvent 构造，字段照旧版 CronMessageEvent 的写法补齐：
消息对象、平台元数据、原始 session 与 context_obj、member 身份、extras 能力。
重写 send / send_streaming：只丢弃并在 DEBUG 记录，绝不向任何会话发送消息，
这是「夜间全程无感」的代码层保证（硬性要求 9）。

注意：每次 agent 调用各用一个新对象，不要共用；role 保持默认 member；
这个事件只给 Context.tool_loop_agent 用，不进入消息流水线，
也不伪装成 aiocqhttp 收到的真实用户消息。
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from astrbot.core.message.components import Plain
from astrbot.core.platform.astr_message_event import AstrMessageEvent
from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.platform_metadata import PlatformMetadata


class SilentNightEvent(AstrMessageEvent):
    """发送被静默的夜间内部事件。"""

    def __init__(
        self,
        *,
        context: Any,
        session: MessageSession,
        message: str = "",
        discard_sink: list | None = None,
        extras: dict[str, Any] | None = None,
    ) -> None:
        platform_meta = PlatformMetadata(
            name="astrhe",
            description="AstrHe Night",
            id=session.platform_id,
        )
        msg_obj = AstrBotMessage()
        msg_obj.type = session.message_type
        msg_obj.self_id = "astrhe"
        msg_obj.session_id = session.session_id
        msg_obj.message_id = uuid.uuid4().hex
        msg_obj.sender = MessageMember(
            user_id=session.session_id, nickname="AstrHe Night"
        )
        msg_obj.message = [Plain(message)]
        msg_obj.message_str = message
        msg_obj.raw_message = message
        msg_obj.timestamp = int(time.time())
        super().__init__(message, msg_obj, platform_meta, session.session_id)
        # 发送与平台判断都以原始 session 为准；运行时上下文挂在事件上
        self.session = session
        self.context_obj = context
        self.is_at_or_wake_command = True
        self.is_wake = True
        if extras:
            self._extras.update(extras)
        # 测试用：被丢弃的内容记录在这里（生产中传 None，只走 DEBUG 日志）
        self._discard_sink = discard_sink

    async def send(self, message) -> None:
        """静默：不发送，只记录。"""
        if self._discard_sink is not None:
            self._discard_sink.append(message)
        try:
            from astrbot.api import logger

            logger.debug("[AstrHe] 夜间静默事件拦截了一次 send")
        except Exception:  # noqa: BLE001
            pass

    async def send_streaming(self, generator, use_fallback: bool = False) -> None:
        """静默：消费掉生成器，不发送任何内容。"""
        try:
            async for chain in generator:
                if self._discard_sink is not None:
                    self._discard_sink.append(chain)
        except Exception:  # noqa: BLE001
            pass
        try:
            from astrbot.api import logger

            logger.debug("[AstrHe] 夜间静默事件拦截了一次 send_streaming")
        except Exception:  # noqa: BLE001
            pass


def make_silent_event(
    context: Any, umo: str, message: str = "", discard_sink: list | None = None
) -> SilentNightEvent:
    """按 UMO 构造一个静默事件。umo 为空时用虚拟会话。"""
    if umo:
        session = MessageSession.from_str(umo)
    else:
        from astrbot.core.platform.message_type import MessageType

        session = MessageSession(
            platform_name="astrhe",
            message_type=MessageType.FRIEND_MESSAGE,
            session_id="astrhe-night",
        )
    return SilentNightEvent(
        context=context,
        session=session,
        message=message or "夜间复盘",
        discard_sink=discard_sink,
    )
